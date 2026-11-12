import { act, screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { mockFetch, renderApp, signInAs } from '../test/render';
import { POLL_MS } from './Monitor';

const tx = (id: string, over: Record<string, unknown> = {}) => ({
  id, type: 'CREDIT', status: 'SETTLED', account_id: `acc-0000-${id}`, amount: 180, currency: 'IDR',
  requested_by: 'payments', trace_id: 't', created_at: '2026-11-11T07:11:18Z', updated_at: '2026-11-11T07:11:18Z', ...over,
});

// A Monitor response as ledger-console builds it, from a Postgres drill: one
// incident still active (Redis), one resolved (the database).
function monitor(over: Record<string, unknown> = {}) {
  return {
    generated_at: '2026-11-11T07:15:00Z',
    window: { since: '2026-11-11T07:05:00Z', until: '2026-11-11T07:15:00Z' },
    signals: { available: true },
    tiles: {
      transactions: { count: 21100, per_second: 35.2 },
      handled_correctly: { ratio: 0.952, system_failures: 1097, valid: 22197 },
      needed_recovery: { recovered: 1, refused: 1096, still_pending: 0, oldest_pending_ms: 0 },
      reconciler: { status: 'clean', mismatches: 0, mode: 'incremental', at: '2026-11-11T07:14:20Z' },
      outbox: { pending: 2, oldest_age_ms: 30 },
    },
    incidents: [
      { kind: 'redis', active: true, started_at: '2026-11-11T07:14:30Z', title: 'Redis unavailable',
        text: 'Payments keep working without the cache, a little slower.', count: 40 },
      { kind: 'database', active: false, started_at: '2026-11-11T07:11:18Z', ended_at: '2026-11-11T07:12:03Z', title: 'Database unavailable',
        text: 'The database was down for 45 s. 1,096 requests were refused with "try again"; 1 stuck payment was completed by the recovery job.', count: 1096, recovered: 1 },
    ],
    feed: [
      { transaction: tx('n1', { settled_at: '2026-11-11T07:11:18.003Z' }), label: 'normal', what: 'Settled in 3 ms', attention: false },
      { transaction: tx('r1'), label: 'recovered', what: 'Completed by the recovery job after the request couldn\'t finish on its own', attention: false },
      { transaction: tx('j1', { type: 'DEBIT', status: 'FAILED', failure_reason: 'insufficient_funds', amount: 900 }), label: 'rejected', what: 'Insufficient funds; balance unchanged', attention: false },
      { transaction: tx('v1', { type: 'VOID' }), label: 'reversed', what: 'A reversal: a reversing entry was added, nothing was deleted', attention: false },
      { transaction: tx('t1', { status: 'FAILED', failure_reason: 'processing_timeout' }), label: 'timed_out', what: 'Couldn\'t complete in time; failed safely, nothing moved', attention: true },
      { transaction: tx('p1', { status: 'PENDING' }), label: 'in_progress', what: 'Still processing after 1m30s; the recovery job will complete it', attention: true },
    ],
    handled: [
      { label: 'normal', count: 19990 }, { label: 'recovered', count: 1 }, { label: 'refused_try_again', count: 1096 },
      { label: 'rejected', count: 12 }, { label: 'reversed', count: 0 }, { label: 'timed_out', count: 0 }, { label: 'in_progress', count: 0 },
    ],
    self_healing: [
      { at: '2026-11-11T07:12:05Z', text: 'Recovery job completed a stuck payment' },
      { at: '2026-11-11T07:12:03Z', text: 'The database is back: writes succeed again, nothing half-done was kept' },
    ],
    ...over,
  };
}

const rows = () => within(screen.getByRole('table', { name: 'Live transactions' })).getAllByRole('row').slice(1);

beforeEach(() => signInAs('ops'));

describe('Monitor', () => {
  it('shows the tiles from the window', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor() } });
    renderApp('/');
    const tiles = await screen.findByRole('region', { name: 'Totals' });
    expect(within(tiles).getByText('21,100')).toBeInTheDocument();
    expect(within(tiles).getByText('35.2 / s')).toBeInTheDocument();
    expect(within(tiles).getByText('95.2%')).toBeInTheDocument();
    expect(within(tiles).getByText('none stuck · 0 timed out')).toBeInTheDocument();
    expect(within(tiles).getByText('1,097')).toBeInTheDocument(); // 1 recovered + 1,096 refused
    expect(within(tiles).getByText('all completed ✓')).toBeInTheDocument();
    expect(within(tiles).getByText('✓ clean')).toBeInTheDocument();
    expect(within(tiles).getByText('2 waiting')).toBeInTheDocument();
  });

  it('shows an active incident in yellow and a resolved one in green', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor() } });
    renderApp('/');
    const active = await screen.findByRole('status', { name: /Incident in progress: Redis unavailable/ });
    expect(active).toHaveClass('banner');
    expect(active).not.toHaveClass('resolved');
    expect(active).toHaveTextContent('Payments keep working without the cache');
    const resolved = screen.getByRole('status', { name: /Resolved: Database unavailable/ });
    expect(resolved).toHaveClass('banner', 'resolved');
    expect(resolved).toHaveTextContent('(45 s)');
    expect(resolved).toHaveTextContent('1 stuck payment was completed by the recovery job');
  });

  it('labels each feed row with a chip, red when it needs attention, and links it', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor() } });
    renderApp('/');
    await screen.findByRole('table', { name: 'Live transactions' });
    const chips = rows().map((r) => within(r).getByTestId('chip'));
    expect(chips.map((c) => c.textContent)).toEqual(['Normal', 'Recovered', 'Rejected', 'Reversed', 'Timed out', 'In progress']);
    expect(chips.map((c) => c.className)).toEqual([
      'chip c-ok', 'chip c-rec', 'chip c-rej', 'chip c-warn', 'chip c-bad', 'chip c-bad',
    ]);
    expect(rows()[2]).toHaveTextContent('IDR 900');
    expect(rows()[2]).toHaveTextContent('…0-j1'); // the account id's last 4 characters
    expect(within(rows()[1]).getByRole('link', { name: /Completed by the recovery job/ })).toHaveAttribute('href', '/transactions/r1');
    expect(within(rows()[1]).getByRole('link', { name: '…0-r1' })).toHaveAttribute('href', '/accounts/acc-0000-r1');
  });

  it('filters the feed by label', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor() } });
    renderApp('/');
    await screen.findByRole('table', { name: 'Live transactions' });
    await userEvent.click(screen.getByRole('button', { name: 'Recovered (1)' }));
    expect(rows()).toHaveLength(1);
    expect(rows()[0]).toHaveTextContent('Completed by the recovery job');
    expect(screen.getByRole('button', { name: 'Recovered (1)' })).toHaveAttribute('aria-pressed', 'true');
    await userEvent.click(screen.getByRole('button', { name: 'Reversed (1)' }));
    expect(rows()).toHaveLength(1);
    await userEvent.click(screen.getByRole('button', { name: 'All (6)' }));
    expect(rows()).toHaveLength(6);
  });

  it('says so when a filter matches nothing in the feed', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor({ feed: [monitor().feed[0]] }) } });
    renderApp('/');
    await screen.findByRole('table', { name: 'Live transactions' });
    await userEvent.click(screen.getByRole('button', { name: 'Recovered (0)' }));
    expect(screen.getByText('None of the newest 1 transactions is Recovered.')).toBeInTheDocument();
  });

  it('draws one bar per way of handling, with its count', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor() } });
    renderApp('/');
    const bars = await screen.findByRole('list', { name: 'How it was handled' });
    const items = within(bars).getAllByRole('listitem').map((li) => li.textContent);
    expect(items).toEqual([
      'Settled normally · 19,990',
      'Completed by the recovery job · 1',
      'Refused with "try again" during an outage · 1,096',
      'Rejected by a business rule · 12',
      'Reversed (voids) · 0',
      'Timed out · 0',
      'Still in progress · 0',
    ]);
  });

  it('lists the self-healing activity, newest first', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 200, body: monitor() } });
    renderApp('/');
    const list = await screen.findByRole('list', { name: 'Self-healing activity' });
    expect(within(list).getAllByRole('listitem')[0]).toHaveTextContent('07:12:05Recovery job completed a stuck payment');
  });

  it('flags a stuck payment, a reconciler mismatch and a slow outbox', async () => {
    const m = monitor();
    m.tiles.needed_recovery = { recovered: 0, refused: 0, still_pending: 3, oldest_pending_ms: 95000 };
    m.tiles.reconciler = { status: 'mismatches', mismatches: 2, mode: 'full', at: '2026-11-11T07:14:20Z' };
    m.tiles.outbox = { pending: 45332, oldest_age_ms: 290000 };
    mockFetch({ 'GET /ops/monitor': { status: 200, body: m } });
    renderApp('/');
    const tiles = await screen.findByRole('region', { name: 'Totals' });
    expect(within(tiles).getByText('3 pending, oldest 1 min 35 s · 0 timed out')).toBeInTheDocument();
    expect(within(tiles).getByText('3 still pending')).toBeInTheDocument();
    expect(within(tiles).getByText('✗ 2 mismatches')).toHaveClass('bad');
    expect(within(tiles).getByText('45,332 waiting')).toHaveClass('warnc');
    expect(within(tiles).getByText('oldest 4 min 50 s')).toBeInTheDocument();
  });

  it('without the log signals, says so and claims no recoveries or incidents', async () => {
    // Incidents and self-healing come only from the logs: even if a response
    // carried some, the screen doesn't show them without the signals.
    const m = monitor({ signals: { available: false } });
    m.tiles.needed_recovery = { recovered: 0, refused: 0, still_pending: 0, oldest_pending_ms: 0 };
    m.tiles.reconciler = { status: 'unknown', mismatches: 0 } as typeof m.tiles.reconciler;
    mockFetch({ 'GET /ops/monitor': { status: 200, body: m } });
    renderApp('/');
    expect(await screen.findByRole('note')).toHaveTextContent('Activity signals unavailable');
    const tiles = screen.getByRole('region', { name: 'Totals' });
    expect(within(tiles).getAllByText('unknown')).toHaveLength(2); // needed recovery, books balanced
    expect(screen.queryByRole('status')).not.toBeInTheDocument();
    const bars = screen.getByRole('list', { name: 'How it was handled' });
    expect(bars).toHaveTextContent('Completed by the recovery job · unknown');
    expect(bars).toHaveTextContent('Refused with "try again" during an outage · unknown');
    // The ledger's own data still renders.
    expect(rows()).toHaveLength(6);
    expect(screen.queryByRole('list', { name: 'Self-healing activity' })).not.toBeInTheDocument();
  });

  it('polls every few seconds, and keeps the last answer when a poll fails', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      let n = 0;
      const calls = mockFetch({
        'GET /ops/monitor': () => (++n === 2 ? { status: 503, body: { error: { code: 'upstream_unavailable', message: 'The ledger could not be reached.' } } } : { status: 200, body: monitor() }),
      });
      renderApp('/');
      await screen.findByText('21,100');
      const polls = () => calls.filter((c) => c.path === '/ops/monitor').length;
      await act(() => vi.advanceTimersByTimeAsync(POLL_MS));
      expect(polls()).toBe(2);
      expect(await screen.findByRole('alert')).toHaveTextContent('The ledger could not be reached.');
      expect(screen.getByText('21,100')).toBeInTheDocument();
      await act(() => vi.advanceTimersByTimeAsync(POLL_MS));
      expect(polls()).toBe(3);
      expect(screen.queryByRole('alert')).not.toBeInTheDocument();
    } finally {
      vi.useRealTimers();
    }
  });

  it('shows the error when the first load fails', async () => {
    mockFetch({ 'GET /ops/monitor': { status: 503, body: { error: { code: 'upstream_unavailable', message: 'The ledger could not be reached.' } } } });
    renderApp('/');
    expect(await screen.findByRole('alert')).toHaveTextContent('The ledger could not be reached.');
  });
});
