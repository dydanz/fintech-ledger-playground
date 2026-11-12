import { screen, within } from '@testing-library/react';
import { mockFetch, renderApp, signInAs } from '../test/render';

const ACC = '01a1c4fb-0000-7000-8000-0000000a7f5a';
const TX = '01a1c4fb-0000-7000-8000-00000000000r';

// A recovered credit from a Postgres drill, as ledger-console returns it.
// The timeline is deliberately out of order: the screen sorts it by time.
function detail(over: Record<string, unknown> = {}) {
  return {
    transaction: {
      id: TX, type: 'CREDIT', status: 'SETTLED', account_id: ACC, amount: 180, currency: 'IDR', requested_by: 'payments',
      trace_id: '4bf92f3577b34da6a3ce929d0e0e4736', created_at: '2026-11-11T07:11:18Z', updated_at: '2026-11-11T07:12:05Z',
      settled_at: '2026-11-11T07:12:05Z', balance_after: 1180,
    },
    label: { label: 'recovered', what: "Completed by the recovery job after the request couldn't finish on its own", attention: false },
    timeline: [
      { at: '2026-11-11T07:12:05Z', kind: 'ok', title: 'Settled: money moved', detail: 'Balance after: 1,180', source: 'ledger' },
      { at: '2026-11-11T07:11:18Z', kind: 'ok', title: 'Request received and saved', detail: 'By payments', source: 'ledger' },
      { at: '2026-11-11T07:12:40Z', kind: 'ok', title: 'Verified by the reconciler', source: 'logs' },
      { at: '2026-11-11T07:12:05.003Z', kind: 'recovered', title: 'Completed by the recovery job', source: 'logs' },
      { at: '2026-11-11T07:12:06Z', kind: 'ok', title: 'Events published to Kafka', source: 'ledger' },
    ],
    journal: [
      { id: 'j1', ledger_id: 'l1', ledger_kind: 'USER', account_id: ACC, amount: 180, currency: 'IDR', balance_after: 1180 },
      { id: 'j2', ledger_id: 'l2', ledger_kind: 'SYSTEM', ledger_code: 'settlement', amount: -180, currency: 'IDR' },
    ],
    explore_url: 'http://localhost:3000/explore?panes=x',
    signals: { available: true },
    ...over,
  };
}

const open = (body: unknown, status = 200) => {
  mockFetch({ [`GET /ops/transactions/${TX}`]: { status, body } });
  renderApp(`/transactions/${TX}`);
};

beforeEach(() => signInAs('ops'));

describe('Transaction', () => {
  it('shows what it was and how it was handled', async () => {
    open(detail());
    expect(await screen.findByRole('heading', { name: 'Credit · IDR 180' })).toBeInTheDocument();
    expect(screen.getByTestId('chip')).toHaveTextContent('Recovered');
    expect(screen.getByText("Completed by the recovery job after the request couldn't finish on its own")).toBeInTheDocument();
  });

  it('lists the timeline in time order, each step marked with where it came from', async () => {
    open(detail());
    const steps = within(await screen.findByRole('list', { name: 'Timeline' })).getAllByRole('listitem');
    expect(steps.map((s) => within(s).getByTestId('step-title').textContent)).toEqual([
      'Request received and saved',
      'Settled: money moved',
      'Completed by the recovery job',
      'Events published to Kafka',
      'Verified by the reconciler',
    ]);
    expect(steps.map((s) => s.className)).toEqual(['ok', 'ok', 'rec', 'ok', 'ok']);
    expect(within(steps[0]).getByText('ledger record')).toBeInTheDocument();
    expect(within(steps[2]).getByText('logs')).toBeInTheDocument();
    expect(steps[2]).toHaveTextContent('11 Nov, 07:12:05');
  });

  it('shows the double entry with money in equal to money out', async () => {
    open(detail());
    const table = await screen.findByRole('table', { name: 'Double entry' });
    const rows = within(table).getAllByRole('row');
    expect(rows[1]).toHaveTextContent('Account …7f5aIDR 180');
    expect(rows[2]).toHaveTextContent('System: settlementIDR 180');
    expect(rows[3]).toHaveTextContent('TotalIDR 180IDR 180');
    expect(screen.getByText('Balanced: money in equals money out.')).toBeInTheDocument();
    expect(within(rows[1]).getByRole('link')).toHaveAttribute('href', `/accounts/${ACC}`);
  });

  it('flags a journal that does not balance', async () => {
    const d = detail();
    d.journal[1].amount = -170;
    open(d);
    expect(await screen.findByText('Not balanced: money in and money out differ. The reconciler reports this.')).toHaveClass('bad');
  });

  it('says why there are no journal lines', async () => {
    open(detail({ journal: [], transaction: { ...detail().transaction, status: 'FAILED', failure_reason: 'insufficient_funds', settled_at: undefined } }));
    expect(await screen.findByText('No money moved: the transaction was refused.')).toBeInTheDocument();
    expect(screen.queryByRole('table', { name: 'Double entry' })).not.toBeInTheDocument();
  });

  it('links the trace id to Grafana and the account, and never shows reference or metadata', async () => {
    open(detail({ transaction: { ...detail().transaction, reference: 'INV-secret-123', metadata: { note: 'private-note' } } }));
    const trace = await screen.findByRole('region', { name: 'Traceability' });
    expect(within(trace).getByRole('link', { name: /Open its logs in Grafana/ })).toHaveAttribute('href', 'http://localhost:3000/explore?panes=x');
    expect(trace).toHaveTextContent('4bf92f3577b34da6a3ce929d0e0e4736');
    expect(trace).toHaveTextContent('payments');
    expect(document.body).not.toHaveTextContent('INV-secret-123');
    expect(document.body).not.toHaveTextContent('private-note');
  });

  it('links a void and its original both ways', async () => {
    open(detail({ reversed_by: '01a1c4fb-0000-7000-8000-0000000000v2', label: { label: 'reversed', what: 'Reversed by a later void', attention: false } }));
    expect(await screen.findByRole('link', { name: '…00v2' })).toHaveAttribute('href', '/transactions/01a1c4fb-0000-7000-8000-0000000000v2');
    expect(screen.getByText('Reversed by')).toBeInTheDocument();
  });

  it('links a void to the transaction it reversed', async () => {
    open(detail({ transaction: { ...detail().transaction, type: 'VOID', original_transaction_id: '01a1c4fb-0000-7000-8000-0000000000o1' } }));
    expect(await screen.findByRole('link', { name: '…00o1' })).toHaveAttribute('href', '/transactions/01a1c4fb-0000-7000-8000-0000000000o1');
    expect(screen.getByText('Reverses')).toBeInTheDocument();
  });

  it('without the log signals, says the log-backed steps are missing', async () => {
    open(detail({ signals: { available: false }, timeline: detail().timeline.filter((s) => s.source === 'ledger') }));
    expect(await screen.findByRole('note')).toHaveTextContent('recovery, fraud and reconciler steps');
  });

  it('says so when the transaction does not exist', async () => {
    open({ error: { code: 'transaction_not_found', message: 'transaction not found' } }, 404);
    expect(await screen.findByRole('alert')).toHaveTextContent('No transaction with this id.');
  });
});
