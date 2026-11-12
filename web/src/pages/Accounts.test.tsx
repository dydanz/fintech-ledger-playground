import { screen, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { mockFetch, renderApp, signInAs } from '../test/render';

const A1 = '01a1c4fb-0000-7000-8000-0000000a7f5a';
const A2 = '01a1c4fb-0000-7000-8000-0000000a19bb';

const account = (id: string, status: 'ACTIVE' | 'FROZEN', balance: number) => ({
  id, user_id: 'u', status, created_at: '2026-11-11T07:00:00Z', ledgers: [{ id: `l-${id}`, currency: 'IDR', balance }],
});

const item = (id: string, label: string, what: string, over: Record<string, unknown> = {}) => ({
  transaction: { id, type: 'CREDIT', status: 'SETTLED', account_id: A1, amount: 100, currency: 'IDR', requested_by: 'payments', trace_id: 't',
    created_at: '2026-11-11T07:10:00Z', updated_at: '2026-11-11T07:10:00Z', ...over },
  label, what, attention: false,
});

beforeEach(() => signInAs('ops'));

describe('Accounts list', () => {
  it('lists accounts with status and balances, and loads more', async () => {
    const calls = mockFetch({
      'GET /ops/accounts?limit=50': { status: 200, body: { accounts: [account(A1, 'ACTIVE', 1180)], next_cursor: 'c1' } },
      'GET /ops/accounts?limit=50&cursor=c1': { status: 200, body: { accounts: [account(A2, 'FROZEN', 75)] } },
    });
    renderApp('/accounts');
    const table = await screen.findByRole('table', { name: 'Accounts' });
    let rows = within(table).getAllByRole('row');
    expect(rows[1]).toHaveTextContent('…7f5aActiveIDR 1,180');
    expect(within(rows[1]).getByRole('link')).toHaveAttribute('href', `/accounts/${A1}`);
    await userEvent.click(screen.getByRole('button', { name: 'Load more' }));
    await screen.findByText('…19bb');
    rows = within(screen.getByRole('table', { name: 'Accounts' })).getAllByRole('row');
    expect(rows).toHaveLength(3);
    expect(within(rows[2]).getByText('Frozen')).toHaveClass('chip', 'c-rej');
    expect(screen.queryByRole('button', { name: 'Load more' })).not.toBeInTheDocument();
    expect(calls.map((c) => c.path).filter((p) => p.startsWith('/ops/'))).toEqual(['/ops/accounts?limit=50', '/ops/accounts?limit=50&cursor=c1']);
  });
});

describe('Account', () => {
  const detail = (over: Record<string, unknown> = {}) => ({
    account: account(A1, 'FROZEN', 1180),
    events: [
      { type: 'AccountOpened', payload: {}, occurred_at: '2026-11-11T07:00:00Z' },
      { type: 'LedgerOpened', payload: { currency: 'IDR' }, occurred_at: '2026-11-11T07:00:00Z' },
      { type: 'AccountFrozen', payload: { actor: 'fraud-engine', rule: 'topup_velocity' }, occurred_at: '2026-11-11T07:08:51Z' },
      { type: 'AccountUnfrozen', payload: { actor: 'ops' }, occurred_at: '2026-11-11T07:09:30Z' },
      { type: 'AccountFrozen', payload: { actor: 'ops' }, occurred_at: '2026-11-11T07:09:40Z' },
    ],
    transactions: [item('t-rec', 'recovered', 'Completed by the recovery job'), item('t-rej', 'rejected', 'Account frozen', { type: 'DEBIT', status: 'FAILED' })],
    next_cursor: 'p2',
    signals: { available: true },
    ...over,
  });

  it('shows the balances, the status history with who and why, and the transactions', async () => {
    mockFetch({ [`GET /ops/accounts/${A1}`]: { status: 200, body: detail() } });
    renderApp(`/accounts/${A1}`);
    expect(await screen.findByRole('heading', { name: 'Account …7f5a' })).toBeInTheDocument();
    expect(screen.getAllByText('Frozen')[0]).toHaveClass('chip', 'c-rej');
    expect(screen.getByRole('region', { name: 'Balances' })).toHaveTextContent('IDR1,180');
    const history = within(screen.getByRole('list', { name: 'Status history' })).getAllByRole('listitem').map((li) => li.textContent);
    expect(history).toEqual([
      '11 Nov, 07:09:40Frozen by ops',
      '11 Nov, 07:09:30Unfrozen by ops',
      '11 Nov, 07:08:51Frozen by the fraud engine: too many top-ups in a short time (topup_velocity)',
      '11 Nov, 07:00:00IDR ledger opened',
      '11 Nov, 07:00:00Account opened',
    ]);
    const rows = within(screen.getByRole('table', { name: 'Transactions of this account' })).getAllByRole('row').slice(1);
    expect(rows.map((r) => within(r).getByTestId('chip').textContent)).toEqual(['Recovered', 'Rejected']);
  });

  it('loads older transactions with the cursor', async () => {
    mockFetch({
      [`GET /ops/accounts/${A1}`]: { status: 200, body: detail() },
      [`GET /ops/accounts/${A1}?cursor=p2`]: { status: 200, body: detail({ transactions: [item('t-old', 'normal', 'Settled in 3 ms')], next_cursor: undefined }) },
    });
    renderApp(`/accounts/${A1}`);
    await screen.findByRole('table', { name: 'Transactions of this account' });
    await userEvent.click(screen.getByRole('button', { name: 'Load older' }));
    await screen.findByText('Settled in 3 ms');
    expect(within(screen.getByRole('table', { name: 'Transactions of this account' })).getAllByRole('row')).toHaveLength(4);
    expect(screen.queryByRole('button', { name: 'Load older' })).not.toBeInTheDocument();
  });

  it('without the log signals, says recoveries cannot be told apart', async () => {
    mockFetch({ [`GET /ops/accounts/${A1}`]: { status: 200, body: detail({ signals: { available: false } }) } });
    renderApp(`/accounts/${A1}`);
    expect(await screen.findByRole('note')).toHaveTextContent('recovered transactions show as Normal');
  });

  it('says so when the account does not exist', async () => {
    mockFetch({ [`GET /ops/accounts/${A1}`]: { status: 404, body: { error: { code: 'account_not_found', message: 'account not found' } } } });
    renderApp(`/accounts/${A1}`);
    expect(await screen.findByRole('alert')).toHaveTextContent('No account with this id.');
  });
});

describe('Transactions list without the log signals', () => {
  it('says recoveries cannot be told apart', async () => {
    mockFetch({ 'GET /ops/transactions?limit=50': { status: 200, body: { transactions: [item('t-1', 'normal', 'Settled in 3 ms')], signals: { available: false } } } });
    renderApp('/transactions');
    expect(await screen.findByRole('note')).toHaveTextContent('recovered transactions show as Normal');
  });
});

describe('Transactions list', () => {
  it('lists the newest changes, filters by status and type, and links each row', async () => {
    const calls = mockFetch({
      'GET /ops/transactions?limit=50': { status: 200, body: { transactions: [item('t-1', 'normal', 'Settled in 3 ms')], signals: { available: true } } },
      'GET /ops/transactions?limit=50&status=FAILED': { status: 200, body: { transactions: [item('t-2', 'rejected', 'Insufficient funds', { status: 'FAILED' })], signals: { available: true } } },
      'GET /ops/transactions?limit=50&status=FAILED&type=DEBIT': { status: 200, body: { transactions: [], signals: { available: true } } },
    });
    renderApp('/transactions');
    const table = await screen.findByRole('table', { name: 'Transactions' });
    expect(within(table).getByRole('link', { name: 'Settled in 3 ms' })).toHaveAttribute('href', '/transactions/t-1');
    await userEvent.selectOptions(screen.getByLabelText('Status'), 'FAILED');
    expect(await screen.findByText('Insufficient funds')).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByLabelText('Type'), 'DEBIT');
    expect(await screen.findByText('No transactions match.')).toBeInTheDocument();
    expect(calls.map((c) => c.path).filter((p) => p.startsWith('/ops/'))).toEqual([
      '/ops/transactions?limit=50', '/ops/transactions?limit=50&status=FAILED', '/ops/transactions?limit=50&status=FAILED&type=DEBIT',
    ]);
  });
});
