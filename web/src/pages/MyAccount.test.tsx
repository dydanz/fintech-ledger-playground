import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { mockFetch, renderApp } from '../test/render';

const account = (status = 'ACTIVE', balance = 100000) => ({
  status: 200,
  body: { account: { id: 'acct-1', status, ledgers: [{ id: 'l-1', currency: 'IDR', balance }] } },
});
const tx = (id: string, type = 'CREDIT', status = 'SETTLED', amount = 500) => ({
  id, type, status, amount, currency: 'IDR', created_at: '2026-11-11T08:00:00Z',
});

beforeEach(() => sessionStorage.setItem('ledger-console.token', 'tok-1'));

describe('My account', () => {
  it('offers to open an account when there is none, then shows it', async () => {
    let opened = false;
    const calls = mockFetch({
      'GET /me/account': () => (opened ? account() : { status: 404, body: { error: { code: 'account_not_found', message: 'account not found' } } }),
      'GET /me/transactions?limit=10': { status: 200, body: { transactions: [] } },
      'POST /me/open': () => ((opened = true), { status: 201, body: { account: { id: 'acct-1' } } }),
    });
    renderApp('/me');
    await userEvent.setup().click(await screen.findByRole('button', { name: 'Open my account' }));
    expect(await screen.findByTestId('balance-IDR')).toHaveTextContent('IDR 100,000');
    expect(calls.find((c) => c.path === '/me/open')?.headers.Authorization).toBe('Bearer tok-1');
  });

  it('shows the balance and pages through the history', async () => {
    const calls = mockFetch({
      'GET /me/account': account(),
      'GET /me/transactions?limit=10': { status: 200, body: { transactions: [tx('t1'), tx('t2', 'DEBIT', 'FAILED')], next_cursor: 'c2' } },
      'GET /me/transactions?limit=10&cursor=c2': { status: 200, body: { transactions: [tx('t3', 'DEBIT')] } },
    });
    renderApp('/me');
    expect(await screen.findByTestId('balance-IDR')).toHaveTextContent('IDR 100,000');
    await waitFor(() => expect(screen.getAllByRole('row')).toHaveLength(3)); // header + 2
    await userEvent.setup().click(screen.getByRole('button', { name: 'Load more' }));
    await waitFor(() => expect(screen.getAllByRole('row')).toHaveLength(4));
    expect(screen.queryByRole('button', { name: 'Load more' })).toBeNull();
    expect(calls.map((c) => c.path)).toContain('/me/transactions?limit=10&cursor=c2');
  });

  it('adds demo money with a fresh key per click and says what happened', async () => {
    const calls = mockFetch({
      'GET /me/account': account(),
      'GET /me/transactions?limit=10': { status: 200, body: { transactions: [] } },
      'POST /me/credit': { status: 201, body: { transaction: tx('t9', 'CREDIT', 'SETTLED', 10000) } },
    });
    renderApp('/me');
    const user = userEvent.setup();
    await user.click(await screen.findByRole('button', { name: 'Add demo money' }));
    expect(await screen.findByRole('status')).toHaveTextContent('Settled: added IDR 10,000.');
    await user.click(screen.getByRole('button', { name: 'Add demo money' }));
    await waitFor(() => expect(calls.filter((c) => c.path === '/me/credit')).toHaveLength(2));
    const [k1, k2] = calls.filter((c) => c.path === '/me/credit').map((c) => c.headers['Idempotency-Key']);
    expect(k1).toMatch(/^[0-9a-f-]{36}$/);
    expect(k1).not.toBe(k2);
    expect(calls.find((c) => c.path === '/me/credit')?.body).toEqual({ amount: 10000 });
  });

  it('explains a refused spend', async () => {
    mockFetch({
      'GET /me/account': account('ACTIVE', 5),
      'GET /me/transactions?limit=10': { status: 200, body: { transactions: [] } },
      'POST /me/debit': { status: 422, body: { error: { code: 'insufficient_funds', message: 'insufficient funds' } } },
    });
    renderApp('/me');
    await userEvent.setup().click(await screen.findByRole('button', { name: 'Spend' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Rejected: not enough balance. Nothing moved.');
    expect(sessionStorage.getItem('ledger-console.token')).toBe('tok-1');
  });

  it('warns when the account is frozen', async () => {
    mockFetch({ 'GET /me/account': account('FROZEN'), 'GET /me/transactions?limit=10': { status: 200, body: { transactions: [] } } });
    renderApp('/me');
    expect(await screen.findByRole('alert')).toHaveTextContent('This account is frozen');
  });

  it('says so, and disables the buttons, when demo actions are turned off', async () => {
    mockFetch({
      'GET /me/account': account(),
      'GET /me/transactions?limit=10': { status: 200, body: { transactions: [] } },
      'POST /me/credit': { status: 403, body: { error: { code: 'demo_actions_disabled', message: 'demo actions are turned off on this console' } } },
    });
    renderApp('/me');
    await userEvent.setup().click(await screen.findByRole('button', { name: 'Add demo money' }));
    expect(await screen.findByText('Demo actions are turned off on this console.')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Add demo money' })).toBeDisabled();
  });
});
