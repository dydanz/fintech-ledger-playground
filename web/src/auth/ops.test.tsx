import { screen, within } from '@testing-library/react';
import { mockFetch, renderApp, signInAs } from '../test/render';

const monitorBody = {
  generated_at: '2026-11-11T07:15:00Z', window: { since: '2026-11-11T07:05:00Z', until: '2026-11-11T07:15:00Z' }, signals: { available: true },
  tiles: {
    transactions: { count: 0, per_second: 0 }, handled_correctly: { ratio: 1, system_failures: 0, valid: 0 },
    needed_recovery: { recovered: 0, refused: 0, still_pending: 0, oldest_pending_ms: 0 },
    reconciler: { status: 'clean', mismatches: 0, at: '2026-11-11T07:14:00Z' }, outbox: { pending: 0, oldest_age_ms: 0 },
  },
  incidents: [], feed: [], handled: [], self_healing: [],
};

const navLinks = () => within(screen.getByRole('navigation', { name: 'Main' })).getAllByRole('link').map((a) => a.textContent);

// The ops screens need a logged-in user with the ops role (D§7.5).
describe('ops access', () => {
  it.each(['/', '/transactions', '/transactions/t-1', '/accounts', '/accounts/a-1'])('sends a signed-out visitor from %s to log in', (path) => {
    const calls = mockFetch({});
    renderApp(path);
    expect(screen.getByRole('heading', { level: 1, name: 'Log in' })).toBeInTheDocument();
    expect(calls.filter((c) => c.path.startsWith('/ops/'))).toEqual([]);
  });

  it('shows a plain user that they need ops access, and reads nothing', async () => {
    signInAs('user');
    const calls = mockFetch({});
    renderApp('/accounts');
    expect(await screen.findByRole('heading', { level: 1, name: 'Ops access needed' })).toBeInTheDocument();
    expect(within(screen.getByRole('main')).getByRole('link', { name: 'My account' })).toHaveAttribute('href', '/me');
    expect(calls.filter((c) => c.path.startsWith('/ops/'))).toEqual([]);
  });

  it('shows an ops user the ops screens, sending their token', async () => {
    signInAs('ops');
    const calls = mockFetch({ 'GET /ops/monitor': { status: 200, body: monitorBody } });
    renderApp('/');
    expect(await screen.findByRole('region', { name: 'Totals' })).toBeInTheDocument();
    const monitor = calls.find((c) => c.path === '/ops/monitor');
    expect(monitor?.headers.Authorization).toBe('Bearer tok-ops');
  });

  it('shows the ops links only to ops users', async () => {
    mockFetch({});
    const { unmount } = renderApp('/me');
    expect(navLinks()).toEqual(['My account']);
    unmount();

    signInAs('user');
    mockFetch({ 'GET /me/account': { status: 404, body: { error: { code: 'account_not_found', message: 'account not found' } } } });
    const second = renderApp('/me');
    await screen.findByRole('heading', { level: 1, name: 'My account' });
    expect(navLinks()).toEqual(['My account']);
    second.unmount();

    signInAs('ops');
    mockFetch({ 'GET /me/account': { status: 404, body: { error: { code: 'account_not_found', message: 'account not found' } } } });
    renderApp('/me');
    await screen.findByRole('link', { name: 'Monitor' });
    expect(navLinks()).toEqual(['Monitor', 'Transactions', 'Accounts', 'My account']);
  });
});
