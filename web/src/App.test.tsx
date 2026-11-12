import { screen } from '@testing-library/react';
import { mockFetch, renderApp, signInAs } from './test/render';

const renderAt = renderApp;

describe('App routes', () => {
  it('sends a signed-out visitor from /me to log in, and shows My account when signed in', () => {
    renderAt('/me');
    expect(screen.getByRole('heading', { level: 1, name: 'Log in' })).toBeInTheDocument();
  });

  it('shows My account to a signed-in user', async () => {
    sessionStorage.setItem('ledger-console.token', 'tok');
    mockFetch({ 'GET /me/account': { status: 404, body: { error: { code: 'account_not_found', message: 'account not found' } } } });
    renderAt('/me');
    expect(await screen.findByRole('heading', { level: 1, name: 'My account' })).toBeInTheDocument();
  });

  it.each([
    ['/', 'Monitor'],
    ['/transactions', 'Transactions'],
    ['/transactions/01a1c4fb-6f14-74d2-8a6a-6a0f0ffdff16', 'Transaction'],
    ['/accounts', 'Accounts'],
    ['/accounts/01a1c4f9-6f63-7f06-9452-413ef1830b81', 'Account'],
  ])('%s shows the %s screen to an ops user', async (path, heading) => {
    signInAs('ops');
    mockFetch({});
    renderAt(path);
    expect(await screen.findByRole('heading', { level: 1, name: heading })).toBeInTheDocument();
  });

  it('shows a not-found page, inside the layout, for an unknown path', () => {
    renderAt('/no/such/page');
    expect(screen.getByRole('heading', { level: 1, name: 'Page not found' })).toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: 'Main' })).toBeInTheDocument();
  });
});
