import { screen } from '@testing-library/react';
import { renderApp } from './test/render';

const renderAt = renderApp;

describe('App routes', () => {
  it('sends a signed-out visitor from /me to log in, and shows My account when signed in', () => {
    renderAt('/me');
    expect(screen.getByRole('heading', { level: 1, name: 'Log in' })).toBeInTheDocument();
  });

  it('shows My account to a signed-in user', () => {
    sessionStorage.setItem('ledger-console.token', 'tok');
    renderAt('/me');
    expect(screen.getByRole('heading', { level: 1, name: 'My account' })).toBeInTheDocument();
  });

  it.each([
    ['/', 'Monitor'],
    ['/transactions', 'Transactions'],
    ['/transactions/01a1c4fb-6f14-74d2-8a6a-6a0f0ffdff16', 'Transactions'],
    ['/accounts', 'Accounts'],
  ])('%s shows the %s screen', (path, heading) => {
    renderAt(path);
    expect(screen.getByRole('heading', { level: 1, name: heading })).toBeInTheDocument();
  });

  it('shows a not-found page, inside the layout, for an unknown path', () => {
    renderAt('/no/such/page');
    expect(screen.getByRole('heading', { level: 1, name: 'Page not found' })).toBeInTheDocument();
    expect(screen.getByRole('navigation', { name: 'Main' })).toBeInTheDocument();
  });
});
