import { render, screen } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { App } from './App';

function renderAt(path: string) {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <App />
    </MemoryRouter>,
  );
}

describe('App routes', () => {
  it.each([
    ['/', 'Monitor'],
    ['/transactions', 'Transactions'],
    ['/transactions/01a1c4fb-6f14-74d2-8a6a-6a0f0ffdff16', 'Transactions'],
    ['/accounts', 'Accounts'],
    ['/me', 'My account'],
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
