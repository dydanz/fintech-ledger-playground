import { screen, within } from '@testing-library/react';
import { mockFetch, renderApp, signInAs } from '../test/render';

describe('Layout', () => {
  beforeEach(() => {
    signInAs('ops');
    mockFetch({});
  });

  it('links every screen from the main navigation', async () => {
    renderApp('/');
    await screen.findByRole('link', { name: 'Monitor' });
    const nav = screen.getByRole('navigation', { name: 'Main' });
    const hrefs = within(nav)
      .getAllByRole('link')
      .map((a) => [a.textContent, a.getAttribute('href')]);
    expect(hrefs).toEqual([
      ['Monitor', '/'],
      ['Transactions', '/transactions'],
      ['Accounts', '/accounts'],
      ['My account', '/me'],
    ]);
  });

  it('marks only the current screen, including its sub-pages', async () => {
    renderApp('/accounts/01a1c4f9-6f63-7f06-9452-413ef1830b81');
    await screen.findByRole('link', { name: 'Accounts' });
    const nav = screen.getByRole('navigation', { name: 'Main' });
    const active = within(nav)
      .getAllByRole('link')
      .filter((a) => a.classList.contains('on'))
      .map((a) => a.textContent);
    expect(active).toEqual(['Accounts']);
  });
});
