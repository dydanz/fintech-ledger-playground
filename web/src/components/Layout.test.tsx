import { screen, within } from '@testing-library/react';
import { renderApp } from '../test/render';

describe('Layout', () => {
  it('links every screen from the main navigation', () => {
    renderApp('/');
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

  it('marks only the current screen, including its sub-pages', () => {
    renderApp('/accounts/01a1c4f9-6f63-7f06-9452-413ef1830b81');
    const nav = screen.getByRole('navigation', { name: 'Main' });
    const active = within(nav)
      .getAllByRole('link')
      .filter((a) => a.classList.contains('on'))
      .map((a) => a.textContent);
    expect(active).toEqual(['Accounts']);
  });
});
