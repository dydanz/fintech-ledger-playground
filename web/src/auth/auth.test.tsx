import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { mockFetch, renderApp } from '../test/render';
import { safeNext } from './session';

const TOKEN_KEY = 'ledger-console.token';
const heading = (name: string) => screen.findByRole('heading', { level: 1, name });

async function fillLogin(email = 'a@example.com', password = 'correct horse battery') {
  const user = userEvent.setup();
  await user.type(screen.getByLabelText('Email'), email);
  await user.type(screen.getByLabelText('Password'), password);
  await user.click(screen.getByRole('button', { name: 'Log in' }));
}

describe('login', () => {
  it('shows the refusal and stores nothing on a wrong password', async () => {
    mockFetch({ 'POST /auth/login': { status: 401, body: { error: { code: 'invalid_credentials', message: 'invalid email or password' } } } });
    renderApp('/login');
    await fillLogin();
    expect(await screen.findByRole('alert')).toHaveTextContent('invalid email or password');
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
    expect(screen.getByRole('heading', { level: 1, name: 'Log in' })).toBeInTheDocument();
  });

  it('stores the token and opens My account', async () => {
    const calls = mockFetch({ 'POST /auth/login': { status: 200, body: { access_token: 'tok-1' } } });
    renderApp('/login');
    await fillLogin();
    await heading('My account');
    expect(sessionStorage.getItem(TOKEN_KEY)).toBe('tok-1');
    expect(calls[0].body).toEqual({ email: 'a@example.com', password: 'correct horse battery' });
  });

  it('returns to the page that asked for a login', async () => {
    mockFetch({
      'POST /auth/login': { status: 200, body: { access_token: 'tok-1' } },
      'GET /profile': { status: 200, body: { id: 'u1', email: 'a@example.com', full_name: '', bio: '' } },
    });
    renderApp('/profile');
    await heading('Log in');
    await fillLogin();
    await heading('Profile');
  });

  it('shows why the console is unreachable', async () => {
    mockFetch({ 'POST /auth/login': new TypeError('Failed to fetch') });
    renderApp('/login');
    await fillLogin();
    expect(await screen.findByRole('alert')).toHaveTextContent('could not be reached');
  });
});

describe('safeNext keeps redirects on this site', () => {
  it.each([
    ['/profile', '/profile'],
    ['/transactions/abc?x=1', '/transactions/abc?x=1'],
    ['//evil.example', '/me'],
    ['/\\evil.example', '/me'],
    ['https://evil.example', '/me'],
    [null, '/me'],
  ])('%s → %s', (next, want) => {
    expect(safeNext(next)).toBe(want);
  });
});

describe('a session that ends', () => {
  it('clears the token and asks for a login when an authenticated call gets 401', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'expired');
    mockFetch({ 'GET /profile': { status: 401, body: { error: { code: 'invalid_or_expired', message: 'token expired' } } } });
    renderApp('/profile');
    await heading('Log in');
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
    expect(screen.getByRole('status')).toHaveTextContent('Your session ended');
  });
});

describe('profile', () => {
  const profile = { id: 'u1', email: 'a@example.com', full_name: 'Ada', bio: 'hello' };

  it('sends the token and saves the details', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    const calls = mockFetch({
      'GET /profile': { status: 200, body: profile },
      'PUT /profile': { status: 200, body: { ...profile, full_name: 'Ada L.' } },
    });
    renderApp('/profile');
    const user = userEvent.setup();
    const name = await screen.findByLabelText('Full name');
    expect(name).toHaveValue('Ada');
    await user.clear(name);
    await user.type(name, 'Ada L.');
    await user.click(screen.getByRole('button', { name: 'Save' }));
    expect(await screen.findByText('Saved.')).toBeInTheDocument();
    expect(calls[0].headers.Authorization).toBe('Bearer tok-1');
    expect(calls.find((c) => c.method === 'PUT')?.body).toEqual({ full_name: 'Ada L.', bio: 'hello' });
  });

  it('logs out everywhere after a password change and asks for a new login', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    const calls = mockFetch({ 'GET /profile': { status: 200, body: profile }, 'PUT /password': { status: 204 } });
    renderApp('/profile');
    const user = userEvent.setup();
    await user.type(await screen.findByLabelText('Current password'), 'correct horse battery');
    await user.type(screen.getByLabelText(/^New password/), 'staple-anew-2026');
    await user.click(screen.getByRole('button', { name: 'Change password' }));
    await heading('Log in');
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
    expect(screen.getByRole('status')).toHaveTextContent('Password changed');
    expect(calls.find((c) => c.path === '/password')?.body).toEqual({ current_password: 'correct horse battery', new_password: 'staple-anew-2026' });
  });

  // 403, not 401: the session is still valid, so the user stays logged in.
  it('keeps the session and shows the message on a wrong current password', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    mockFetch({
      'GET /profile': { status: 200, body: profile },
      'PUT /password': { status: 403, body: { error: { code: 'invalid_credentials', message: 'current password is incorrect' } } },
    });
    renderApp('/profile');
    const user = userEvent.setup();
    await user.type(await screen.findByLabelText('Current password'), 'wrong one');
    await user.type(screen.getByLabelText(/^New password/), 'staple-anew-2026');
    await user.click(screen.getByRole('button', { name: 'Change password' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('current password is incorrect');
    expect(sessionStorage.getItem(TOKEN_KEY)).toBe('tok-1');
    expect(screen.getByRole('heading', { level: 1, name: 'Profile' })).toBeInTheDocument();
  });
});

describe('register', () => {
  async function fill() {
    const user = userEvent.setup();
    await user.type(screen.getByLabelText('Email'), 'new@example.com');
    await user.type(screen.getByLabelText(/^Password/), 'correct horse battery');
    await user.click(screen.getByRole('button', { name: 'Create account' }));
  }

  it('sends the new user to log in', async () => {
    mockFetch({ 'POST /auth/register': { status: 201, body: { id: 'u1', email: 'new@example.com' } } });
    renderApp('/register');
    await fill();
    await heading('Log in');
    expect(screen.getByRole('status')).toHaveTextContent('Account created');
  });

  it('shows why a registration is refused', async () => {
    mockFetch({ 'POST /auth/register': { status: 409, body: { error: { code: 'email_taken', message: 'email is already registered' } } } });
    renderApp('/register');
    await fill();
    expect(await screen.findByRole('alert')).toHaveTextContent('email is already registered');
  });
});

describe('logout', () => {
  it('ends the session upstream and here', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    const calls = mockFetch({ 'POST /auth/logout': { status: 204 } });
    renderApp('/');
    await userEvent.setup().click(screen.getByRole('button', { name: 'Log out' }));
    await heading('Log in');
    expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull();
    expect(calls[0]).toMatchObject({ method: 'POST', path: '/auth/logout' });
    expect(calls[0].headers.Authorization).toBe('Bearer tok-1');
  });

  it('still ends the session here when auth-api is unreachable', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    mockFetch({ 'POST /auth/logout': { status: 503, body: { error: { code: 'upstream_unavailable', message: 'auth-api is temporarily unavailable' } } } });
    renderApp('/');
    await userEvent.setup().click(screen.getByRole('button', { name: 'Log out' }));
    await heading('Log in');
    await waitFor(() => expect(sessionStorage.getItem(TOKEN_KEY)).toBeNull());
  });

  it('shows Log in when signed out, and Profile and Log out when signed in', () => {
    renderApp('/');
    expect(screen.getByRole('link', { name: 'Log in' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Log out' })).toBeNull();
  });
});

describe('logout from a page that needs a login', () => {
  it('keeps the logged-out notice instead of the guard redirect', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    mockFetch({ 'GET /profile': { status: 200, body: { id: 'u1', email: 'a@example.com', full_name: '', bio: '' } }, 'POST /auth/logout': { status: 204 } });
    renderApp('/profile');
    await heading('Profile');
    await userEvent.setup().click(screen.getByRole('button', { name: 'Log out' }));
    await heading('Log in');
    expect(screen.getByRole('status')).toHaveTextContent('Logged out.');
  });
});

describe('the flash notice', () => {
  it('is shown once, then gone on the next visit to the login page', async () => {
    sessionStorage.setItem(TOKEN_KEY, 'tok-1');
    mockFetch({ 'POST /auth/logout': { status: 204 } });
    const first = renderApp('/');
    await userEvent.setup().click(screen.getByRole('button', { name: 'Log out' }));
    await heading('Log in');
    expect(screen.getByRole('status')).toHaveTextContent('Logged out.');
    first.unmount();
    renderApp('/login');
    await heading('Log in');
    expect(screen.queryByRole('status')).toBeNull();
  });
});
