import { Link, NavLink, Outlet, useNavigate } from 'react-router';
import { useQueryClient } from '@tanstack/react-query';
import { api } from '../api/client';
import { session, useToken } from '../auth/session';

const links = [
  { to: '/', label: 'Monitor', end: true },
  { to: '/transactions', label: 'Transactions' },
  { to: '/accounts', label: 'Accounts' },
  { to: '/me', label: 'My account' },
];

export function Layout() {
  const token = useToken();
  const navigate = useNavigate();
  const queryClient = useQueryClient();

  // Logging out always ends the session here, even if auth-api can't be
  // reached; the server-side session then expires on its own.
  async function logout() {
    try {
      await api('/auth/logout', { method: 'POST', auth: true });
    } catch {
      /* the local session is cleared either way */
    }
    session.end('Logged out.');
    queryClient.clear();
    navigate('/login', { replace: true });
  }

  return (
    <>
      <header className="topbar">
        <b className="brand">fintech-ledger-playground</b>
        <nav aria-label="Main">
          {links.map((l) => (
            <NavLink key={l.to} to={l.to} end={l.end} className={({ isActive }) => (isActive ? 'on' : undefined)}>
              {l.label}
            </NavLink>
          ))}
        </nav>
        <span className="viewer">ops viewer · read only</span>
        <span className="account" aria-label="Account">
          {token ? (
            <>
              <Link to="/profile">Profile</Link>
              <button type="button" className="link" onClick={logout}>
                Log out
              </button>
            </>
          ) : (
            <Link to="/login">Log in</Link>
          )}
        </span>
      </header>
      <main className="page">
        <Outlet />
      </main>
    </>
  );
}
