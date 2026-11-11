import { NavLink, Outlet } from 'react-router';

const links = [
  { to: '/', label: 'Monitor', end: true },
  { to: '/transactions', label: 'Transactions' },
  { to: '/accounts', label: 'Accounts' },
  { to: '/me', label: 'My account' },
];

export function Layout() {
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
      </header>
      <main className="page">
        <Outlet />
      </main>
    </>
  );
}
