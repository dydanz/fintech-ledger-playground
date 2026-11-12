import { useEffect } from 'react';
import { Route, Routes, useNavigate } from 'react-router';
import { Layout } from './components/Layout';
import { Monitor } from './pages/Monitor';
import { Transactions } from './pages/Transactions';
import { Transaction } from './pages/Transaction';
import { Accounts } from './pages/Accounts';
import { Account } from './pages/Account';
import { MyAccount } from './pages/MyAccount';
import { Login } from './pages/Login';
import { Register } from './pages/Register';
import { Profile } from './pages/Profile';
import { NotFound } from './pages/NotFound';
import { RequireAuth } from './auth/RequireAuth';
import { setUnauthorizedHandler } from './api/client';

// The screens from spec/2026-11-10 Web Console Design §5. The ops screens
// (Monitor, Transactions, Accounts) need no login in the MVP; Profile and My
// account do.
export function App() {
  const navigate = useNavigate();
  useEffect(() => {
    setUnauthorizedHandler(() => navigate('/login', { replace: true }));
    return () => setUnauthorizedHandler(null);
  }, [navigate]);

  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Monitor />} />
        <Route path="transactions" element={<Transactions />} />
        <Route path="transactions/:id" element={<Transaction />} />
        <Route path="accounts" element={<Accounts />} />
        <Route path="accounts/:id" element={<Account />} />
        <Route path="login" element={<Login />} />
        <Route path="register" element={<Register />} />
        <Route path="profile" element={<RequireAuth><Profile /></RequireAuth>} />
        <Route path="me" element={<RequireAuth><MyAccount /></RequireAuth>} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
