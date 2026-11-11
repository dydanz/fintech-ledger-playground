import { Route, Routes } from 'react-router';
import { Layout } from './components/Layout';
import { Monitor } from './pages/Monitor';
import { Transactions } from './pages/Transactions';
import { Accounts } from './pages/Accounts';
import { MyAccount } from './pages/MyAccount';
import { NotFound } from './pages/NotFound';

// The screens from spec/2026-11-10 Web Console Design §5. They are
// placeholders until C2.2 (auth pages), C2.3 (My account) and C3 (the ops
// screens) fill them in.
export function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route index element={<Monitor />} />
        <Route path="transactions/*" element={<Transactions />} />
        <Route path="accounts/*" element={<Accounts />} />
        <Route path="me" element={<MyAccount />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
