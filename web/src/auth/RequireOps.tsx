import { Link } from 'react-router';
import type { ReactNode } from 'react';
import { ErrorLine } from './Notice';
import { useProfile } from './useProfile';

// RequireOps shows the ops screens only to a user with the ops role (D§7.5).
// The console checks the role again on every ops request; this keeps a plain
// user from seeing a screen of errors. Use inside RequireAuth.
export function RequireOps({ children }: { children: ReactNode }) {
  const profile = useProfile();
  if (profile.isError) return <ErrorLine error={profile.error} />;
  if (!profile.data) return null;
  if (profile.data.role !== 'ops') {
    return (
      <section>
        <h1>Ops access needed</h1>
        <p className="sub">
          The Monitor, Transactions and Accounts show every customer's money, so they're for ops users only. Your account doesn't have that
          role. See your own balance and history on <Link to="/me">My account</Link>.
        </p>
        <p className="hint">An administrator grants it with make grant-role EMAIL=&lt;your email&gt;.</p>
      </section>
    );
  }
  return <>{children}</>;
}
