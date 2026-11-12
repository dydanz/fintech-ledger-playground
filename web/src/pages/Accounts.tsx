import { Link } from 'react-router';
import { useInfiniteQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { ErrorLine } from '../auth/Notice';
import { money, shortId, when } from '../ops/format';
import type { Account, AccountPage } from '../ops/types';

export function StatusChip({ status }: { status: Account['status'] }) {
  return <span className={`chip ${status === 'FROZEN' ? 'c-rej' : 'c-ok'}`}>{status === 'FROZEN' ? 'Frozen' : 'Active'}</span>;
}

export const balances = (a: Account) => a.ledgers.map((l) => money(l.balance, l.currency)).join(' · ') || 'no ledger yet';

// Accounts: every account, in the order they were opened, 50 at a time.
// (The opening date stands in for "last activity", which the list query
// doesn't have.)
export function Accounts() {
  const q = useInfiniteQuery({
    queryKey: ['ops', 'accounts'],
    queryFn: ({ pageParam }) => api<AccountPage>(`/ops/accounts?limit=50${pageParam ? `&cursor=${encodeURIComponent(pageParam)}` : ''}`),
    initialPageParam: '',
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const accounts = q.data?.pages.flatMap((p) => p.accounts) ?? [];
  return (
    <section>
      <h1>Accounts</h1>
      <p className="sub">Every account, its status and its balances. Click one to see its history and transactions.</p>
      <ErrorLine error={q.error} />
      {q.isSuccess && (
        <div className="card">
          <table aria-label="Accounts">
            <thead>
              <tr>
                <th>Account</th>
                <th>Status</th>
                <th>Balances</th>
                <th>Opened</th>
              </tr>
            </thead>
            <tbody>
              {accounts.map((a) => (
                <tr key={a.id}>
                  <td className="mono" title={a.id}>
                    <Link to={`/accounts/${a.id}`}>{shortId(a.id)}</Link>
                  </td>
                  <td>
                    <StatusChip status={a.status} />
                  </td>
                  <td>{balances(a)}</td>
                  <td>{when(a.created_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {accounts.length === 0 && <p className="sub">No accounts yet.</p>}
          {q.hasNextPage && (
            <button type="button" className="more" disabled={q.isFetchingNextPage} onClick={() => q.fetchNextPage()}>
              Load more
            </button>
          )}
        </div>
      )}
    </section>
  );
}
