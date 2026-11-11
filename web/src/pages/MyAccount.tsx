import { useState, type FormEvent } from 'react';
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, ApiError } from '../api/client';
import { ErrorLine } from '../auth/Notice';
import { newKey } from '../api/key';

type Ledger = { id: string; currency: string; balance: number };
type Account = { id: string; status: 'ACTIVE' | 'FROZEN'; ledgers: Ledger[] };
type Transaction = {
  id: string;
  type: 'CREDIT' | 'DEBIT' | 'VOID';
  status: 'PENDING' | 'PROCESSING' | 'SETTLED' | 'FAILED' | 'VOIDED';
  amount: number;
  currency: string;
  failure_reason?: string;
  created_at: string;
};
type Page = { transactions: Transaction[]; next_cursor?: string };

const money = (n: number, currency: string) => `${currency} ${n.toLocaleString('en-US')}`;

// What a demo action did, in plain words (D§6.2's labels, for one user).
export function outcome(res: { transaction?: Transaction } | undefined, err: unknown): string {
  if (err instanceof ApiError) {
    if (err.code === 'insufficient_funds') return 'Rejected: not enough balance. Nothing moved.';
    if (err.code === 'account_frozen') return 'Rejected: the account is frozen. Nothing moved.';
    return err.message;
  }
  if (err) return err instanceof Error ? err.message : String(err);
  const t = res?.transaction;
  if (!t) return '';
  if (t.status === 'SETTLED') return `Settled: ${t.type === 'CREDIT' ? 'added' : 'spent'} ${money(t.amount, t.currency)}.`;
  return 'Accepted and still processing; the recovery job will complete it.';
}

export function MyAccount() {
  const queryClient = useQueryClient();
  const account = useQuery({
    queryKey: ['me', 'account'],
    queryFn: () => api<{ account: Account }>('/me/account', { auth: true }).then((r) => r.account),
  });
  const history = useInfiniteQuery({
    queryKey: ['me', 'transactions'],
    queryFn: ({ pageParam }) => api<Page>(`/me/transactions?limit=10${pageParam ? `&cursor=${encodeURIComponent(pageParam)}` : ''}`, { auth: true }),
    initialPageParam: '',
    getNextPageParam: (last) => last.next_cursor,
    enabled: account.isSuccess,
  });
  const refresh = () => queryClient.invalidateQueries({ queryKey: ['me'] });

  const open = useMutation({ mutationFn: () => api('/me/open', { method: 'POST', auth: true }), onSuccess: refresh });

  const [amount, setAmount] = useState('10000');
  const action = useMutation({
    // A fresh key per click: a retried request is applied once, a new click
    // is a new transaction.
    mutationFn: (kind: 'credit' | 'debit') =>
      api<{ transaction: Transaction }>(`/me/${kind}`, {
        method: 'POST',
        auth: true,
        body: { amount: Number(amount) },
        headers: { 'Idempotency-Key': newKey() },
      }),
    onSettled: refresh,
  });

  if (account.isPending)
    return (
      <section>
        <h1>My account</h1>
        <p className="sub">Loading your account…</p>
      </section>
    );
  if (account.error instanceof ApiError && account.error.code === 'account_not_found') {
    return (
      <section className="narrow">
        <h1>My account</h1>
        <p className="sub">You don't have a ledger account yet.</p>
        <ErrorLine error={open.error} />
        <button type="button" onClick={() => open.mutate()} disabled={open.isPending}>
          Open my account
        </button>
      </section>
    );
  }
  if (account.isError)
    return (
      <section>
        <h1>My account</h1>
        <ErrorLine error={account.error} />
      </section>
    );

  const a = account.data;
  const disabled = action.error instanceof ApiError && action.error.code === 'demo_actions_disabled';
  return (
    <section>
      <h1>My account</h1>
      {a.status === 'FROZEN' && (
        <p className="error" role="alert">
          This account is frozen. Money can still come in, but spending is blocked.
        </p>
      )}
      <div className="tiles">
        {a.ledgers.map((l) => (
          <div className="tile" key={l.id}>
            <div className="k">Balance ({l.currency})</div>
            <div className="v" data-testid={`balance-${l.currency}`}>
              {money(l.balance, l.currency)}
            </div>
          </div>
        ))}
      </div>

      <form
        className="form narrow"
        onSubmit={(e: FormEvent) => e.preventDefault()}
        aria-label="Demo money"
      >
        <h2>Demo money</h2>
        <p className="sub">Real ledger transactions on your own account, in IDR. Four top-ups within 5 minutes trigger the fraud freeze.</p>
        <label>
          Amount <span className="hint">1 to 1,000,000</span>
          <input type="number" min={1} max={1000000} step={1} required value={amount} onChange={(e) => setAmount(e.target.value)} />
        </label>
        <div className="row">
          <button type="button" onClick={() => action.mutate('credit')} disabled={action.isPending || disabled}>
            Add demo money
          </button>
          <button type="button" onClick={() => action.mutate('debit')} disabled={action.isPending || disabled}>
            Spend
          </button>
        </div>
        {disabled ? (
          <p className="sub">Demo actions are turned off on this console.</p>
        ) : action.isSuccess ? (
          <p className="notice" role="status">
            {outcome(action.data, null)}
          </p>
        ) : action.isError ? (
          <p className="error" role="alert">
            {outcome(undefined, action.error)}
          </p>
        ) : null}
      </form>

      <h2>History</h2>
      <ErrorLine error={history.error} />
      <table className="table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Type</th>
            <th>Amount</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          {history.data?.pages.flatMap((p) => p.transactions).map((t) => (
            <tr key={t.id}>
              <td>{new Date(t.created_at).toLocaleString()}</td>
              <td>{t.type}</td>
              <td>{money(t.amount, t.currency)}</td>
              <td>
                {t.status}
                {t.failure_reason ? ` (${t.failure_reason.replace(/_/g, ' ')})` : ''}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {history.data?.pages[0]?.transactions.length === 0 && <p className="sub">No transactions yet.</p>}
      {history.hasNextPage && (
        <button type="button" onClick={() => history.fetchNextPage()} disabled={history.isFetchingNextPage}>
          Load more
        </button>
      )}
    </section>
  );
}
