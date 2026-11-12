import { useState } from 'react';
import { useInfiniteQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { ErrorLine } from '../auth/Notice';
import { SignalsNote } from '../ops/SignalsNote';
import { TxTable } from '../ops/TxTable';
import type { TxPage } from '../ops/types';

const statuses = ['PENDING', 'SETTLED', 'FAILED', 'VOIDED'];
const types = ['CREDIT', 'DEBIT', 'VOID'];
const title = (s: string) => s[0] + s.slice(1).toLowerCase();

// Transactions: every account's transactions, newest change first, filtered
// by status and type, 50 at a time.
export function Transactions() {
  const [status, setStatus] = useState('');
  const [type, setType] = useState('');
  const q = useInfiniteQuery({
    queryKey: ['ops', 'transactions', status, type],
    queryFn: ({ pageParam }) => {
      const p = new URLSearchParams({ limit: '50' });
      if (status) p.set('status', status);
      if (type) p.set('type', type);
      if (pageParam) p.set('cursor', pageParam);
      return api<TxPage>(`/ops/transactions?${p}`, { auth: true });
    },
    initialPageParam: '',
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const items = q.data?.pages.flatMap((p) => p.transactions) ?? [];
  const select = (label: string, value: string, set: (v: string) => void, options: string[]) => (
    <label className="inline">
      {label}{' '}
      <select value={value} onChange={(e) => set(e.target.value)}>
        <option value="">All</option>
        {options.map((o) => (
          <option key={o} value={o}>
            {title(o)}
          </option>
        ))}
      </select>
    </label>
  );
  return (
    <section>
      <h1>Transactions</h1>
      <p className="sub">Every account's transactions, the most recently changed first. Click one to see its full audit trail.</p>
      <div className="filters">
        {select('Status', status, setStatus, statuses)}
        {select('Type', type, setType, types)}
      </div>
      <ErrorLine error={q.error} />
      {q.data?.pages.some((p) => !p.signals.available) && <SignalsNote />}
      {q.isSuccess && (
        <div className="card">
          <TxTable label="Transactions" items={items} />
          {items.length === 0 && <p className="sub">No transactions match.</p>}
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
