import { useParams } from 'react-router';
import { useInfiniteQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { ErrorLine } from '../auth/Notice';
import { SignalsNote } from '../ops/SignalsNote';
import { TxTable } from '../ops/TxTable';
import { num, shortId, when } from '../ops/format';
import type { AccountDetail, AccountEvent } from '../ops/types';
import { StatusChip } from './Accounts';
import { notFound } from './Transaction';

const actorName: Record<string, string> = { 'fraud-engine': 'the fraud engine' };
const ruleName: Record<string, string> = {
  topup_velocity: 'too many top-ups in a short time',
  debit_volume: 'spending over the limit',
};

// historyLine says what an account event did, by whom and under which rule.
// The free-text reason an operator typed is never in the event (D§7.2).
function historyLine(e: AccountEvent): string {
  const by = e.payload.actor ? ` by ${actorName[e.payload.actor] ?? e.payload.actor}` : '';
  switch (e.type) {
    case 'AccountOpened':
      return 'Account opened';
    case 'LedgerOpened':
      return `${e.payload.currency ?? 'A'} ledger opened`;
    case 'AccountFrozen': {
      const rule = e.payload.rule ? `: ${ruleName[e.payload.rule] ?? e.payload.rule} (${e.payload.rule})` : '';
      return `Frozen${by}${rule}`;
    }
    case 'AccountUnfrozen':
      return `Unfrozen${by}`;
    default:
      return e.type;
  }
}

// The Account screen (D§5.3): balances, the status history, and the
// account's transactions, newest first, 20 at a time.
export function Account() {
  const { id = '' } = useParams();
  const q = useInfiniteQuery({
    queryKey: ['ops', 'account', id],
    queryFn: ({ pageParam }) =>
      api<AccountDetail>(`/ops/accounts/${encodeURIComponent(id)}${pageParam ? `?cursor=${encodeURIComponent(pageParam)}` : ''}`),
    initialPageParam: '',
    getNextPageParam: (last) => last.next_cursor || undefined,
  });
  const first = q.data?.pages[0];
  if (!first) {
    return (
      <section>
        <h1>Account</h1>
        {q.isError && <ErrorLine error={notFound(q.error, 'No account with this id.')} />}
      </section>
    );
  }
  const a = first.account;
  const items = q.data!.pages.flatMap((p) => p.transactions);
  // Newest first: the stream's own order reversed (events can share a time).
  const history = [...first.events].reverse();
  return (
    <section>
      <h1>Account {shortId(a.id)}</h1>
      <p className="sub">
        <StatusChip status={a.status} /> <span className="mono">{a.id}</span>
      </p>
      <ErrorLine error={q.error} />
      {q.data!.pages.some((p) => !p.signals.available) && <SignalsNote />}
      <div className="tiles" role="region" aria-label="Balances">
        {a.ledgers.map((l) => (
          <div className="tile" key={l.id}>
            <div className="k">{l.currency}</div>
            <div className="v">{num(l.balance)}</div>
          </div>
        ))}
        {a.ledgers.length === 0 && <p className="sub">No ledger yet.</p>}
      </div>
      <div className="grid">
        <div className="card">
          <h3>Transactions</h3>
          <TxTable label="Transactions of this account" items={items} account={false} />
          {items.length === 0 && <p className="sub">No transactions yet.</p>}
          {q.hasNextPage && (
            <button type="button" className="more" disabled={q.isFetchingNextPage} onClick={() => q.fetchNextPage()}>
              Load older
            </button>
          )}
        </div>
        <div className="card">
          <h3>Status history</h3>
          <ul className="healing" aria-label="Status history">
            {history.map((e, i) => (
              <li key={i}>
                <span className="when">{when(e.occurred_at)}</span>
                {historyLine(e)}
              </li>
            ))}
          </ul>
        </div>
      </div>
    </section>
  );
}
