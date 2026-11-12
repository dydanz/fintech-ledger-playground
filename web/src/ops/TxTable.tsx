import { Link } from 'react-router';
import { LabelChip } from './LabelChip';
import { clock, money, shortId } from './format';
import type { FeedItem } from './types';

const typeName = { CREDIT: 'Credit', DEBIT: 'Debit', VOID: 'Void' };

// TxTable lists labelled transactions (the Monitor's feed, the Transactions
// list, an account's page). "What happened" links to the transaction.
export function TxTable({ label, items, account = true }: { label: string; items: FeedItem[]; account?: boolean }) {
  return (
    <table aria-label={label}>
      <thead>
        <tr>
          <th>Time</th>
          {account && <th>Account</th>}
          <th>Type</th>
          <th className="num">Amount</th>
          <th>Outcome</th>
          <th>What happened</th>
        </tr>
      </thead>
      <tbody>
        {items.map(({ transaction: t, label: l, what, attention }) => (
          <tr key={t.id}>
            <td>{clock(t.updated_at)}</td>
            {account && (
              <td className="mono" title={t.account_id}>
                <Link to={`/accounts/${t.account_id}`}>{shortId(t.account_id)}</Link>
              </td>
            )}
            <td>{typeName[t.type]}</td>
            <td className="num">{money(t.amount, t.currency)}</td>
            <td>
              <LabelChip label={l} attention={attention} />
            </td>
            <td>
              <Link to={`/transactions/${t.id}`}>{what}</Link>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
