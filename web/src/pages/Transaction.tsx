import { Link, useParams } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { api, ApiError } from '../api/client';
import { ErrorLine } from '../auth/Notice';
import { LabelChip } from '../ops/LabelChip';
import { money, shortId, when } from '../ops/format';
import type { JournalLine, TimelineEntry, TransactionDetail } from '../ops/types';

const typeName = { CREDIT: 'Credit', DEBIT: 'Debit', VOID: 'Void' };
const stepClass: Record<TimelineEntry['kind'], string> = { ok: 'ok', recovered: 'rec', refused: 'bad', info: 'info' };

// The Transaction screen (D§5.2): what happened to one transaction, step by
// step, the money it moved, and how to trace it. Refreshes while it's still
// in progress.
export function Transaction() {
  const { id = '' } = useParams();
  const q = useQuery({
    queryKey: ['ops', 'transaction', id],
    queryFn: () => api<TransactionDetail>(`/ops/transactions/${encodeURIComponent(id)}`, { auth: true }),
    refetchInterval: (query) => {
      const s = query.state.data?.transaction.status;
      return s === 'PENDING' || s === 'PROCESSING' ? 5000 : false;
    },
  });
  const d = q.data;
  if (!d) {
    return (
      <section>
        <h1>Transaction</h1>
        {q.isError && <ErrorLine error={notFound(q.error, 'No transaction with this id.')} />}
      </section>
    );
  }
  const t = d.transaction;
  return (
    <section>
      <h1>
        {typeName[t.type]} · {money(t.amount, t.currency)}
      </h1>
      <p className="sub">
        <LabelChip label={d.label.label} attention={d.label.attention} /> {d.label.what}
      </p>
      {q.isError && <ErrorLine error={q.error} />}
      {!d.signals.available && (
        <p className="signals-note" role="note">
          <b>Activity signals unavailable.</b> The logs can't be read right now, so the recovery, fraud and reconciler steps aren't shown. The
          steps from the ledger's own record are complete.
        </p>
      )}
      <div className="grid">
        <div className="card">
          <h3>What happened</h3>
          <Timeline steps={d.timeline} />
        </div>
        <div>
          <div className="card">
            <h3>Double entry</h3>
            <Journal lines={d.journal} status={t.status} />
          </div>
          <Traceability d={d} />
        </div>
      </div>
    </section>
  );
}

// notFound turns a 404 into a plain sentence; other errors pass through.
export function notFound(err: unknown, text: string): unknown {
  return err instanceof ApiError && err.status === 404 ? new Error(text) : err;
}

function Timeline({ steps }: { steps: TimelineEntry[] }) {
  // Sorted here too: the order is the point of the screen.
  const sorted = [...steps].sort((a, b) => Date.parse(a.at) - Date.parse(b.at));
  return (
    <ol className="timeline" aria-label="Timeline">
      {sorted.map((s, i) => (
        <li key={i} className={stepClass[s.kind]}>
          <div>
            <span className="t" data-testid="step-title">
              {s.title}
            </span>{' '}
            <span className={`source ${s.source}`}>{s.source === 'ledger' ? 'ledger record' : 'logs'}</span>
          </div>
          {s.actor && (
            <div className="actor" data-testid="step-actor">
              by {s.actor}
            </div>
          )}
          <div className="when">{when(s.at)}</div>
          {s.detail && <div>{s.detail}</div>}
        </li>
      ))}
    </ol>
  );
}

// Journal: the two signed lines of a settled transaction, as money in and
// money out of each ledger. They always sum to zero; the screen says so.
function Journal({ lines, status }: { lines: JournalLine[]; status: string }) {
  if (lines.length === 0) {
    const why =
      status === 'FAILED' ? 'No money moved: the transaction was refused.' : 'No money has moved yet: the transaction is still in progress.';
    return <p className="sub">{why}</p>;
  }
  const cur = lines[0].currency;
  const into = lines.filter((l) => l.amount > 0).reduce((n, l) => n + l.amount, 0);
  const out = lines.filter((l) => l.amount < 0).reduce((n, l) => n - l.amount, 0);
  return (
    <>
      <table aria-label="Double entry">
        <thead>
          <tr>
            <th>Ledger</th>
            <th className="num">Money in</th>
            <th className="num">Money out</th>
          </tr>
        </thead>
        <tbody>
          {lines.map((l) => (
            <tr key={l.id}>
              <td>
                {l.ledger_kind === 'USER' && l.account_id ? (
                  <>
                    Account <Link to={`/accounts/${l.account_id}`}>{shortId(l.account_id)}</Link>
                  </>
                ) : (
                  `System: ${l.ledger_code ?? 'ledger'}`
                )}
              </td>
              <td className="num">{l.amount > 0 ? money(l.amount, l.currency) : ''}</td>
              <td className="num">{l.amount < 0 ? money(-l.amount, l.currency) : ''}</td>
            </tr>
          ))}
          <tr className="total">
            <td>Total</td>
            <td className="num">{money(into, cur)}</td>
            <td className="num">{money(out, cur)}</td>
          </tr>
        </tbody>
      </table>
      {into === out ? (
        <p className="good">Balanced: money in equals money out.</p>
      ) : (
        <p className="bad">Not balanced: money in and money out differ. The reconciler reports this.</p>
      )}
    </>
  );
}

function Traceability({ d }: { d: TransactionDetail }) {
  const t = d.transaction;
  const txLink = (id: string) => <Link to={`/transactions/${id}`}>{shortId(id)}</Link>;
  return (
    <div className="card spaced-card" role="region" aria-label="Traceability">
      <h3>Traceability</h3>
      <dl className="facts">
        <dt>Transaction</dt>
        <dd className="mono">{t.id}</dd>
        <dt>Account</dt>
        <dd className="mono">
          <Link to={`/accounts/${t.account_id}`}>{t.account_id}</Link>
        </dd>
        <dt>Requested by</dt>
        <dd>{t.requested_by}</dd>
        <dt>Trace id</dt>
        <dd>
          <span className="mono">{t.trace_id}</span>
          {d.explore_url && (
            <>
              <br />
              <a href={d.explore_url} target="_blank" rel="noreferrer">
                Open its logs in Grafana ↗
              </a>
            </>
          )}
        </dd>
        {t.original_transaction_id && (
          <>
            <dt>Reverses</dt>
            <dd className="mono">{txLink(t.original_transaction_id)}</dd>
          </>
        )}
        {d.reversed_by && (
          <>
            <dt>Reversed by</dt>
            <dd className="mono">{txLink(d.reversed_by)}</dd>
          </>
        )}
        <dt>Created</dt>
        <dd>{when(t.created_at)}</dd>
        {t.settled_at && (
          <>
            <dt>Settled</dt>
            <dd>{when(t.settled_at)}</dd>
          </>
        )}
      </dl>
    </div>
  );
}
