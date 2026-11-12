import { useState } from 'react';
import { Link } from 'react-router';
import { useQuery } from '@tanstack/react-query';
import { api } from '../api/client';
import { ErrorLine } from '../auth/Notice';
import { LabelChip, labelName } from '../ops/LabelChip';
import { between, clock, duration, money, num, percent, shortId } from '../ops/format';
import type { FeedItem, Incident, Label, Monitor as MonitorData } from '../ops/types';

// POLL_MS is how often the Monitor refreshes while its tab is visible
// (TanStack pauses the interval in a background tab, D§10 "about 3 s").
export const POLL_MS = 3000;

const stuckAfterMs = 60_000; // NFR-7, as labels.go
const outboxSlowMs = 5_000; // the "outbox lag above 5 s" alert

// The Monitor (D§5.1): is money handled correctly right now, what went
// wrong, and how did the system recover? Polls the console's ops API.
export function Monitor() {
  const q = useQuery({
    queryKey: ['ops', 'monitor'],
    queryFn: () => api<MonitorData>('/ops/monitor'),
    refetchInterval: POLL_MS,
  });
  const m = q.data;
  return (
    <section>
      <h1>Monitor</h1>
      <p className="sub">
        Is money handled correctly right now, what went wrong, and how did the system recover? The last 10 minutes, refreshed every {POLL_MS / 1000} s
        {m ? `; updated ${clock(m.generated_at)}` : ''}.
      </p>
      {q.isError && <ErrorLine error={q.error} />}
      {m && <Body m={m} />}
    </section>
  );
}

function Body({ m }: { m: MonitorData }) {
  const signals = m.signals.available;
  return (
    <>
      {!signals && (
        <p className="signals-note" role="note">
          <b>Activity signals unavailable.</b> The logs (Loki) can't be read right now, so outages, recoveries and the reconciler's result aren't
          shown. Balances and transactions come from the ledger itself and are complete.
        </p>
      )}
      <Tiles m={m} />
      {signals && <Incidents incidents={m.incidents ?? []} now={m.generated_at} />}
      <div className="grid">
        <Feed feed={m.feed ?? []} />
        <div className="card">
          <Handled m={m} />
          {signals && <SelfHealing items={m.self_healing ?? []} />}
        </div>
      </div>
    </>
  );
}

function Tile({ k, v, s, tone }: { k: string; v: string; s: string; tone?: string }) {
  return (
    <div className="tile">
      <div className="k">{k}</div>
      <div className={`v ${tone ?? ''}`.trim()}>{v}</div>
      <div className="s">{s}</div>
    </div>
  );
}

function Tiles({ m }: { m: MonitorData }) {
  const t = m.tiles;
  const signals = m.signals.available;
  const timedOut = m.handled.find((h) => h.label === 'timed_out')?.count ?? 0;
  const nr = t.needed_recovery;
  const stuck = nr.still_pending > 0 && nr.oldest_pending_ms > stuckAfterMs;
  const stuckText = stuck ? `${num(nr.still_pending)} pending, oldest ${duration(nr.oldest_pending_ms)}` : 'none stuck';
  const ok = t.handled_correctly.ratio >= 0.999 && !stuck && timedOut === 0;

  const rc = t.reconciler;
  const books: Record<typeof rc.status, [string, string]> = {
    clean: ['✓ clean', 'good'],
    mismatches: [`✗ ${num(rc.mismatches)} mismatches`, 'bad'],
    failed: ['✗ check failed', 'bad'],
    unknown: ['unknown', ''],
  };
  const booksSub = !signals
    ? 'needs the log signals'
    : rc.at
      ? `reconciler, ${between(rc.at, m.generated_at)} ago${rc.mode ? ` · ${rc.mode}` : ''}`
      : 'no reconciler run in the last 30 min';

  const ob = t.outbox;
  return (
    <div className="tiles" role="region" aria-label="Totals">
      <Tile k="Transactions (last 10 min)" v={num(t.transactions.count)} s={`${t.transactions.per_second.toFixed(1)} / s`} />
      <Tile
        k="Handled correctly"
        v={percent(t.handled_correctly.ratio)}
        tone={ok ? 'good' : 'warnc'}
        s={`${stuckText} · ${num(timedOut)} timed out${signals ? '' : ' · refusals unknown'}`}
      />
      <Tile
        k="Needed recovery"
        v={signals ? num(nr.recovered + nr.refused) : 'unknown'}
        s={nr.still_pending === 0 ? 'all completed ✓' : `${num(nr.still_pending)} still pending`}
      />
      <Tile k="Books balanced" v={books[rc.status][0]} tone={books[rc.status][1]} s={booksSub} />
      <Tile
        k="Events to Kafka"
        v={`${num(ob.pending)} waiting`}
        tone={ob.oldest_age_ms > outboxSlowMs ? 'warnc' : ''}
        s={ob.pending > 0 ? `oldest ${duration(ob.oldest_age_ms)}` : 'all delivered'}
      />
    </div>
  );
}

// Incidents: one banner each, the active ones first, then the newest.
function Incidents({ incidents, now }: { incidents: Incident[]; now: string }) {
  const sorted = [...incidents].sort((a, b) => Number(b.active) - Number(a.active) || b.started_at.localeCompare(a.started_at));
  return (
    <>
      {sorted.map((inc) => {
        const head = inc.active
          ? `Incident in progress: ${inc.title} since ${clock(inc.started_at)} (${between(inc.started_at, now)})`
          : `Resolved: ${inc.title}, ${clock(inc.started_at)} to ${clock(inc.ended_at!)} (${between(inc.started_at, inc.ended_at!)})`;
        return (
          <div key={`${inc.kind}-${inc.started_at}`} className={inc.active ? 'banner' : 'banner resolved'} role="status" aria-label={head}>
            <div className="icon" aria-hidden>
              {inc.active ? '⚠️' : '✅'}
            </div>
            <div>
              <b>{head}</b>
              <br />
              {inc.text}
            </div>
          </div>
        );
      })}
    </>
  );
}

const labels = Object.keys(labelName) as Label[];
const typeName = { CREDIT: 'Credit', DEBIT: 'Debit', VOID: 'Void' };

function Feed({ feed }: { feed: FeedItem[] }) {
  const [only, setOnly] = useState<Label | null>(null);
  const shown = only ? feed.filter((f) => f.label === only) : feed;
  const filter = (l: Label | null, name: string, n: number) => (
    <button key={name} type="button" className={`pill${only === l ? ' on' : ''}`} aria-pressed={only === l} onClick={() => setOnly(l)}>
      {name} ({n})
    </button>
  );
  return (
    <div className="card">
      <h3>Live transactions</h3>
      <div className="filters">
        {filter(null, 'All', feed.length)}
        {labels.map((l) => filter(l, labelName[l], feed.filter((f) => f.label === l).length))}
      </div>
      <table aria-label="Live transactions">
        <thead>
          <tr>
            <th>Time</th>
            <th>Account</th>
            <th>Type</th>
            <th className="num">Amount</th>
            <th>Outcome</th>
            <th>What happened</th>
          </tr>
        </thead>
        <tbody>
          {shown.map(({ transaction: t, label, what, attention }) => (
            <tr key={t.id}>
              <td>{clock(t.updated_at)}</td>
              <td className="mono" title={t.account_id}>
                {shortId(t.account_id)}
              </td>
              <td>{typeName[t.type]}</td>
              <td className="num">{money(t.amount, t.currency)}</td>
              <td>
                <LabelChip label={label} attention={attention} />
              </td>
              <td>
                <Link to={`/transactions/${t.id}`}>{what}</Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {feed.length === 0 && <p className="sub">No transactions yet.</p>}
      {feed.length > 0 && shown.length === 0 && (
        <p className="sub">
          None of the newest {feed.length} transactions is {labelName[only!]}.
        </p>
      )}
    </div>
  );
}

const handledName: Record<MonitorData['handled'][number]['label'], [string, string]> = {
  normal: ['Settled normally', 'var(--ok)'],
  recovered: ['Completed by the recovery job', 'var(--info)'],
  refused_try_again: ['Refused with "try again" during an outage', 'var(--info)'],
  rejected: ['Rejected by a business rule', 'var(--bad)'],
  reversed: ['Reversed (voids)', 'var(--warn)'],
  timed_out: ['Timed out', 'var(--bad)'],
  in_progress: ['Still in progress', 'var(--muted)'],
};

// Handled: a bar per way of handling, for the window. Recoveries and
// refusals are known only from the logs, so without them they say "unknown".
function Handled({ m }: { m: MonitorData }) {
  const fromLogs = (l: string) => l === 'recovered' || l === 'refused_try_again';
  const known = m.handled.filter((h) => m.signals.available || !fromLogs(h.label));
  const top = Math.max(1, ...known.map((h) => h.count));
  return (
    <>
      <h3>How the last 10 minutes were handled</h3>
      <ul className="bars" aria-label="How it was handled">
        {m.handled.map((h) => {
          const [name, color] = handledName[h.label];
          const unknown = !m.signals.available && fromLogs(h.label);
          const width = unknown || h.count === 0 ? 0 : Math.max(3, (h.count / top) * 180);
          return (
            <li key={h.label}>
              <i style={{ width, background: color }} />
              {name} · {unknown ? 'unknown' : num(h.count)}
            </li>
          );
        })}
      </ul>
    </>
  );
}

function SelfHealing({ items }: { items: { at: string; text: string }[] }) {
  return (
    <>
      <h3 className="spaced">Self-healing activity</h3>
      {items.length === 0 ? (
        <p className="sub">Nothing needed fixing in the last 30 minutes.</p>
      ) : (
        <ul className="healing" aria-label="Self-healing activity">
          {items.map((h, i) => (
            <li key={i}>
              <span className="when">{clock(h.at)}</span>
              {h.text}
            </li>
          ))}
        </ul>
      )}
    </>
  );
}
