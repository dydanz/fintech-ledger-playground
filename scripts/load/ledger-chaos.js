// Steady ledger writes during a chaos fault, spec/TRD §13.5. Run with
// `make chaos FAULT=...` (scripts/chaos/run.sh), which injects the fault and
// checks the results.
//
// RATE writes/s (60% debit, 40% credit, 1–1,000 IDR) across ACCOUNTS accounts
// for DURATION. A request that gets a 5xx, no response, or 409
// request_in_progress is retried with the same Idempotency-Key every second
// for up to RETRY_FOR, as a payments client would. Every final answer is
// logged (JSON, source=console) with its key, transaction id, status and
// signed amount, so run.sh can compare the database with what the client was
// told: every confirmed transaction exists once, nothing else does, and each
// balance is the sum of its confirmed amounts.
//
// Users are inserted by run.sh with ids md5("chaos-RUN-i").
import http from 'k6/http';
import crypto from 'k6/crypto';
import { sleep } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const LEDGER = __ENV.LEDGER_URL || 'http://ledger-api:8081';
const ONBOARDING = __ENV.ONBOARDING_TOKEN || 'dev-onboarding-token';
const PAYMENTS = __ENV.PAYMENTS_TOKEN || 'dev-payments-token';
const RUN = __ENV.RUN;
const ACCOUNTS = parseInt(__ENV.ACCOUNTS || '100');
const RATE = parseInt(__ENV.RATE || '50');
const DURATION = __ENV.DURATION || '3m';
const RETRY_FOR = parseInt(__ENV.RETRY_FOR || '180'); // seconds
const FUNDING = 100000000;

if (!RUN) throw new Error('RUN is required (scripts/chaos/run.sh sets it)');

export const options = {
  scenarios: {
    writes: {
      executor: 'constant-arrival-rate', rate: RATE, timeUnit: '1s', duration: DURATION,
      // Retrying requests hold their VU: a 60 s outage at 50/s needs ~3,000.
      preAllocatedVUs: RATE, maxVUs: RATE * 80,
    },
  },
  // Every request ends in a final answer within RETRY_FOR (no loss as seen by
  // the client). How many needed a retry is the fault's visible impact.
  thresholds: { chaos_final_ok: ['rate==1'] },
  summaryTrendStats: ['avg', 'p(50)', 'p(99)', 'max'],
  setupTimeout: '5m',
};

const finalOK = new Rate('chaos_final_ok');
const firstTryOK = new Rate('chaos_first_try_ok');
const attempts = new Counter('chaos_attempts');

const params = (token, key) => ({
  headers: { Authorization: `Bearer ${token}`, 'Idempotency-Key': key, 'Content-Type': 'application/json' },
  timeout: '10s',
});

function userId(i) {
  const h = crypto.md5(`chaos-${RUN}-${i}`, 'hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// A final answer is 201/202 (transaction id in transaction.id) or a 422
// business outcome (a FAILED transaction, id in error.transaction_id).
function final(res) {
  if (res.status === 201 || res.status === 202) return { tx: res.json('transaction.id'), ok: true };
  if (res.status === 422) {
    try { return { tx: res.json('error.transaction_id'), code: res.json('error.code'), ok: true }; } catch (_) { /* not JSON */ }
  }
  return null; // 5xx, 0 (no response), 409 request_in_progress: retry
}

function log(entry) {
  console.log(JSON.stringify(Object.assign({ chaos: RUN }, entry)));
}

// Sends one logical request until it has a final answer. Returns the answer.
function send(url, body, key, delta, acct) {
  const started = Date.now();
  for (let n = 1; ; n++) {
    const res = http.post(url, body, params(PAYMENTS, key));
    attempts.add(1, { status: String(res.status) });
    const f = final(res);
    if (n === 1) firstTryOK.add(f !== null);
    if (f !== null) {
      finalOK.add(true);
      log({ key, acct, tx: f.tx, status: res.status, code: f.code || '', delta: res.status === 422 ? 0 : delta, tries: n, ms: Date.now() - started });
      return f;
    }
    if (n === 1 || res.status !== 503) {
      // The first failure of each request, and anything other than a 503.
      let code = '';
      try { code = res.json('error.code'); } catch (_) { /* not JSON */ }
      log({ key, attempt: n, status: res.status, code, error: res.error || '', at: new Date().toISOString() });
    }
    if ((Date.now() - started) / 1000 >= RETRY_FOR) {
      finalOK.add(false);
      log({ key, acct, gave_up: true, tries: n, last_status: res.status });
      return null;
    }
    sleep(1);
  }
}

export function setup() {
  const accounts = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const r = http.post(`${LEDGER}/internal/v1/accounts`, JSON.stringify({ user_id: userId(i) }), params(ONBOARDING, `chaos-open-${RUN}-${i}`));
    if (r.status !== 201) throw new Error(`open account ${i}: ${r.status} ${r.body}`);
    const id = r.json('account.id');
    const fund = send(`${LEDGER}/internal/v1/accounts/${id}/credits`, JSON.stringify({ amount: FUNDING, currency: 'IDR' }),
      `chaos-fund-${RUN}-${i}`, FUNDING, id);
    if (!fund) throw new Error(`fund account ${i}`);
    accounts.push(id);
  }
  return { accounts };
}

export default function (data) {
  const acct = data.accounts[Math.floor(Math.random() * data.accounts.length)];
  const key = `chaos-${RUN}-${exec.scenario.iterationInTest}`;
  const amount = 1 + Math.floor(Math.random() * 1000);
  const debit = Math.random() < 0.6;
  send(`${LEDGER}/internal/v1/accounts/${acct}/${debit ? 'debits' : 'credits'}`,
    JSON.stringify({ amount, currency: 'IDR' }), key, debit ? -amount : amount, acct);
}
