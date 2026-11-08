// Ledger load test, spec/TRD §13.4. Run with `make load` (scripts/load/run.sh),
// which seeds the users, samples the outbox lag and checks the reconciler.
//
// PROFILE=full (default): 30 minutes of mixed writes at MIXED_RATE (300/s:
// 60% debit, 35% credit, 5% void) across ACCOUNTS (10,000) accounts, plus
// HOT_ACCOUNTS (10) hot accounts at HOT_RATE (20/s) each, then a SPIKE_RATE
// (600/s) mixed spike for SPIKE_DURATION (5m). PROFILE=quick: the same shape
// in about 2 minutes, to check the setup.
//
// Users are inserted by scripts/load/seed-users.sh with ids md5("load-RUN-i"),
// because registering 10,000 users through auth-api (bcrypt cost 12) would take
// most of an hour; accounts are opened and funded through ledger-api.
import http from 'k6/http';
import crypto from 'k6/crypto';
import { check } from 'k6';
import exec from 'k6/execution';
import { Counter, Rate } from 'k6/metrics';

const LEDGER = __ENV.LEDGER_URL || 'http://ledger-api:8081';
const ONBOARDING = __ENV.ONBOARDING_TOKEN || 'dev-onboarding-token';
const PAYMENTS = __ENV.PAYMENTS_TOKEN || 'dev-payments-token';
const OPS = __ENV.OPS_TOKEN || 'dev-ops-token';
const RUN = __ENV.RUN;
const PROFILE = __ENV.PROFILE || 'full';
const quick = PROFILE === 'quick';
const ACCOUNTS = parseInt(__ENV.ACCOUNTS || (quick ? '1000' : '10000'));
const HOT_ACCOUNTS = parseInt(__ENV.HOT_ACCOUNTS || '10');
const MIXED_RATE = parseInt(__ENV.MIXED_RATE || '300');
const HOT_RATE = parseInt(__ENV.HOT_RATE || '20');
const SPIKE_RATE = parseInt(__ENV.SPIKE_RATE || '600');
const DURATION = __ENV.DURATION || (quick ? '90s' : '30m');
const SPIKE_DURATION = __ENV.SPIKE_DURATION || (quick ? '30s' : '5m');
const FUNDING = 100000000; // IDR minor units per account, so debits rarely hit the floor

if (!RUN) throw new Error('RUN is required (scripts/load/run.sh sets it)');
if (!['full', 'quick'].includes(PROFILE)) throw new Error(`PROFILE must be full or quick, not ${PROFILE}`);

// k6 durations like "30m", "90s" or "1h" in seconds, for the spike's start time.
function seconds(d) {
  const m = /^(\d+)(s|m|h)$/.exec(d);
  if (!m) throw new Error(`duration ${d}: use <n>s, <n>m or <n>h`);
  return parseInt(m[1]) * { s: 1, m: 60, h: 3600 }[m[2]];
}

const writes = (rate, duration, extra) => Object.assign({
  executor: 'constant-arrival-rate', rate, timeUnit: '1s', duration,
  preAllocatedVUs: Math.max(50, rate / 2), maxVUs: rate * 2,
}, extra);

export const options = {
  scenarios: {
    mixed: writes(MIXED_RATE, DURATION, { exec: 'mixed', tags: { phase: 'steady' } }),
    hot: writes(HOT_ACCOUNTS * HOT_RATE, DURATION, { exec: 'hot', tags: { phase: 'steady' } }),
    spike: writes(SPIKE_RATE, SPIKE_DURATION, { exec: 'mixed', startTime: `${seconds(DURATION) + 10}s`, tags: { phase: 'spike' } }),
  },
  // NFR-2 (steady state, 300 TPS + hot accounts) and NFR-5 (both phases).
  // NFR-1 is the achieved rate: dropped_iterations stays 0 when k6 could
  // start every request on time, which it can't if the API falls behind.
  thresholds: {
    'http_req_duration{phase:steady}': ['p(50)<30', 'p(99)<150'],
    'ledger_success{phase:steady}': ['rate>=0.9999'],
    'ledger_success{phase:spike}': ['rate>=0.9999'],
    'dropped_iterations': ['count==0'],
  },
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  setupTimeout: '15m',
};

// NFR-5: 1 − system failures / valid requests. 5xx is a system failure;
// business outcomes (insufficient_funds, account_frozen, not_voidable) are
// correct answers. A 202 is not a failure here: the transaction is PENDING and
// run.sh counts the ones that later end FAILED processing_timeout.
const success = new Rate('ledger_success');
const outcomes = new Counter('ledger_outcomes');
const BUSINESS = ['insufficient_funds', 'account_frozen', 'not_voidable'];

const json = { 'Content-Type': 'application/json' };
const headers = (token, key) => ({ headers: Object.assign({ Authorization: `Bearer ${token}`, 'Idempotency-Key': key }, json) });

function userId(i) {
  const h = crypto.md5(`load-${RUN}-${i}`, 'hex');
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}

// Runs requests in batches and returns the responses, failing setup on any
// status other than `want`.
function batched(reqs, want, what) {
  const out = [];
  for (let i = 0; i < reqs.length; i += 200) {
    for (const [j, r] of http.batch(reqs.slice(i, i + 200)).entries()) {
      if (r.status !== want) throw new Error(`${what} ${i + j}: ${r.status} ${r.body}`);
      out.push(r);
    }
  }
  return out;
}

export function setup() {
  const total = ACCOUNTS + HOT_ACCOUNTS;
  const opened = batched(Array.from({ length: total }, (_, i) => ['POST', `${LEDGER}/internal/v1/accounts`,
    JSON.stringify({ user_id: userId(i) }), headers(ONBOARDING, `load-open-${RUN}-${i}`)]), 201, 'open account');
  const ids = opened.map((r) => r.json('account.id'));
  batched(ids.map((id, i) => ['POST', `${LEDGER}/internal/v1/accounts/${id}/credits`,
    JSON.stringify({ amount: FUNDING, currency: 'IDR' }), headers(PAYMENTS, `load-fund-${RUN}-${i}`)]), 201, 'fund account');
  return { accounts: ids.slice(0, ACCOUNTS), hot: ids.slice(ACCOUNTS) };
}

// Each VU remembers transactions it settled, to void some of them.
const settled = [];

function record(res, op) {
  let code = '';
  if (res.status === 422) {
    try { code = res.json('error.code'); } catch (_) { /* not JSON */ }
  }
  const business = res.status === 422 && BUSINESS.includes(code);
  const ok = res.status === 201 || res.status === 202 || business;
  success.add(res.status < 500, { op });
  outcomes.add(1, { op, status: String(res.status), code: business ? code : '' });
  check(res, { 'settled, pending or a business outcome': () => ok });
  if (res.status === 201 && op !== 'void') {
    settled.push(res.json('transaction.id'));
    if (settled.length > 50) settled.shift();
  }
}

function write(acct) {
  // Unique per request: each scenario numbers its iterations across all VUs.
  const key = `load-${RUN}-${exec.scenario.name}-${exec.scenario.iterationInTest}`;
  const amount = 1 + Math.floor(Math.random() * 1000);
  const p = Math.random();
  if (p < 0.05 && settled.length > 0) {
    const id = settled.splice(Math.floor(Math.random() * settled.length), 1)[0];
    record(http.post(`${LEDGER}/internal/v1/transactions/${id}/void`, JSON.stringify({ reason: 'load test' }),
      Object.assign(headers(OPS, key), { tags: { name: 'void' } })), 'void');
  } else if (p < 0.40) {
    record(http.post(`${LEDGER}/internal/v1/accounts/${acct}/credits`, JSON.stringify({ amount, currency: 'IDR' }),
      Object.assign(headers(PAYMENTS, key), { tags: { name: 'credit' } })), 'credit');
  } else {
    record(http.post(`${LEDGER}/internal/v1/accounts/${acct}/debits`, JSON.stringify({ amount, currency: 'IDR' }),
      Object.assign(headers(PAYMENTS, key), { tags: { name: 'debit' } })), 'debit');
  }
}

export function mixed(data) {
  write(data.accounts[Math.floor(Math.random() * data.accounts.length)]);
}

export function hot(data) {
  write(data.hot[Math.floor(Math.random() * data.hot.length)]);
}
