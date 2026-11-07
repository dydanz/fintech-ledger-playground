// Minimal load for the outbox lag check (spec/TRD §14 P3 exit): credits at
// a constant RATE (default 300/s) for DURATION across ACCOUNTS accounts.
// Run with `make load`; scripts/load/outbox-lag.sh samples the lag meanwhile.
import http from 'k6/http';
import { check } from 'k6';

const AUTH = __ENV.AUTH_URL || 'http://api:8080';
const LEDGER = __ENV.LEDGER_URL || 'http://ledger-api:8081';
const ONBOARDING = __ENV.ONBOARDING_TOKEN || 'dev-onboarding-token';
const PAYMENTS = __ENV.PAYMENTS_TOKEN || 'dev-payments-token';
const ACCOUNTS = parseInt(__ENV.ACCOUNTS || '50');
const RATE = parseInt(__ENV.RATE || '300');
const DURATION = __ENV.DURATION || '30s';

export const options = {
  scenarios: {
    credits: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: DURATION,
      preAllocatedVUs: 100, maxVUs: 400,
    },
  },
  thresholds: {
    'http_req_failed{scenario:credits}': ['rate<0.01'],
    'http_req_duration{scenario:credits}': ['p(99)<500'],
  },
  setupTimeout: '5m',
};

const json = { 'Content-Type': 'application/json' };

export function setup() {
  const run = Date.now();
  const accounts = [];
  for (let i = 0; i < ACCOUNTS; i++) {
    const reg = http.post(`${AUTH}/api/v1/auth/register`,
      JSON.stringify({ email: `load-${run}-${i}@example.com`, password: 'load-password-123' }), { headers: json });
    if (reg.status !== 201) throw new Error(`register ${i}: ${reg.status} ${reg.body}`);
    const open = http.post(`${LEDGER}/internal/v1/accounts`, JSON.stringify({ user_id: reg.json('id') }), {
      headers: Object.assign({ Authorization: `Bearer ${ONBOARDING}`, 'Idempotency-Key': `load-open-${run}-${i}` }, json),
    });
    if (open.status !== 201) throw new Error(`open ${i}: ${open.status} ${open.body}`);
    accounts.push(open.json('account.id'));
  }
  return { run, accounts };
}

export default function (data) {
  const acct = data.accounts[Math.floor(Math.random() * data.accounts.length)];
  const res = http.post(`${LEDGER}/internal/v1/accounts/${acct}/credits`, JSON.stringify({ amount: 1, currency: 'IDR' }), {
    headers: Object.assign({
      Authorization: `Bearer ${PAYMENTS}`,
      'Idempotency-Key': `load-${data.run}-${__VU}-${__ITER}`,
    }, json),
    tags: { name: 'credit' },
  });
  check(res, { 'credit settled (201)': (r) => r.status === 201 });
}
