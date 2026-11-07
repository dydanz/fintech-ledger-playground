#!/usr/bin/env bash
# End-to-end fraud check against a running stack (make up): for each of RUNS
# new accounts, 4 top-ups within the window must freeze the account (commit →
# freeze measured from the 4th 201 to the account reading FROZEN), and a
# debit must then return 422 account_frozen. Prints p50/max latency.
set -euo pipefail

AUTH="${AUTH_URL:-http://localhost:8080}"
LEDGER="${LEDGER_URL:-http://localhost:8081}"
ONBOARDING_TOKEN="${ONBOARDING_TOKEN:-dev-onboarding-token}"
PAYMENTS_TOKEN="${PAYMENTS_TOKEN:-dev-payments-token}"
RUNS="${RUNS:-10}"
RUN="$(date +%s)"
BODY="$(mktemp)"
trap 'rm -f "$BODY"' EXIT

req() { curl -sS -o "$BODY" -w '%{http_code}' "$@"; }
post() { req -X POST "$LEDGER$3" -H "Authorization: Bearer $1" -H "Idempotency-Key: $2" -H 'Content-Type: application/json' -d "$4"; }
now_ms() { python3 -c 'import time; print(int(time.time() * 1000))'; }

lat=()
for n in $(seq 1 "$RUNS"); do
  code=$(req -X POST "$AUTH/api/v1/auth/register" -H 'Content-Type: application/json' \
    -d "{\"email\":\"fraud-smoke+$RUN-$n@example.com\",\"password\":\"smoke-password-123\"}")
  [[ "$code" == 201 ]] || { echo "FAIL register: $code $(cat "$BODY")"; exit 1; }
  user=$(sed -E 's/.*"id":"([^"]+)".*/\1/' "$BODY")
  code=$(post "$ONBOARDING_TOKEN" "fraud-open-$RUN-$n" /internal/v1/accounts "{\"user_id\":\"$user\"}")
  [[ "$code" == 201 ]] || { echo "FAIL open: $code $(cat "$BODY")"; exit 1; }
  acct=$(sed -E 's/.*"account":\{"id":"([^"]+)".*/\1/' "$BODY")

  for i in 1 2 3 4; do
    code=$(post "$PAYMENTS_TOKEN" "fraud-topup-$RUN-$n-$i" "/internal/v1/accounts/$acct/credits" '{"amount":1000,"currency":"IDR"}')
    [[ "$code" == 201 ]] || { echo "FAIL top-up $i: $code $(cat "$BODY")"; exit 1; }
  done
  start=$(now_ms)
  status=""
  for _ in $(seq 1 100); do
    req "$LEDGER/internal/v1/accounts/$acct" -H "Authorization: Bearer $PAYMENTS_TOKEN" >/dev/null
    status=$(sed -E 's/.*"status":"([A-Z]+)".*/\1/' "$BODY")
    [[ "$status" == FROZEN ]] && break
    sleep 0.02
  done
  [[ "$status" == FROZEN ]] || { echo "FAIL account $n not frozen after 4 top-ups"; exit 1; }
  lat+=($(( $(now_ms) - start )))

  code=$(post "$PAYMENTS_TOKEN" "fraud-debit-$RUN-$n" "/internal/v1/accounts/$acct/debits" '{"amount":1,"currency":"IDR"}')
  [[ "$code" == 422 ]] && grep -q '"account_frozen"' "$BODY" || { echo "FAIL debit after freeze: $code $(cat "$BODY")"; exit 1; }
done

sorted=($(printf '%s\n' "${lat[@]}" | sort -n))
p50=${sorted[$(( ${#sorted[@]} / 2 ))]}
max=${sorted[$(( ${#sorted[@]} - 1 ))]}
echo "ok   $RUNS accounts frozen after 4 top-ups; debits then account_frozen"
echo "ok   4th top-up → FROZEN: p50 ${p50} ms, max ${max} ms (target p99 < 2000 ms)"
(( max < 2000 )) || { echo "FAIL max latency ${max} ms"; exit 1; }
echo "all fraud smoke checks passed"
