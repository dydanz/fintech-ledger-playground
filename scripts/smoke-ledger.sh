#!/usr/bin/env bash
# End-to-end smoke test of ledger-api against a running stack (make up):
# register a user on auth-api -> open account -> credit -> debit ->
# insufficient funds -> idempotent replay -> key reuse -> auth rejections ->
# the user's own balance and history, read with an auth-api login token.
set -euo pipefail

AUTH="${AUTH_URL:-http://localhost:8080}"
LEDGER="${LEDGER_URL:-http://localhost:8081}"
ONBOARDING_TOKEN="${ONBOARDING_TOKEN:-dev-onboarding-token}"
PAYMENTS_TOKEN="${PAYMENTS_TOKEN:-dev-payments-token}"
RUN="$(date +%s)"
EMAIL="ledger-smoke+$RUN@example.com"
BODY="$(mktemp)"
HEADERS="$(mktemp)"
trap 'rm -f "$BODY" "$HEADERS"' EXIT

req() { curl -sS -o "$BODY" -D "$HEADERS" -w '%{http_code}' "$@"; }
expect() {
  local want=$1 got=$2 step=$3
  if [[ "$got" != "$want" ]]; then
    echo "FAIL $step: want $want got $got"; cat "$BODY"; echo; exit 1
  fi
  echo "ok   $step ($got)"
}
json() { sed -E "s/.*\"$1\":(\"([^\"]*)\"|([0-9-]+)).*/\2\3/" "$BODY"; }
post() { # token key path body
  req -X POST "$LEDGER$3" -H "Authorization: Bearer $1" -H "Idempotency-Key: $2" \
    -H 'Content-Type: application/json' -d "$4"
}

code=$(req -X POST "$AUTH/api/v1/auth/register" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"smoke-password-123\"}")
expect 201 "$code" "register user on auth-api"
USER_ID=$(json id)

code=$(post "$ONBOARDING_TOKEN" "open-$RUN" /internal/v1/accounts "{\"user_id\":\"$USER_ID\"}")
expect 201 "$code" "open account"
ACCOUNT_ID=$(sed -E 's/.*"account":\{"id":"([^"]+)".*/\1/' "$BODY")

code=$(post "$PAYMENTS_TOKEN" "credit-$RUN" "/internal/v1/accounts/$ACCOUNT_ID/credits" '{"amount":1000,"currency":"IDR"}')
expect 201 "$code" "credit 1000"
CREDIT_BODY="$(cat "$BODY")"
CREDIT_ID=$(json id)

code=$(post "$PAYMENTS_TOKEN" "debit-$RUN" "/internal/v1/accounts/$ACCOUNT_ID/debits" '{"amount":300,"currency":"IDR"}')
expect 201 "$code" "debit 300"
[[ "$(json balance_after)" == "700" ]] || { echo "FAIL balance_after: $(cat "$BODY")"; exit 1; }
echo "ok   balance_after = 700"

code=$(post "$PAYMENTS_TOKEN" "nsf-$RUN" "/internal/v1/accounts/$ACCOUNT_ID/debits" '{"amount":5000,"currency":"IDR"}')
expect 422 "$code" "debit beyond balance"
[[ "$(json code)" == "insufficient_funds" ]] || { echo "FAIL error code: $(cat "$BODY")"; exit 1; }

code=$(post "$PAYMENTS_TOKEN" "credit-$RUN" "/internal/v1/accounts/$ACCOUNT_ID/credits" '{"amount":1000,"currency":"IDR"}')
expect 201 "$code" "replay credit with the same key"
grep -qi '^Idempotent-Replayed: true' "$HEADERS" || { echo "FAIL missing Idempotent-Replayed header"; exit 1; }
[[ "$(cat "$BODY")" == "$CREDIT_BODY" ]] || { echo "FAIL replay body differs"; exit 1; }
echo "ok   replay is byte-identical"

code=$(post "$PAYMENTS_TOKEN" "credit-$RUN" "/internal/v1/accounts/$ACCOUNT_ID/credits" '{"amount":999,"currency":"IDR"}')
expect 422 "$code" "same key, different request"

code=$(req -X POST "$LEDGER/internal/v1/accounts/$ACCOUNT_ID/credits" -H "Authorization: Bearer $PAYMENTS_TOKEN" \
  -H 'Content-Type: application/json' -d '{"amount":1,"currency":"IDR"}')
expect 400 "$code" "missing Idempotency-Key"

code=$(post "not-a-real-token" "x-$RUN" "/internal/v1/accounts/$ACCOUNT_ID/credits" '{"amount":1,"currency":"IDR"}')
expect 401 "$code" "unknown service token"

code=$(post "$PAYMENTS_TOKEN" "open2-$RUN" /internal/v1/accounts "{\"user_id\":\"$USER_ID\"}")
expect 403 "$code" "payments may not open accounts"

code=$(req "$LEDGER/internal/v1/accounts/$ACCOUNT_ID" -H "Authorization: Bearer $PAYMENTS_TOKEN")
expect 200 "$code" "read account"
[[ "$(json balance)" == "700" ]] || { echo "FAIL account balance: $(cat "$BODY")"; exit 1; }
echo "ok   account balance = 700"

code=$(req -X POST "$AUTH/api/v1/auth/login" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"smoke-password-123\"}")
expect 200 "$code" "log in on auth-api"
USER_TOKEN=$(json access_token)

code=$(req "$LEDGER/api/v1/ledger/account" -H "Authorization: Bearer $USER_TOKEN")
expect 200 "$code" "user reads own account"
[[ "$(json balance)" == "700" ]] || { echo "FAIL user balance: $(cat "$BODY")"; exit 1; }
echo "ok   user balance = 700"

code=$(req "$LEDGER/api/v1/ledger/transactions?limit=2" -H "Authorization: Bearer $USER_TOKEN")
expect 200 "$code" "user lists transactions (page 1: both debits)"
CURSOR=$(json next_cursor)
code=$(req "$LEDGER/api/v1/ledger/transactions?limit=2&cursor=$CURSOR" -H "Authorization: Bearer $USER_TOKEN")
expect 200 "$code" "user lists transactions (page 2)"
[[ "$(json id)" == "$CREDIT_ID" ]] || { echo "FAIL page 2 should hold the credit: $(cat "$BODY")"; exit 1; }
echo "ok   page 2 holds the credit"

code=$(req "$LEDGER/api/v1/ledger/transactions/$CREDIT_ID" -H "Authorization: Bearer $USER_TOKEN")
expect 200 "$code" "user reads own transaction"

code=$(req "$LEDGER/api/v1/ledger/account" -H "Authorization: Bearer $PAYMENTS_TOKEN")
expect 401 "$code" "service token on a user route"

echo "all ledger smoke checks passed"
