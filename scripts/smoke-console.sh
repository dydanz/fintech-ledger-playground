#!/usr/bin/env bash
# End-to-end smoke test of the web console's API against a running stack
# (make up): register -> login -> open account -> top up -> balance ->
# refusals -> four top-ups freeze the account (the real fraud rule) ->
# a spend on a frozen account is refused. Everything goes through
# ledger-console, the way the browser does.
set -euo pipefail

BASE="${CONSOLE_URL:-http://localhost:8090}/console/api"
EMAIL="console-smoke+$(date +%s)@example.com"
PASS="console-smoke-123"
BODY=$(mktemp)
trap 'rm -f "$BODY"' EXIT

req() { curl -sS -o "$BODY" -w '%{http_code}' "$@"; }
field() { # $1 a dotted path into the last JSON body, e.g. account.ledgers.0.balance
  python3 -c 'import json, sys
d = json.load(open(sys.argv[1]))
for k in sys.argv[2].split("."):
    d = d[int(k)] if k.isdigit() else d[k]
print(d)' "$BODY" "$1"
}
expect() {
  local want=$1 got=$2 step=$3
  if [[ "$got" != "$want" ]]; then
    echo "FAIL $step: want $want got $got"; cat "$BODY"; echo; exit 1
  fi
  echo "ok   $step ($got)"
}
key() { python3 -c 'import uuid; print(uuid.uuid4())'; }
money() { # $1 credit|debit, $2 amount
  local body='{"amount":'"$2"'}'
  req -X POST "$BASE/me/$1" -H "Authorization: Bearer $TOKEN" -H "Idempotency-Key: $(key)" \
    -H 'Content-Type: application/json' -d "$body"
}

# Bodies go in variables: inside "$( )" bash would brace-expand {a,b}.
CREDS='{"email":"'"$EMAIL"'","password":"'"$PASS"'"}'
code=$(req -X POST "$BASE/auth/register" -H 'Content-Type: application/json' -d "$CREDS"); expect 201 "$code" register
code=$(req -X POST "$BASE/auth/login" -H 'Content-Type: application/json' -d "$CREDS"); expect 200 "$code" login
TOKEN=$(field access_token)

expect 404 "$(req "$BASE/me/account" -H "Authorization: Bearer $TOKEN")" "no account yet"
expect 201 "$(req -X POST "$BASE/me/open" -H "Authorization: Bearer $TOKEN")" "open account"
ACCOUNT=$(field account.id)
# The same key per user: ledger-api replays the first answer (same 201, same account).
expect 201 "$(req -X POST "$BASE/me/open" -H "Authorization: Bearer $TOKEN")" "open again (idempotent replay)"
[[ "$(field account.id)" == "$ACCOUNT" ]] || { echo "FAIL open again: a different account"; exit 1; }
echo "ok   open again returns the same account"

expect 201 "$(money credit 50000)" "top up 50,000"
expect 200 "$(req "$BASE/me/account" -H "Authorization: Bearer $TOKEN")" "read account"
bal=$(field account.ledgers.0.balance)
[[ "$bal" == 50000 ]] || { echo "FAIL balance: want 50000 got $bal"; exit 1; }
echo "ok   balance is 50,000"

expect 400 "$(money debit 1000001)" "spend above the demo cap refused"
expect 422 "$(money debit 60000)" "overdraw refused"
[[ "$(field error.code)" == insufficient_funds ]] && echo "ok   refusal is insufficient_funds"

# The fraud rule freezes after more than 3 top-ups in 5 minutes (the default
# FRAUD_TOPUP_MAX=3; make load-up raises it, so run this on make up).
for i in 2 3 4; do expect 201 "$(money credit 1000)" "top up $i"; done
status=
for _ in $(seq 1 20); do
  req "$BASE/me/account" -H "Authorization: Bearer $TOKEN" >/dev/null
  status=$(field account.status)
  [[ "$status" == FROZEN ]] && break
  sleep 0.5
done
[[ "$status" == FROZEN ]] || { echo "FAIL account not frozen within 10 s (status $status)"; exit 1; }
echo "ok   account frozen by the fraud engine after 4 top-ups"
expect 422 "$(money debit 100)" "spend on a frozen account refused"
[[ "$(field error.code)" == account_frozen ]] && echo "ok   refusal is account_frozen"

expect 200 "$(req "$BASE/me/transactions?limit=10" -H "Authorization: Bearer $TOKEN")" "history"
n=$(python3 -c 'import json,sys; print(len(json.load(open(sys.argv[1]))["transactions"]))' "$BODY")
echo "ok   history has $n transactions"
echo "all console smoke checks passed"
