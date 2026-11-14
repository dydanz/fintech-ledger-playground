#!/usr/bin/env bash
# End-to-end smoke test of the web console's API against a running stack
# (make up): register -> login -> open account -> top up -> balance ->
# refusals -> four top-ups freeze the account (the real fraud rule) ->
# a spend on a frozen account is refused -> the ops screens (D§9): refused
# without the ops role; with it, the top-up is on the Monitor, labelled
# normal, and the freezing top-up's timeline shows the fraud decision.
# Everything goes through ledger-console, the way the browser does; the
# role is granted with auth-api's grant-role, as make grant-role does.
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
TOPUP=$(field transaction.id)
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
FREEZER=$(field transaction.id) # the 4th: over the limit
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

# The ops screens need the ops role.
expect 403 "$(req "$BASE/ops/monitor" -H "Authorization: Bearer $TOKEN")" "Monitor without the ops role refused"
docker compose run --rm --no-deps api grant-role --email "$EMAIL" --role ops --by smoke-console >/dev/null 2>&1 \
  || { echo "FAIL grant-role (run from the repo, with the stack's COMPOSE_PROJECT_NAME)"; exit 1; }
echo "ok   ops role granted"
expect 200 "$(req "$BASE/ops/monitor" -H "Authorization: Bearer $TOKEN")" "Monitor with the ops role"
label=$(python3 -c 'import json, sys
feed = json.load(open(sys.argv[1]))["feed"]
print(next((f["label"] for f in feed if f["transaction"]["id"] == sys.argv[2]), "missing"))' "$BODY" "$TOPUP")
[[ "$label" == normal ]] || { echo "FAIL the top-up on the Monitor feed: label $label, want normal"; exit 1; }
echo "ok   the top-up is on the Monitor, labelled normal"

# The fraud decision reaches Loki through Alloy, usually within seconds. Right
# after make up recreates fraud-engine, Alloy finds the new container on its
# next Docker discovery (every minute) and ships its lines then: allow 90 s.
step=
for _ in $(seq 1 90); do
  req "$BASE/ops/transactions/$FREEZER" -H "Authorization: Bearer $TOKEN" >/dev/null
  step=$(python3 -c 'import json, sys
for s in json.load(open(sys.argv[1]))["timeline"]:
    if s["title"].startswith("Fraud engine"):
        print(s["actor"] + ": " + s["detail"])' "$BODY")
  [[ -n "$step" ]] && break
  sleep 1
done
[[ -n "$step" ]] || { echo "FAIL no fraud step on the freezing top-up's timeline within 90 s"; cat "$BODY"; echo; exit 1; }
echo "ok   the freezing top-up's timeline shows the fraud decision ($step)"
echo "all console smoke checks passed"
