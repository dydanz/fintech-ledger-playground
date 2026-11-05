#!/usr/bin/env bash
# End-to-end smoke test against a running stack: register -> login ->
# profile update -> logout -> token rejected.
set -euo pipefail

BASE="${BASE_URL:-http://localhost:8080}"
EMAIL="smoke+$(date +%s)@example.com"
PASS="smoke-password-123"

req() { curl -sS -o /tmp/smoke_body -w '%{http_code}' "$@"; }
expect() {
  local want=$1 got=$2 step=$3
  if [[ "$got" != "$want" ]]; then
    echo "FAIL $step: want $want got $got"; cat /tmp/smoke_body; echo; exit 1
  fi
  echo "ok   $step ($got)"
}

code=$(req -X POST "$BASE/api/v1/auth/register" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")
expect 201 "$code" register

code=$(req -X POST "$BASE/api/v1/auth/login" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASS\"}")
expect 200 "$code" login
TOKEN=$(sed -E 's/.*"access_token":"([^"]+)".*/\1/' /tmp/smoke_body)

code=$(req -X PUT "$BASE/api/v1/users/profile" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"full_name":"Smoke Test","bio":"hello"}')
expect 200 "$code" "update profile"

code=$(req -X POST "$BASE/api/v1/auth/logout" -H "Authorization: Bearer $TOKEN")
expect 204 "$code" logout

code=$(req -X PUT "$BASE/api/v1/users/profile" -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' -d '{"bio":"after logout"}')
expect 401 "$code" "token rejected after logout"

rm -f /tmp/smoke_body
echo "all smoke checks passed"
