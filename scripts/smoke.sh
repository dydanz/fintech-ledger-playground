#!/usr/bin/env bash
# End-to-end smoke test against a running stack: register -> login ->
# profile update -> logout -> token rejected -> password change ends every
# session -> old password refused, new one accepted.
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

login() { # $1 password; sets TOKEN
  code=$(req -X POST "$BASE/api/v1/auth/login" -H 'Content-Type: application/json' \
    -d "{\"email\":\"$EMAIL\",\"password\":\"$1\"}")
  TOKEN=$(sed -E 's/.*"access_token":"([^"]+)".*/\1/' /tmp/smoke_body)
}
NEWPASS="smoke-password-456"

login "$PASS"; expect 200 "$code" "login again"
FIRST=$TOKEN
login "$PASS"; expect 200 "$code" "second session"

code=$(req -X PUT "$BASE/api/v1/users/password" -H "Authorization: Bearer $FIRST" \
  -H 'Content-Type: application/json' -d "{\"current_password\":\"wrong-password\",\"new_password\":\"$NEWPASS\"}")
expect 403 "$code" "password change with a wrong current password"

code=$(req -X PUT "$BASE/api/v1/users/password" -H "Authorization: Bearer $FIRST" \
  -H 'Content-Type: application/json' -d "{\"current_password\":\"$PASS\",\"new_password\":\"$NEWPASS\"}")
expect 204 "$code" "password change"

code=$(req "$BASE/api/v1/users/profile" -H "Authorization: Bearer $FIRST")
expect 401 "$code" "requesting session ended"
code=$(req "$BASE/api/v1/users/profile" -H "Authorization: Bearer $TOKEN")
expect 401 "$code" "other session ended"

login "$PASS"; expect 401 "$code" "old password refused"
login "$NEWPASS"; expect 200 "$code" "new password accepted"

rm -f /tmp/smoke_body
echo "all smoke checks passed"
