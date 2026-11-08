#!/usr/bin/env bash
# Reloads Grafana's provisioned alert rules. Grafana reads
# deploy/grafana/provisioning/alerting/ only at startup, and `docker compose up`
# doesn't restart it when only those files change, so a new or edited rule
# would otherwise stay unloaded. Dashboards don't need this: Grafana rescans
# their files on its own. `make up` and scripts/check-alerts.sh run it.
#
# Never fails the caller: if Grafana isn't reachable it warns and exits 0.
set -uo pipefail

GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
# Same credentials as compose: the environment, else .env, else the defaults.
env_file="$(dirname "$0")/../.env"
from_env_file() { [[ -f "$env_file" ]] && sed -n "s/^$1=//p" "$env_file" | tail -1; }
user="${GRAFANA_ADMIN_USER:-$(from_env_file GRAFANA_ADMIN_USER)}"
pass="${GRAFANA_ADMIN_PASSWORD:-$(from_env_file GRAFANA_ADMIN_PASSWORD)}"
AUTH="${user:-admin}:${pass:-admin}"

up=
for _ in $(seq 1 ${GRAFANA_WAIT_SECONDS:-60}); do
  curl -fsS "$GRAFANA/api/health" >/dev/null 2>&1 && { up=1; break; }
  sleep 1
done
if [[ -z "$up" ]]; then
  echo "grafana: WARNING not reachable at $GRAFANA; alert rules not reloaded (run ./scripts/grafana-reload.sh once it is up)" >&2
  exit 0
fi
# /api/health is OK before the alerting engine is ready, when a reload
# answers 500: retry for up to 30 s.
ok=
for _ in $(seq 1 30); do
  out=$(curl -fsS -u "$AUTH" -X POST "$GRAFANA/api/admin/provisioning/alerting/reload" 2>&1) && { ok=1; break; }
  sleep 1
done
if [[ -n "$ok" ]]; then
  # Grafana can drop the first request right after a reload; retry quietly.
  n="?"
  for _ in $(seq 1 15); do
    n=$(curl -fsS -u "$AUTH" "$GRAFANA/api/v1/provisioning/alert-rules" 2>/dev/null |
      python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' 2>/dev/null) && break
    n="?"; sleep 1
  done
  echo "grafana: alert rules reloaded ($n provisioned)"
else
  echo "grafana: WARNING could not reload alert rules at $GRAFANA: ${out##*: }" >&2
fi
exit 0
