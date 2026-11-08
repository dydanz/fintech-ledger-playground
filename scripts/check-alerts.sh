#!/usr/bin/env bash
# Checks the ledger observability wiring against a running stack (make up):
#   1. Grafana loaded the provisioned ledger alert rules;
#   2. every query in the ledger dashboard parses and runs on Loki;
#   3. every alert is quiet on a healthy stack, then goes Pending/Firing on
#      synthetic log lines pushed to Loki.
# Step 3 leaves those alerts firing for up to 10 minutes, so prefer a
# throwaway stack: docker compose -p alertcheck up -d --wait.
set -euo pipefail

GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
LOKI="${LOKI_URL:-http://localhost:3100}"
AUTH="${GRAFANA_ADMIN_USER:-admin}:${GRAFANA_ADMIN_PASSWORD:-admin}"
DASHBOARD="$(dirname "$0")/../deploy/grafana/dashboards/ledger.json"
RULES=(ledger-success-ratio ledger-outbox-lag ledger-stuck-pending ledger-fraud-freeze-failing ledger-reconcile-failure ledger-partitions-running-out ledger-archive-failing)

fail() { echo "FAIL $*"; exit 1; }

rule_states() {
  curl -fsS -u "$AUTH" "$GRAFANA/api/prometheus/grafana/api/v1/rules" | python3 -c '
import sys, json
want = set(sys.argv[1:])
out = {}
for g in json.load(sys.stdin)["data"]["groups"]:
    for r in g["rules"]:
        uid = r.get("uid") or ""
        if uid in want:
            out[uid] = r.get("state", "?")
missing = "missing"
print(" ".join(f"{k}={out.get(k, missing)}" for k in sorted(want)))
' "${RULES[@]}"
}

# wait_states REGEX SECONDS: until no rule's state matches REGEX.
wait_states() {
  local bad=$1 deadline=$(( $(date +%s) + $2 ))
  while :; do
    states=$(rule_states)
    grep -qE "=($bad)( |$)" <<<"$states" || return 0
    (( $(date +%s) < deadline )) || return 1
    sleep 5
  done
}

# 1. Provisioned rules.
healthy=
for i in $(seq 1 30); do curl -fsS -u "$AUTH" "$GRAFANA/api/health" >/dev/null 2>&1 && { healthy=1; break; }; sleep 1; done
[[ -n "$healthy" ]] || fail "Grafana is not reachable at $GRAFANA (is the stack up?)"
ready=
for i in $(seq 1 60); do curl -fsS "$LOKI/ready" >/dev/null 2>&1 && { ready=1; break; }; sleep 1; done # Loki waits ~15s after start
[[ -n "$ready" ]] || fail "Loki is not ready at $LOKI"
rules_json=$(curl -fsS -u "$AUTH" "$GRAFANA/api/v1/provisioning/alert-rules") || fail "cannot list alert rules"
uids=$(python3 -c 'import sys,json; print(" ".join(sorted(r["uid"] for r in json.load(sys.stdin))))' <<<"$rules_json")
for uid in "${RULES[@]}"; do
  [[ " $uids " == *" $uid "* ]] || fail "rule $uid not provisioned (have: $uids)"
done
echo "ok   ${#RULES[@]} alert rules provisioned"

# 2. Dashboard queries run on Loki.
python3 - "$DASHBOARD" "$LOKI" <<'PY'
import json, sys, time, urllib.parse, urllib.request
dash, loki = sys.argv[1], sys.argv[2]
n = 0
for p in json.load(open(dash))["panels"]:
    for t in p.get("targets", []):
        q = urllib.parse.urlencode({"query": t["expr"], "start": int((time.time() - 3600) * 1e9),
                                    "end": int(time.time() * 1e9), "step": "60"})
        try:
            body = json.load(urllib.request.urlopen(f"{loki}/loki/api/v1/query_range?{q}", timeout=30))
        except urllib.error.HTTPError as e:
            sys.exit(f"FAIL panel {p['id']} ({p['title']}): {e.read().decode()[:300]}")
        if body.get("status") != "success":
            sys.exit(f"FAIL panel {p['id']} ({p['title']}): {body}")
        n += 1
print(f"ok   {n} dashboard queries run on Loki")
PY

# Baseline: on a healthy stack every rule settles to inactive (the outbox.lag
# heartbeat needs the relay up ~30 s). A rule that fires regardless would fail here.
wait_states "pending|firing|missing|nodata|error" 120 || fail "alerts not quiet on a healthy stack: $states (use a fresh stack)"
echo "ok   baseline: $states"

# 3. Synthetic lines that must trip each rule.
python3 - "$LOKI" <<'PY'
import json, sys, time, urllib.request
loki = sys.argv[1]
def line(**f):
    return json.dumps(dict({"timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "synthetic": True}, **f))
now = time.time_ns()
api, worker = [], []
for i in range(9):  # 1 of 10 valid writes is a 5xx: ratio 0.9 < 0.9999
    api.append(line(level="info", service="ledger-api", message="http_request", method="POST",
                    route="/internal/v1/accounts/:account_id/credits", status=201, latency_ms=3))
api.append(line(level="error", service="ledger-api", message="http_request", method="POST",
                route="/internal/v1/accounts/:account_id/credits", status=503, latency_ms=3))
worker.append(line(level="warn", service="ledger-worker", message="outbox lagging", event="outbox.lag",
                   outcome="success", pending=42, lag_ms=9000))
worker.append(line(level="warn", service="ledger-worker", message="stuck transaction recovered", event="ledger.recover",
                   outcome="success", reason="processing_timeout", status="FAILED"))
worker.append(line(level="error", service="ledger-worker", message="reconcile mismatch", event="ledger.reconcile",
                   outcome="failure", reason="balance_mismatch", mode="incremental", check=1, field="balance",
                   expected=100, actual=107, ledger_id="00000000-0000-0000-0000-000000000000"))
worker.append(line(level="error", service="ledger-worker", message="ledger partitions running out", event="ledger.partition",
                   outcome="failure", reason="insufficient_future_partitions", table="journal_entries", future_months=1))
worker.append(line(level="error", service="ledger-worker", message="archive step failed; partition kept", event="ledger.archive",
                   outcome="failure", reason="verify_failed", step="verify", table="journal_entries", month="2019-03"))
fraud = [line(level="error", service="fraud-engine", message="fraud freeze still failing, retrying", event="fraud.decision",
              outcome="failure", reason="freeze_call_failed", rule="topup_velocity")]
streams = []
for svc, lines in (("ledger-api", api), ("ledger-worker", worker), ("fraud-engine", fraud)):
    for i, l in enumerate(lines):
        streams.append({"stream": {"service": svc, "level": json.loads(l)["level"], "synthetic": "true"},
                        "values": [[str(now + i), l]]})
req = urllib.request.Request(f"{loki}/loki/api/v1/push", json.dumps({"streams": streams}).encode(),
                             {"Content-Type": "application/json"})
urllib.request.urlopen(req, timeout=10)
print(f"ok   pushed {len(api) + len(worker) + len(fraud)} synthetic lines")
PY

wait_states "inactive|normal|missing|nodata" 150 || fail "alerts did not trip within 150s: $states"
echo "ok   alerts tripped: $states"
echo "all alert checks passed"
