#!/usr/bin/env bash
# Ledger load test, spec/TRD §13.4 (`make load`; PROFILE=full or quick).
#
#   1. Inserts the load users into Postgres (ids md5("load-RUN-i"), see ledger-mixed.js).
#   2. Runs scripts/load/ledger-mixed.js in the compose network, while sampling the
#      outbox lag (age of the oldest unpublished row) every 0.5 s.
#   3. Reports, against the NFRs: outbox lag p50/p99/max (NFR-8), transaction
#      outcomes and FAILED processing_timeout (NFR-5), server-side write latency
#      from the access log (NFR-2), fraud decisions, and a full reconciler sweep
#      after restarting ledger-worker (it sweeps everything on gaining leadership).
#
# Start the stack with `make load-up` first: with the default fraud thresholds the
# engine freezes most accounts within minutes, and the run measures 422s instead.
# Results go to load-results/<RUN>/ (k6.txt, summary.json, lag.txt, report.txt).
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

PROFILE="${PROFILE:-full}"
RUN="${RUN:-$(date +%s)}"
if [[ "$PROFILE" == quick ]]; then ACCOUNTS="${ACCOUNTS:-1000}"; else ACCOUNTS="${ACCOUNTS:-10000}"; fi
HOT_ACCOUNTS="${HOT_ACCOUNTS:-10}"
# Always passed to k6: the compose k6 service defaults DURATION to 30s for make load-credits.
if [[ "$PROFILE" == quick ]]; then DURATION="${DURATION:-90s}"; else DURATION="${DURATION:-30m}"; fi
OUT="load-results/$RUN"
mkdir -p "$OUT"
C=(docker compose ${COMPOSE_PROJECT:+-p "$COMPOSE_PROJECT"})
sql() { "${C[@]}" exec -T postgres psql -U "${POSTGRES_USER:-auth}" -d "${POSTGRES_DB:-auth}" -X -q -At -c "$1"; }
LOKI="${LOKI_URL:-http://localhost:3100}"

topup_max=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$("${C[@]}" ps -q fraud-engine)" 2>/dev/null |
  sed -n 's/^FRAUD_TOPUP_MAX=//p')
if [[ -z "$topup_max" ]]; then
  echo "fraud-engine is not running (start the stack with make load-up)" >&2; exit 1
elif (( topup_max < 1000000 )); then
  echo "WARNING fraud-engine FRAUD_TOPUP_MAX=$topup_max: it will freeze load accounts; use make load-up" >&2
fi

echo "run $RUN, profile $PROFILE: seeding $((ACCOUNTS + HOT_ACCOUNTS)) users"
sql "INSERT INTO users (id, email, password_hash)
     SELECT md5('load-$RUN-' || i)::uuid, 'load-$RUN-' || i || '@load.test', '!no-login'
     FROM generate_series(0, $((ACCOUNTS + HOT_ACCOUNTS - 1))) AS i" || exit 1
t0=$(sql "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
start=$(date +%s)

# Outbox lag sampler, until k6 finishes.
: > "$OUT/lag.txt"
( while [[ ! -e "$OUT/.done" ]]; do
    sql "SELECT coalesce(round(extract(epoch FROM clock_timestamp() - min(created_at)) * 1000), 0)
         FROM ledger.outbox WHERE published_at IS NULL" >> "$OUT/lag.txt" 2>/dev/null
    sleep 0.5
  done ) &
sampler=$!

"${C[@]}" --profile load run --rm -v "$PWD/$OUT:/results" \
  ${LEDGER_URL:+-e LEDGER_URL="$LEDGER_URL"} -e RUN="$RUN" -e PROFILE="$PROFILE" -e ACCOUNTS="$ACCOUNTS" -e HOT_ACCOUNTS="$HOT_ACCOUNTS" \
  ${MIXED_RATE:+-e MIXED_RATE="$MIXED_RATE"} ${HOT_RATE:+-e HOT_RATE="$HOT_RATE"} ${SPIKE_RATE:+-e SPIKE_RATE="$SPIKE_RATE"} \
  -e DURATION="$DURATION" ${SPIKE_DURATION:+-e SPIKE_DURATION="$SPIKE_DURATION"} \
  k6 run --summary-export=/results/summary.json /scripts/ledger-mixed.js 2>&1 | tee "$OUT/k6.txt"
k6_exit=${PIPESTATUS[0]}
sleep 10 # let the relay drain, then stop sampling
touch "$OUT/.done"; wait "$sampler"; rm -f "$OUT/.done"
elapsed=$(( $(date +%s) - start ))

{
echo "== run $RUN, profile $PROFILE, $ACCOUNTS + $HOT_ACCOUNTS hot accounts, from $t0, ${elapsed}s, k6 exit $k6_exit"
echo
echo "== NFR-8 outbox lag, $(wc -l < "$OUT/lag.txt" | tr -d ' ') samples every 0.5 s (ms)"
sort -n "$OUT/lag.txt" | awk '{a[NR]=$1} END {if (NR) printf "p50 %d  p99 %d  max %d\n", a[int(NR*0.5)+1>NR?NR:int(NR*0.5)+1], a[int(NR*0.99)+1>NR?NR:int(NR*0.99)+1], a[NR]}'
echo
echo "== Transactions created during the run (type, status, failure_reason, count)"
sql "SELECT type, status, coalesce(failure_reason, '-'), count(*) FROM ledger.transactions
     WHERE created_at >= '$t0' GROUP BY 1, 2, 3 ORDER BY 1, 2, 3" | tr '|' ' '
echo
echo "== NFR-5 server side: FAILED processing_timeout, and still PENDING/PROCESSING after the sweeper"
sleep 60 # the sweeper completes anything left PENDING (§7.5)
sql "SELECT 'processing_timeout ' || count(*) FILTER (WHERE status = 'FAILED' AND failure_reason = 'processing_timeout')
       || '  still_pending ' || count(*) FILTER (WHERE status IN ('PENDING', 'PROCESSING'))
     FROM ledger.transactions WHERE created_at >= '$t0'"
echo
echo "== NFR-2 server side: ledger-api write latency from the access log (ms, all phases)"
range=$(( elapsed + 60 ))s
for q in 0.5 0.99; do
  v=$(curl -fsS -G "$LOKI/loki/api/v1/query" --max-time 120 \
    --data-urlencode "query=quantile_over_time($q, {service=\"ledger-api\"} | json message=\"message\", method=\"method\", route=\"route\", latency_ms=\"latency_ms\" | message=\"http_request\" | method=\"POST\" | route=~\"/internal/v1/(accounts/:account_id/(credits|debits)|transactions/:transaction_id/void)\" | unwrap latency_ms [$range]) by (service)" 2>/dev/null |
    python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(round(float(r[0]["value"][1]),1) if r else "n/a")' 2>/dev/null)
  echo "p$(python3 -c "print(int($q*100))") ${v:-n/a (Loki query failed)}"
done
echo
echo "== Fraud decisions during the run"
"${C[@]}" logs --since "$t0" fraud-engine 2>/dev/null | grep -c '"fraud.decision"' | sed 's/^/fraud.decision events: /'
echo
echo "== Reconciler during the run (incremental): outcome counts"
"${C[@]}" logs --since "$t0" ledger-worker 2>/dev/null | grep '"ledger.reconcile"' |
  sed -E 's/.*"outcome":"([a-z]+)".*/\1/' | sort | uniq -c
} 2>&1 | tee "$OUT/report.txt"

# A full sweep after the run: ledger-worker sweeps everything when it gains leadership.
{
echo
echo "== Reconciler full sweep after the run"
restarted=$(date -u +%Y-%m-%dT%H:%M:%SZ)
"${C[@]}" restart ledger-worker >/dev/null 2>&1
line=
for _ in $(seq 1 180); do
  line=$("${C[@]}" logs --since "$restarted" ledger-worker 2>/dev/null | grep '"ledger.reconcile"' | grep '"mode":"full"' |
    grep -E '"outcome":"(success|failure)"' | grep -v '"check"' | tail -1)
  [[ -n "$line" ]] && break
  sleep 5
done
if [[ -z "$line" ]]; then
  echo "no full sweep summary within 15 minutes"
else
  echo "${line#*| }" | python3 -c '
import json,sys
r=json.loads(sys.stdin.read())
print(" ".join(f"{k}={r.get(k)}" for k in ("level","mode","outcome","reason","transactions","ledgers","accounts","mismatches","duration_ms") if k in r))'
fi
} 2>&1 | tee -a "$OUT/report.txt"
echo "results in $OUT/"
exit "$k6_exit"
