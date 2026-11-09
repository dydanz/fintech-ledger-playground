#!/usr/bin/env bash
# Ledger chaos runbook, spec/TRD §13.5 (`make chaos FAULT=...`). One fault per run:
#
#   redis     stop Redis for HOLD (60 s): writes continue on Postgres idempotency
#   kafka     stop Kafka for HOLD (300 s): writes continue, the outbox drains after
#   postgres  SIGKILL Postgres, restart after HOLD (30 s): 503 ledger_unavailable,
#             no partial state, the sweeper completes anything PENDING
#   worker    two ledger-worker replicas; SIGKILL the outbox_relay leader while it
#             publishes: the other takes over within 10 s
#
# While scripts/load/ledger-chaos.js writes at RATE/s (retrying with the same
# Idempotency-Key, as a client would), it samples the outbox and PENDING
# transactions every second and then checks (scripts/chaos/check.py) that the
# database, the balances and the Kafka topic match what the client was told, and
# that a full reconciler sweep is clean.
#
# Run it against a throwaway stack: it stops and kills containers.
#   COMPOSE_PROJECT_NAME=chaos make load-up
#   COMPOSE_PROJECT_NAME=chaos make chaos FAULT=redis
# Results go to chaos-results/<RUN>-<FAULT>/ (report.txt, k6.txt, k6-log.json, samples.txt).
set -uo pipefail
cd "$(dirname "$0")/../.." || exit 1

FAULT="${FAULT:-}"
case "$FAULT" in
  redis) HOLD="${HOLD:-60}" ;;
  kafka) HOLD="${HOLD:-300}" ;;
  postgres) HOLD="${HOLD:-30}" ;;
  worker) HOLD=0 ;;
  *) echo "FAULT must be redis, kafka, postgres or worker" >&2; exit 2 ;;
esac
RUN="${RUN:-$(date +%s)}"
RATE="${RATE:-50}"
ACCOUNTS="${ACCOUNTS:-100}"
WARMUP="${WARMUP:-30}"
TAIL="${TAIL:-90}" # writes keep going this long after recovery
DURATION="$(( WARMUP + HOLD + TAIL ))s"
OUT="chaos-results/$RUN-$FAULT"
mkdir -p "$OUT"
C=(docker compose ${COMPOSE_PROJECT:+-p "$COMPOSE_PROJECT"})
sql() { "${C[@]}" exec -T postgres psql -U "${POSTGRES_USER:-auth}" -d "${POSTGRES_DB:-auth}" -X -q -At -c "$1"; }
GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
GAUTH="${GRAFANA_ADMIN_USER:-admin}:${GRAFANA_ADMIN_PASSWORD:-admin}"
TOPIC="${KAFKA_TOPIC_EVENTS:-ledger.events.v1}"
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
mark() { echo "$(now) $*" | tee -a "$OUT/timeline.txt"; }
# Waits up to $1 s for the command in $2 to succeed; prints the seconds taken.
wait_for() {
  local t0=$SECONDS
  while (( SECONDS - t0 < $1 )); do eval "$2" >/dev/null 2>&1 && { echo $(( SECONDS - t0 )); return 0; }; sleep 1; done
  echo "timeout"; return 1
}
drained() { [[ "$(sql "SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL")" == 0 ]]; }
settled() { [[ "$(sql "SELECT count(*) FROM ledger.transactions WHERE status IN ('PENDING', 'PROCESSING')")" == 0 ]]; }
alert_state() { # the Grafana state of one alert rule, e.g. Normal, Pending, Alerting
  curl -fsS -u "$GAUTH" "$GRAFANA/api/prometheus/grafana/api/v1/rules" 2>/dev/null | python3 -c '
import json,sys
for g in json.load(sys.stdin)["data"]["groups"]:
    for r in g["rules"]:
        if r["name"] == sys.argv[1]:
            print(r["state"], *sorted({a["state"] for a in r.get("alerts", [])}))' "$1"
}
worker_ids() { "${C[@]}" ps -q ledger-worker; }

: > "$OUT/timeline.txt"; rm -f "$OUT/.failed"
"${C[@]}" ps -q ledger-api | grep -q . || { echo "ledger-api is not running (start the stack with make load-up)" >&2; exit 1; }
topup_max=$(docker inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$("${C[@]}" ps -q fraud-engine)" 2>/dev/null | sed -n 's/^FRAUD_TOPUP_MAX=//p')
(( ${topup_max:-0} >= 1000000 )) || echo "WARNING fraud-engine FRAUD_TOPUP_MAX=${topup_max:-?}: it will freeze chaos accounts; use make load-up" >&2

if [[ "$FAULT" == worker ]]; then
  mark "scaling ledger-worker to 2 replicas"
  "${C[@]}" up -d --no-build --no-recreate --scale ledger-worker=2 ledger-worker >/dev/null 2>&1 || exit 1
  sleep 10
fi

sql "INSERT INTO users (id, email, password_hash)
     SELECT md5('chaos-$RUN-' || i)::uuid, 'chaos-$RUN-' || i || '@chaos.test', '!no-login'
     FROM generate_series(0, $((ACCOUNTS - 1))) AS i" >/dev/null || exit 1
t0=$(now)
mark "run $RUN fault $FAULT: $RATE writes/s across $ACCOUNTS accounts for $DURATION (warmup ${WARMUP}s, hold ${HOLD}s, tail ${TAIL}s)"

# Every second: outbox rows not yet published, age of the oldest (ms), PENDING
# or PROCESSING transactions. "down" while Postgres can't be reached.
echo "time pending lag_ms in_flight" > "$OUT/samples.txt"
( while [[ ! -e "$OUT/.done" ]]; do
    s=$(sql "SELECT (SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL) || ' ' ||
                    coalesce((SELECT round(extract(epoch FROM clock_timestamp() - min(created_at)) * 1000)
                              FROM ledger.outbox WHERE published_at IS NULL), 0) || ' ' ||
                    (SELECT count(*) FROM ledger.transactions WHERE status IN ('PENDING', 'PROCESSING'))" 2>/dev/null)
    echo "$(date -u +%H:%M:%S) ${s:-down}" >> "$OUT/samples.txt"
    sleep 1
  done ) &
sampler=$!

"${C[@]}" --profile load run --rm -T -v "$PWD/$OUT:/results" \
  ${LEDGER_URL:+-e LEDGER_URL="$LEDGER_URL"} -e RUN="$RUN" -e RATE="$RATE" -e ACCOUNTS="$ACCOUNTS" -e DURATION="$DURATION" \
  ${RETRY_FOR:+-e RETRY_FOR="$RETRY_FOR"} \
  k6 run --log-format json --log-output file=/results/k6-log.json --summary-export=/results/summary.json \
  /scripts/ledger-chaos.js > "$OUT/k6.txt" 2>&1 &
k6=$!

# The fault starts WARMUP seconds after the first write (setup opens and funds the accounts first).
wait_for 300 "grep -q 'chaos-$RUN-[0-9]' '$OUT/k6-log.json'" >/dev/null || { mark "k6 did not start writing"; kill "$k6"; exit 1; }
mark "writes started"
sleep "$WARMUP"

case "$FAULT" in
redis)
  mark "FAULT stop redis"; "${C[@]}" stop redis >/dev/null 2>&1
  sleep "$HOLD"
  mark "RECOVER start redis"; "${C[@]}" start redis >/dev/null 2>&1
  mark "redis healthy after $(wait_for 60 "${C[*]} exec -T redis redis-cli ping | grep -q PONG")s"
  ;;
kafka)
  mark "FAULT stop kafka"; "${C[@]}" stop kafka >/dev/null 2>&1
  for (( s = 30; s <= HOLD; s += 30 )); do
    sleep 30
    mark "kafka down ${s}s: outbox pending $(sql "SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL"), alert ledger-outbox-lag: $(alert_state 'Ledger outbox lag above 5 s')"
  done
  mark "RECOVER start kafka"; "${C[@]}" start kafka >/dev/null 2>&1
  mark "outbox drained $(wait_for 600 drained)s after kafka start"
  ;;
postgres)
  mark "FAULT kill -9 postgres"; "${C[@]}" kill -s SIGKILL postgres >/dev/null 2>&1
  sleep "$HOLD"
  mark "RECOVER start postgres"; "${C[@]}" start postgres >/dev/null 2>&1
  mark "postgres accepting queries after $(wait_for 120 "sql 'SELECT 1'")s"
  mark "nothing PENDING after $(wait_for 120 settled)s"
  ;;
worker)
  # The replica that holds outbox_relay is the leader to kill (it usually leads every job).
  leader=
  for id in $(worker_ids); do
    last=$(docker logs "$id" 2>&1 | grep '"app.leader"' | grep '"job":"outbox_relay"' | tail -1)
    [[ "$last" == *'"state":"acquired"'* ]] && leader=$id
  done
  [[ -n "$leader" ]] || { mark "no outbox_relay leader found"; kill "$k6"; exit 1; }
  follower=$(worker_ids | grep -v "^$leader")
  jobs=$(docker logs "$leader" 2>&1 | grep '"state":"acquired"' | sed -E 's/.*"job":"([a-z_]+)".*/\1/' | sort -u | tr '\n' ' ')
  mark "leader ${leader:0:12} holds: $jobs; outbox pending $(sql "SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL")"
  killed_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
  mark "FAULT docker kill -s KILL ${leader:0:12}"; docker kill -s KILL "$leader" >/dev/null
  sleep 15
  # Takeover per job: the follower's "acquired" time minus the kill time.
  docker logs "$follower" 2>&1 | grep '"state":"acquired"' | python3 -c '
import json,sys,datetime
killed=int(sys.argv[1])/1000
for l in sys.stdin:
    r=json.loads(l[l.index("{"):])
    ts=r["timestamp"].rstrip("Z")  # RFC 3339, UTC, up to nanoseconds
    t=datetime.datetime.strptime(ts[:19],"%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()+float("0"+ts[19:26])
    if t >= killed - 1:
        job=r.get("job")
        print(f"takeover job={job} after {t-killed:.1f}s")' "$killed_ms" | tee -a "$OUT/timeline.txt"
  mark "killed replica: $(docker inspect -f '{{.State.Status}} restarts={{.RestartCount}}' "$leader")"
  ;;
esac

wait "$k6"; k6_exit=$?
mark "k6 finished (exit $k6_exit)"
mark "outbox drained $(wait_for 300 drained)s after k6; nothing PENDING after $(wait_for 120 settled)s"
touch "$OUT/.done"; wait "$sampler"; rm -f "$OUT/.done"

# The data check.
sql "SELECT t.id || '|' || t.account_id || '|' || t.status || '|' || coalesce(t.failure_reason, '')
     FROM ledger.transactions t JOIN ledger.accounts a ON a.id = t.account_id JOIN users u ON u.id = a.user_id
     WHERE u.email LIKE 'chaos-$RUN-%'" > "$OUT/tx.txt"
sql "SELECT a.id || '|' || l.balance FROM ledger.ledgers l JOIN ledger.accounts a ON a.id = l.account_id
     JOIN users u ON u.id = a.user_id WHERE u.email LIKE 'chaos-$RUN-%' AND l.currency = 'IDR'" > "$OUT/balances.txt"
sql "SELECT event_id FROM ledger.outbox WHERE created_at >= '$t0'" > "$OUT/outbox.txt"
"${C[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "$TOPIC" \
  --from-beginning --timeout-ms 20000 2>/dev/null | python3 -c '
import json,sys
for l in sys.stdin:
    try: print(json.loads(l)["event_id"])
    except ValueError: pass' > "$OUT/kafka.txt"

{
echo "== run $RUN, fault $FAULT, from $t0, k6 exit $k6_exit"
echo
echo "== timeline"
cat "$OUT/timeline.txt"
echo
echo "== client vs database vs Kafka"
python3 scripts/chaos/check.py "$OUT" "$FAULT" || echo fail >> "$OUT/.failed"
echo
echo "== host stalls (gaps over 5 s between the 1 s samples: the Mac slept or the VM froze)"
awk -F'[: ]' 'NR>1 {t=$1*3600+$2*60+$3; if (NR>2 && t-p>5) {printf "STALL %ds before %s:%s:%s\n", t-p, $1, $2, $3; n++}; p=t}
     END {if (!n) print "none"}' "$OUT/samples.txt" | tee "$OUT/stalls.txt"
echo
echo "== samples (outbox pending, oldest unpublished ms, PENDING/PROCESSING): peak, and the first and last 'down'"
awk 'NR>1 && $2!="down" {if ($2>p) {p=$2; pl=$1}; if ($3>l) {l=$3}; if ($4>f) {f=$4}} NR>1 && $2=="down" {if (!d1) d1=$1; d2=$1}
     END {printf "peak pending %d (at %s), peak lag %d ms, peak in-flight %d", p, pl, l, f; if (d1) printf ", postgres unreachable from %s to %s", d1, d2; print ""}' "$OUT/samples.txt"
echo
echo "== log events since the run started (service event outcome reason: count)"
"${C[@]}" logs --no-log-prefix --since "$t0" ledger-api ledger-worker fraud-engine 2>/dev/null | python3 -c '
import collections,json,sys
c=collections.Counter()
for l in sys.stdin:
    try: r=json.loads(l)
    except ValueError: continue
    e=r.get("event") or ("http.request" if r.get("message")=="http_request" else None)
    if not e or (e=="http.request" and r.get("status",0)<500) or e in ("ledger.credit","ledger.debit") and r.get("outcome")=="success":
        continue
    c[(r.get("service"),e,r.get("outcome","-"),r.get("reason") or r.get("dependency") or str(r.get("status","")) or "-")]+=1
for k,n in sorted(c.items()): print(" ".join(map(str,k))+":",n)'
echo
echo "== fraud-engine consumer lag after the run"
"${C[@]}" exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --all-groups 2>/dev/null |
  awk '$6 ~ /^[0-9]+$/ {lag[$1]+=$6} END {for (g in lag) print g, "lag", lag[g]}'
} 2>&1 | tee "$OUT/report.txt"

# A full reconciler sweep: ledger-worker sweeps everything when it gains leadership.
{
echo
echo "== reconciler full sweep after the run"
[[ "$FAULT" == worker ]] && { "${C[@]}" up -d --no-build --scale ledger-worker=1 ledger-worker >/dev/null 2>&1; sleep 10; }
restarted=$(now)
"${C[@]}" restart ledger-worker >/dev/null 2>&1
line=
for _ in $(seq 1 60); do
  line=$("${C[@]}" logs --no-log-prefix --since "$restarted" ledger-worker 2>/dev/null | grep '"ledger.reconcile"' | grep '"mode":"full"' |
    grep -E '"outcome":"(success|failure)"' | grep -v '"check"' | grep -v '"error":"context canceled"' | tail -1)
  [[ -n "$line" ]] && break
  sleep 5
done
if [[ -z "$line" ]]; then
  echo "FAIL  no full sweep summary within 5 minutes"; echo fail >> "$OUT/.failed"
else
  echo "$line" | python3 -c '
import json,sys
r=json.loads(sys.stdin.read())
print("PASS " if r.get("outcome")=="success" else "FAIL ", " ".join(f"{k}={r.get(k)}" for k in ("mode","outcome","reason","transactions","ledgers","accounts","mismatches","duration_ms") if k in r))'
  [[ "$line" == *'"outcome":"success"'* ]] || echo fail >> "$OUT/.failed"
fi
} 2>&1 | tee -a "$OUT/report.txt"
echo "results in $OUT/"
if grep -q STALL "$OUT/stalls.txt"; then
  echo "INVALID: the host stalled during the run, so its timings mean nothing; rerun it with the Mac awake (on power)"; exit 3
fi
[[ ! -e "$OUT/.failed" ]] && (( k6_exit == 0 ))
