"""Compares a chaos run's database and Kafka state with what the client was told.

Usage: check.py <out-dir> <fault>. Reads, from the run directory written by run.sh:
  k6-log.json   k6's console log (one JSON line per final answer or failed attempt)
  tx.txt        id|account_id|status|failure_reason for the run's accounts
  balances.txt  account_id|balance of their IDR ledgers
  outbox.txt    event_id of every outbox row written since the run started
  kafka.txt     event_id of every message on the events topic
  timeline.txt  run.sh's marks: FAULT, RECOVER, and "takeover job=<job> after <s>s" lines
Prints one line per check, PASS or FAIL, and exits 1 if any failed.
"""
import collections
import datetime
import json
import sys

out, fault = sys.argv[1], sys.argv[2]


def lines(name):
    with open(f"{out}/{name}") as f:
        return [l.rstrip("\n") for l in f if l.strip()]


finals, failed_attempts, gave_up = [], [], []
for line in lines("k6-log.json"):
    rec = json.loads(line)
    if rec.get("source") != "console":
        continue
    e = json.loads(rec["msg"])
    e["time"] = rec["time"]
    if e.get("gave_up"):
        gave_up.append(e)
    elif "tx" in e:
        finals.append(e)
    else:
        failed_attempts.append(e)

tx = {}
for row in lines("tx.txt"):
    tid, acct, status, reason = row.split("|")
    tx[tid] = (acct, status, reason)
balances = dict(r.split("|") for r in lines("balances.txt"))
outbox = set(lines("outbox.txt"))
kafka = collections.Counter(lines("kafka.txt"))

ok = True


def report(name, passed, detail):
    global ok
    ok &= passed
    print(f"{'PASS' if passed else 'FAIL'}  {name}: {detail}")


# The client's view.
retried = [e for e in finals if e["tries"] > 1]
statuses = collections.Counter(f"{e['status']}{' ' + e['code'] if e['code'] else ''}" for e in failed_attempts)
when = sorted(e["at"] for e in failed_attempts if "at" in e)
print(f"requests {len(finals) + len(gave_up)}: {len(finals)} answered, {len(gave_up)} gave up, "
      f"{len(retried)} needed a retry (max tries {max((e['tries'] for e in finals), default=0)}, "
      f"max time to answer {max((e['ms'] for e in finals), default=0)} ms)")
print(f"failed attempts by status: {dict(statuses) or '{}'}")
if when:
    print(f"failed attempts from {when[0]} to {when[-1]}")
print(f"final answers by status: {dict(collections.Counter(e['status'] for e in finals))}")
report("every request answered", not gave_up, f"{len(gave_up)} gave up after RETRY_FOR")

# Time to a final answer (retries included), before, during and after the
# fault, from timeline.txt's FAULT and RECOVER marks (a kill has no RECOVER:
# "during" is the 15 s after it). k6 log times have 1 s resolution.
def stamp(s):
    return datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=datetime.timezone.utc).timestamp()


marks = {}
for l in lines("timeline.txt"):
    t, what = l.split(" ", 1)
    for m in ("FAULT", "RECOVER"):
        if what.startswith(m) and m not in marks:
            marks[m] = stamp(t)
if "FAULT" in marks:
    start, end = marks["FAULT"], marks.get("RECOVER", marks["FAULT"] + 15)
    phases = collections.defaultdict(list)
    for e in finals:
        if e["key"].startswith(f"chaos-{e.get('chaos')}-"):  # not the setup's funding credits
            t = stamp(e["time"])
            phases["before" if t < start else "during" if t <= end else "after"].append(e["ms"])
    for p in ("before", "during", "after"):
        v = sorted(phases[p])
        if v:
            print(f"time to answer {p} the fault: n {len(v)}  p50 {v[len(v) // 2]} ms  p99 {v[min(len(v) - 1, int(len(v) * 0.99))]} ms  max {v[-1]} ms")

# Each confirmed transaction exists exactly once, in the state the client was told.
confirmed = {}
dupes = 0
for e in finals:
    if e["tx"] in confirmed:
        dupes += 1  # two keys answered with one transaction: should never happen
    confirmed[e["tx"]] = e
want = {201: "SETTLED", 202: "SETTLED", 422: "FAILED"}  # a 202 settles via the sweeper
missing = [t for t in confirmed if t not in tx]
wrong = [(t, confirmed[t]["status"], tx[t][1]) for t in confirmed if t in tx and tx[t][1] != want[confirmed[t]["status"]]]
extra = [t for t in tx if t not in confirmed]
report("confirmed transactions in Postgres", not missing and not dupes,
       f"{len(confirmed)} confirmed, {len(missing)} missing, {dupes} keys sharing a transaction")
report("transaction states match the answers", not wrong,
       f"{len(wrong)} differ" + (f", e.g. {wrong[:3]}" if wrong else ""))
report("no transactions the client wasn't told about", not extra,
       f"{len(extra)} extra" + (f", e.g. {[(t, tx[t][1]) for t in extra[:3]]}" if extra else ""))
pending = [t for t, v in tx.items() if v[1] in ("PENDING", "PROCESSING")]
timeouts = [t for t, v in tx.items() if v[2] == "processing_timeout"]
report("nothing left PENDING, no processing_timeout", not pending and not timeouts,
       f"{len(pending)} pending, {len(timeouts)} processing_timeout")

# Balances: the sum of the confirmed amounts, per account.
expected = collections.Counter()
for e in finals:
    expected[e["acct"]] += e["delta"]
bad = [(a, expected[a], int(balances.get(a, 0))) for a in expected if int(balances.get(a, 0)) != expected[a]]
report("balances equal the confirmed amounts", not bad,
       f"{len(expected)} accounts, {len(bad)} differ" + (f", e.g. {bad[:3]}" if bad else ""))

# Events: every outbox row of the run reached Kafka (duplicates are allowed: at least once).
lost = outbox - set(kafka)
dup = sum(n - 1 for e, n in kafka.items() if e in outbox and n > 1)
report("every outbox event on Kafka", not lost,
       f"{len(outbox)} outbox events, {len(lost)} missing from the topic, {dup} delivered more than once")

# The fault's own expectation (TRD §13.5).
if fault in ("redis", "kafka", "worker"):
    report(f"{fault}: writes continue", not failed_attempts, f"{len(failed_attempts)} failed attempts")
if fault == "postgres":
    other = [e for e in failed_attempts if (e["status"], e["code"]) not in ((503, "ledger_unavailable"), (0, ""))]
    report("postgres: failures are 503 ledger_unavailable", bool(failed_attempts) and not other,
           f"{len(failed_attempts)} failed attempts, {len(other)} of another kind"
           + (" (none at all: did the fault happen?)" if not failed_attempts else ""))
if fault == "worker":
    takeovers = {}
    for l in lines("timeline.txt"):
        if "takeover job=" in l:
            job, secs = l.split("takeover job=")[1].split(" after ")
            takeovers[job] = float(secs.rstrip("s"))
    slow = {j: t for j, t in takeovers.items() if t > 10}
    report("worker: a new leader within 10 s", bool(takeovers) and not slow,
           f"{len(takeovers)} jobs taken over, slowest {max(takeovers.values(), default=0):.1f} s"
           + (f", over 10 s: {slow}" if slow else ""))

sys.exit(0 if ok else 1)
