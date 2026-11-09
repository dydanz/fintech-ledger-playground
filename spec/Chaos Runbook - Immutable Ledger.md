# Chaos Runbook: Immutable Ledger

**Plan step:** P6.3 (`docs/ledger-chaos-runbook`), TRD §13.5 · **Runs:** 2026-11-08 · **Code:** `main` after #37, harness commit `764da16`

This runbook has two uses:
- **To repeat a drill:** §3 has the steps, §4 what to watch.
- **For the recorded results:** §2 is the verdict, §5 the run details, §6 what the drills found.

## 1. What a drill checks

Each drill injects one fault while a client writes 50 credits and debits per second across 100 accounts. The client behaves like a payments caller: a request that gets a 5xx, no response or `409 request_in_progress` is retried every second with the same `Idempotency-Key`, until it gets a final answer.

After the run, `scripts/chaos/check.py` compares the system with what the client was told:

| Check | Passes when |
| :--- | :--- |
| Every request answered | No request was still failing after 180 s of retries |
| Confirmed transactions in Postgres | Every transaction id the client received exists, and no two keys share one |
| States match the answers | A 201 or 202 is SETTLED (a 202 through the sweeper); a 422 is FAILED |
| Nothing extra | No transaction exists for these accounts that the client wasn't told about: no partial or duplicate writes |
| Nothing stuck | No PENDING or PROCESSING left, and no `FAILED processing_timeout` |
| Balances | Each account's balance equals the sum of its confirmed amounts |
| Events | Every outbox row written during the run is on `ledger.events.v1`. Duplicates are allowed (at-least-once delivery) and counted |
| The fault's own expectation | §13.5, listed per fault in §2 |
| Reconciler | A full sweep after the run (forced by restarting `ledger-worker`) finds 0 mismatches |

The runner also samples the outbox and in-flight transactions every second.
- **Stalls:** a gap of more than 5 s between samples means the host slept or the VM froze, and the run is reported `INVALID` (§7).
- **Logs:** it counts every non-success log event from ledger-api, ledger-worker and fraud-engine.

## 2. Verdict

| Fault | §13.5 expects | Result | Status |
| :--- | :--- | :--- | :--- |
| Stop Redis (60 s) | Writes continue; idempotency falls back to Postgres | 9,101 of 9,101 writes answered on the first try. Write time p50 191 ms, p99 235 ms during the outage (5 ms / 55 ms before) | ✅ Met (latency: §6.2) |
| Stop Kafka (5 min) | Writes continue; the outbox grows and drains after recovery with no loss | 21,101 of 21,101 answered on the first try, latency unchanged. The outbox grew to 46,320 rows and drained 9 s after Kafka started. 0 of 63,503 events lost, 458 delivered twice | ✅ Met (alerting: §6.1) |
| Kill -9 Postgres (30 s) | 503 `ledger_unavailable`; no partial state; the sweeper finishes any PENDING after recovery | 866 requests failed for up to 34 s. Their 15,303 failed attempts were all `503 ledger_unavailable`, except 1 connection cut at the kill. All 866 succeeded with their keys after recovery: no missing, extra or duplicate transactions. One write had committed only Tx1 and got a 202; the sweeper completed it (`ledger.recover stuck_pending`) | ✅ Met |
| Kill -9 the `ledger-worker` leader | A new leader within 10 s | All 7 jobs taken over by the other replica 2.2 s after the kill; writes unaffected; outbox lag peaked at 1.3 s | ✅ Met |

Every recorded run also passed the common checks in §1 and had no host stalls. Each was followed by a clean full sweep: 0 mismatches over up to 96,962 transactions and 1,100 ledgers.

**Problems found:**
- **Fixed** (`fix/outbox-publish-stall`): a Kafka outage blocked the outbox relay for its whole length, so the relay didn't report lag or escalate errors while it lasted. The lag alert fired 4.5 minutes in, through its "no heartbeat" path. After the fix, the same drill fired the alert on the lag value 2.5 minutes in (§6.1).
- **Open:** a hung (not dead) worker leader keeps its jobs indefinitely (§6.3). This is outside §13.5 and was found with an extra probe.

## 3. How to run a drill

Use a throwaway compose project. The drills stop and kill containers, and the accounts they create stay in the database.

```bash
COMPOSE_PROJECT_NAME=chaos make load-up                 # make up, fraud thresholds raised (see below)
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=redis       # about 4 minutes
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=kafka       # about 8 minutes
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=postgres    # about 3 minutes
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=worker      # about 3 minutes
COMPOSE_PROJECT_NAME=chaos docker compose down -v       # remove it afterwards
```

**Fraud thresholds:** `make load-up` raises them. Each account receives about 18 writes a minute, and the real thresholds would freeze the accounts within a minute, so the drill would measure `account_frozen` instead of the fault.

**Host:** keep it awake and on power. On battery, macOS sleeps despite `caffeinate`, and a run with a stall is reported `INVALID` (exit 3).

**Exit code:** `run.sh` exits non-zero when any check fails.

**Results:** each run writes `chaos-results/<run>-<fault>/` with:
- `report.txt`: the timeline, the checks, the log event counts and the reconciler result;
- `k6.txt` and `summary.json`;
- `k6-log.json`: every final answer and failed attempt;
- `samples.txt`: the 1 s samples.

**Settings:** `HOLD`, `WARMUP`, `TAIL`, `RATE`, `ACCOUNTS` and `RETRY_FOR` override the defaults.

### What each fault does

| `FAULT` | Injection | Recovery |
| :--- | :--- | :--- |
| `redis` | `docker compose stop redis`, 30 s after the first write | `start redis` after `HOLD` (60 s) |
| `kafka` | `docker compose stop kafka`. Every 30 s it records the outbox backlog and the state of the alert `Ledger outbox lag above 5 s` | `start kafka` after `HOLD` (300 s); records how long the outbox takes to drain |
| `postgres` | `docker compose kill -s SIGKILL postgres`: a crash, not a clean shutdown | `start postgres` after `HOLD` (30 s); records when it accepts queries and when nothing is PENDING |
| `worker` | Scales `ledger-worker` to 2 replicas, finds the one holding the `outbox_relay` lock, and runs `docker kill -s KILL` on it | Records when the other replica logs `app.leader state=acquired` for each job; scales back to 1 at the end |

### Running a drill by hand

The steps for a shared or production-like environment are the same as in the table: inject, hold, recover. Watch the signals in §4 while the fault is active.

- **Afterwards:** check that the ledger dashboard is clean, and that the next `ledger.reconcile` summary has `mismatches=0`.
- **Forcing a full sweep:** restart `ledger-worker`. The replica that gains `reconciler` leadership sweeps everything.

## 4. What to watch during each fault

| Fault | Expected signals (Loki: `{service="…"} \| json \| event="…"`) | Not expected |
| :--- | :--- | :--- |
| Redis | ledger-api `dependency.call` warn, `dependency=redis`, `reason` `timeout` or `unknown`: two per write (§6.2). fraud-engine `dependency.call` warn while it retries. Writes stay 201 | Any 5xx; `ledger.idempotency` replays (there are none without retries) |
| Kafka | `outbox.publish` warn `kafka_unavailable` every 10–15 s (the 10 s publish timeout plus backoff), error from 1 minute on. `outbox.lag` warn every 30 s with the growing `pending` and `lag_ms`. The alert `Ledger outbox lag above 5 s` goes Pending within about 30 s and fires about 2.5 min in. After recovery: `outbox.lag` back to info, lag 0. (Before the fix in §6.1, `outbox.lag` went silent and the alert fired at about 4.5 min, on NoData.) | Any write failure; outbox rows still unpublished a minute after Kafka is back |
| Postgres | Every write is 503 `ledger_unavailable`, answered quickly (access log: p50 3 ms, p99 95 ms, max 110 ms). ledger-api `ledger.credit`/`ledger.debit` failure `pg_error`. ledger-worker `app.leader` failure `connection_lost` and `pg_error`, then `acquired` again after recovery. `outbox.publish` failure `pg_error`. After recovery: `ledger.recover stuck_pending` for any write that committed only Tx1 | Requests hanging until the client timeout; `processing_timeout`; reconciler mismatches |
| Worker | The surviving replica logs `app.leader state=acquired` for all 7 jobs within the 5 s `AcquireEvery`. Outbox lag peaks at about 1 s | Lag beyond a few seconds; any job without a leader |

## 5. Recorded runs

All on commit `764da16`, compose project `chaos`: Postgres 17, Kafka 4.1.0 (single broker), Redis 7.4, one ledger-api, one ledger-worker (two in the worker drill) and fraud-engine. Docker Desktop on an Apple M-series Mac, on power.

| Fault | Run | Writes | Retried | Write time p50 / p99 before → during → after | Outbox peak | Full sweep |
| :--- | :--- | ---: | ---: | :--- | :--- | :--- |
| redis | `1794175965` | 9,101 | 0 | 5 / 55 → 191 / 235 → 5 / 29 ms | 5 rows, 14 ms | clean, 62,985 tx |
| kafka | `1794176181` | 21,101 | 0 | 5 / 20 → 4 / 18 → 5 / 56 ms | 46,320 rows, 309 s | clean, 84,086 tx |
| postgres | `1794176637` | 6,775 | 866 (max 35 tries, 34.6 s) | 5 / 32 → 6 / 22 → 5 / 33,459 ms ¹ | 2,749 rows, 35 s | clean, 90,861 tx |
| worker | `1794176824` | 6,101 | 0 | 5 / 12 → 5 / 19 → 5 / 21 ms | 196 rows, 1.3 s | clean, 96,962 tx |

¹ The time until a final answer, retries included, grouped by when the answer came. Requests failing during the outage are answered after it, so they land in "after".

The full sweep's counts grow from run to run because the drills share one database.

**Timelines from the reports (UTC):**

- **Kafka:**

  | Time | Event |
  | :--- | :--- |
  | 22:16:54 | Kafka stopped |
  | 22:19:26 | Alert Pending (NoData) |
  | 22:21:27 | Alert firing (NoData), backlog 40,918 rows |
  | 22:21:57 | Kafka started |
  | 22:22:06 | Outbox drained |

- **Postgres:**

  | Time | Event |
  | :--- | :--- |
  | 22:24:30 | Killed |
  | 22:25:00 | Started |
  | 22:25:04 | Accepting queries; the last failed attempt |
  | 22:25:08 | Nothing PENDING |

- **Worker:**

  | Time | Event |
  | :--- | :--- |
  | 22:27:51 | Leader killed, holding all 7 jobs, outbox pending 2 |
  | 22:27:53 | All 7 jobs acquired by the other replica (2.2 s after the kill) |

## 6. Findings

### 6.1 A Kafka outage blocked the outbox relay, hiding the lag (fixed)

**What happened:**
- The first publish after the stop failed at once (22:17:07, `unable to dial`). The relay's next batch of 500 rows then blocked: `Publish` returned only at 22:22:03, after Kafka was back, with `failing_ms=296244`. `OUTBOX_PUBLISH_TIMEOUT` is 10 s.
- While it was blocked, the relay's loop didn't run, so:
  - no `outbox.lag` events: the heartbeat went silent;
  - no escalation of `outbox.publish` to error after 1 minute. The error line was logged at recovery.
- The alert `Ledger outbox lag above 5 s` fired only through its NoData path ("no outbox.lag heartbeat for 2 minutes"). It went Pending at about 2.5 min and fired at about 4.5 min. Its summary blamed a relay that wasn't running, when Kafka was down.

**Cause:**
- `Relay.Drain` bounds each batch with `context.WithTimeout(ctx, PublishTimeout)` and calls franz-go's `ProduceSync`.
- franz-go fails buffered records on context cancellation only when that's "safe": with idempotent production (the default), it isn't once a produce request has been sent. The client then retries until the broker answers.
- `TestStalledPublishIsLoggedAndRetried` passes because its fake publisher honours the context. The real client doesn't.

**Why nothing was lost:** events stay in the outbox until they're published, and they all drained within 9 s of recovery. Detection is what suffers:
- the lag alert is late and misleading;
- the publish error never escalates while the outage lasts.

**Fix** (`fix/outbox-publish-stall`):
- **Publish:** `kafka.Producer.Publish` returns when its context ends, leaving the records in flight. Until they settle it produces nothing new and waits for them instead. So however often the relay retries, an outage costs at most one extra copy of one batch.
- **Heartbeat:** the relay logs `outbox.lag` from its own goroutine, so a slow or stuck publish can't silence it.
- **Tests:**
  - **Kafka tests:** a broker that holds produce requests unanswered (franz-go's `kfake`) reproduces the stall with the real client. `TestPublishReturnsAtTheDeadlineWhenTheBrokerStopsAnswering` and `TestPublishDoesNotRebufferWhileAnAbandonedBatchIsInFlight` both fail on the old `Publish`.
  - **Relay test:** `TestLagIsReportedWhilePublishIsStuck` uses a publisher that ignores its context, and fails with the old inline heartbeat.

**The drill after the fix** (run `1794220886`, commit `468ec06`):

| | Before (run `1794176181`) | After |
| :--- | :--- | :--- |
| `outbox.publish` failures logged during the 5-minute outage | 2 (the error only at recovery) | 22, warn then error from 71 s on |
| `outbox.lag` during the outage | none | every 30 s, `lag_ms` up to 287,606 |
| Alert `Ledger outbox lag above 5 s` | Pending at 2.5 min on NoData, firing at 4.5 min | Pending at 33 s on the lag value, firing at 2 min 33 s |
| Backlog cleared after Kafka started | 8 s | 11 s: the relay may be up to 5 s into its retry backoff when the broker returns |
| Events delivered twice | 458 | 1 |
| Lost events, data checks, full sweep | 0 lost, all pass, clean | 0 lost, all pass, clean |

The table's backlog times are read from the 1 s samples: the first sample with at most 3 unpublished rows and lag under 100 ms. The runner's own "outbox drained" mark (9 s and 23 s) waits for a sample with exactly 0 unpublished rows. While writes continue, that depends on when the sample happens to land.

The alert also showed `Pending (NoData)` in the first minute of the after run. That came from the stack itself: it had started 51 s before the fault, and Grafana's first evaluation, 24 s after the start, had no heartbeat in Loki yet. The NoData state cleared at 10:43:00, while the lag-value path stayed Pending and then fired.

### 6.2 A Redis outage adds about 190 ms to every write

**What happens:**
- With Redis down, each write tries the idempotency cache twice (`idempotency_get` before Tx1, `idempotency_set` after). Each try waits out the 100 ms `cacheTimeout`.
- Writes keep succeeding, but they take 191 ms at p50 and 235 ms at p99. That exceeds the NFR-2 p99 target of 150 ms for as long as Redis is down.
- It's also two `dependency.call` warn lines per write: 6,148 in 60 s at 50/s. At 300 TPS that would be about 600 lines a second.

**Not decided:** whether this is acceptable for a degraded mode. If it isn't, a short circuit breaker would skip Redis after a failure, for example for 5 s, and probe again. That would remove both the latency and the log volume. Logging once per outage, as `outbox.Notifier` does, would also need an exception to the one-event-per-occurrence rule in `.claude/rules/observability-logging.md`.

### 6.3 A hung leader keeps its jobs (outside §13.5)

**The probe:**
- The kill drill covers a leader that dies: its connections close, Postgres releases the advisory locks, and the other replica takes over within `AcquireEvery`.
- As an extra probe, I froze the leader with `docker pause` for 60 s. The follower took over **no** jobs, and the frozen replica kept all 7 advisory locks.
- The frozen process's TCP connections stay open, so Postgres has no reason to end its sessions. Its pings stop, but nothing on the Postgres side notices.

**Why it matters:** a leader stuck in a GC pause, a deadlock or a frozen VM would stop every ledger-worker job until it recovers or is killed:
- the outbox relay;
- the sweeper;
- the reconciler.

**A likely fix:** set `idle_session_timeout` (e.g. 10 s) on the lock connection. The leader pings it every second (`LeaseCheck`), so it's never idle while healthy, and Postgres would end a hung leader's session and free its locks. Not tested yet.

### 6.4 Smaller observations

- **Sweeper path:** the Postgres crash caught one write between Tx1 and Tx2. The client got a 202; after recovery the sweeper completed it (`ledger.recover stuck_pending`), and a retry with the same key would replay it. Whether a run catches such a write is down to timing: the earlier, unrecorded run (`1794175306`) caught none. `TestSweeperCompletesStuckTransactions` covers this path deterministically.
- **Duplicates after a Kafka outage:** 458 of 63,503 events (0.7%) were delivered twice. That is at-least-once delivery working as designed (TRD §7.3): fraud-engine skips events it has seen (`fraud:seen:<event_id>`).
- **Fast failure:** with Postgres down, ledger-api answers 503 quickly rather than hanging (p99 95 ms in the access log). Connections fail at once for two reasons:
  - **While the container is gone:** the `postgres` hostname stops resolving (`no such host`).
  - **While it restarts:** `57P03 the database system is starting up`, or `connection refused`.

  A network partition that drops packets instead would make requests wait on the connection timeouts. This drill didn't test that.
- **Restart policy:** `docker kill` counts as a manual stop, so Docker didn't restart the killed worker (`restarts=0`). A real crash (panic, OOM kill) is restarted by `restart: unless-stopped`.
- **Fraud engine:** it retried through the Redis outage (`dependency.call` warn) and was caught up, with consumer lag 0, after every drill.

## 7. Limits

- **Single nodes.** One Kafka broker and one Postgres without a standby. Broker failover and Postgres promotion (NFR-6, RPO 0) need the production topology, so they aren't covered here.
- **Clean faults.** Containers were stopped or killed. Network partitions, packet loss, slow disks and clock skew weren't injected.
- **One host.** Every service, k6 and the observability stack share one Docker Desktop VM.
- **Log counts.** Docker keeps 3 × 10 MB of logs per container, so ledger-api's access log rotates within a few drills. The log event counts in each `report.txt` are taken right after its run. Counts read later can be smaller: of the Postgres run's 503s, only 4,751 were still in the log by the end. The client-side counts come from `k6-log.json`, which isn't rotated.
- **Host sleep.** An earlier attempt was invalidated when the Mac slept on battery mid-run (a 928 s gap). The runner now detects this and reports such a run `INVALID`.
