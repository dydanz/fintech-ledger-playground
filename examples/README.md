# Example test results

These are three passing runs from the regression on `main` at `e9e70d3` (after #39), recorded 2026-11-10. Each folder shows what a run writes and what a passing run looks like.

The stack was a throwaway compose project started with `make load-up`, on Docker Desktop on an Apple M-series Mac, on AC power with Low Power Mode off. The runs are copied unedited from `load-results/` and `chaos-results/`, which are gitignored.

| Folder | Command | Result |
| :--- | :--- | :--- |
| [`load/quick-1794293767`](load/quick-1794293767/) | `make load PROFILE=quick` | ✅ every k6 threshold passed |
| [`chaos/kafka-1794294190`](chaos/kafka-1794294190/) | `make chaos FAULT=kafka` | ✅ 9 of 9 checks |
| [`chaos/postgres-1794294645`](chaos/postgres-1794294645/) | `make chaos FAULT=postgres` | ✅ 9 of 9 checks |

## Load test, quick profile

The quick profile is the TRD §13.4 shape shortened to about 2 minutes, plus setup and the reconciler sweep:
- **mixed:** 300 writes/s across 1,000 accounts;
- **hot:** 10 hot accounts at 20/s each, alongside the mixed load;
- **spike:** then 600/s for 30 s.

| | Result | Target |
| :--- | :--- | :--- |
| Requests | 63,003 writes at 479/s, 0 dropped | NFR-1: every request starts on time |
| Steady-phase latency (k6) | p50 2.25 ms, p99 95.15 ms | NFR-2: p50 < 30 ms, p99 < 150 ms |
| Success ratio | 100.00%, steady and spike | NFR-5: ≥ 99.99% |
| Outbox lag | p50 12 ms, p99 163 ms, max 720 ms | NFR-8: p99 < 1 s |
| Stuck transactions | 0 `processing_timeout`, 0 left PENDING | |
| Reconciler full sweep | 64,013 transactions, 1,010 ledgers, 0 mismatches | clean |

**Files:**
- `report.txt`: the NFR report;
- `k6.txt`: k6's output and thresholds;
- `summary.json`: k6's `--summary-export`;
- `lag.txt`: the outbox lag samples.

The full 30-minute profile is written up in [`spec/Load Test Results - Immutable Ledger.md`](<../spec/Load Test Results - Immutable Ledger.md>).

## Chaos drill: stop Kafka for 5 minutes

| | Result |
| :--- | :--- |
| Writes | 21,100 of 21,100 answered on the first try; p99 10 ms during the outage |
| Outbox | grew to 45,332 rows while Kafka was down; drained after Kafka started |
| Lag alert | Pending from 30 s, firing from 2.5 min, on the lag value |
| Relay logs | 22 `outbox.publish` failures and 15 `outbox.lag` heartbeats during the run |
| Delivery | 0 of 63,500 events lost, 2 delivered twice |
| Database | every confirmed transaction exists once, no extra ones, every balance matches |
| Reconciler | clean full sweep |

Before #39, the same drill fired the alert only on a missing heartbeat, after 4.5 minutes, and delivered 458 events twice (runbook §6.1).

## Chaos drill: kill -9 Postgres for 30 s

| | Result |
| :--- | :--- |
| During the outage | 1,097 requests failed: all `503 ledger_unavailable`, except one connection cut at the kill |
| Recovery | Postgres accepted queries 14 s after restart (crash recovery) |
| Retries | all 1,097 requests succeeded on retry with their original `Idempotency-Key` |
| Sweeper | one write had committed only Tx1 and got a `202`; the sweeper completed it |
| Database | 6,511 confirmed transactions, 0 missing, 0 extra, 0 duplicates; every balance matches |
| Reconciler | clean full sweep |

**Files in each chaos folder:**
- `report.txt`: timeline, PASS/FAIL checks, log event counts, reconciler result;
- `timeline.txt`: the fault marks;
- `samples.txt`: outbox and in-flight transactions every second;
- `stalls.txt`: host stalls (none);
- `k6.txt` and `summary.json`: k6's output.

The raw comparison inputs are left out for size: about 21 MB per run in `k6-log.json`, `tx.txt`, `balances.txt`, `outbox.txt` and `kafka.txt`. The procedure and every recorded drill are in [`spec/Chaos Runbook - Immutable Ledger.md`](<../spec/Chaos Runbook - Immutable Ledger.md>).
