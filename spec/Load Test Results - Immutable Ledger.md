# Load Test Results: Immutable Ledger

**Plan step:** P6.2 (`test/ledger-load`), TRD §13.4 · **Runs:** 2026-11-08 · **Code:** `main` after #36

## 1. Verdict

| NFR | Target | Result on this machine | Status |
| :--- | :--- | :--- | :--- |
| NFR-5 success ratio | ≥ 99.99% | 100.00% of 1,071,540 writes; 0 `FAILED processing_timeout`; nothing left PENDING | ✅ Met |
| Reconciler after the run | clean | Full sweep of 1,081,550 transactions, 10,010 ledgers and accounts: 0 mismatches | ✅ Met |
| NFR-8 outbox lag | p99 < 1 s | p99 980 ms (max 2.5 s) | ⚠️ Met, barely |
| NFR-1 throughput | 300 TPS sustained; 600 TPS for 5 min | 514 req/s offered; 8,462 requests k6 could not start on time | ❌ Not met locally |
| NFR-2 write latency | p50 ≤ 30 ms, p99 ≤ 150 ms | p50 4.6 ms, p99 1.41 s (steady phase) | ❌ Not met locally |

The figures are from the full run with production-like Postgres settings (run `1794144048`, §3).

**NFR-1 and NFR-2 can't be confirmed on this machine.** The limit is how fast its disk completes a flush, not the ledger code. With `synchronous_commit=off`, set as a diagnostic only, the same load met every target. Steady p99 was 50 ms, 10 of 692,013 requests were dropped, and lag p99 was 58 ms (§4.2).

The run did find one real bottleneck in the code: a `NOTIFY` lock that serialized every commit. It was fixed in #36 (§4.1).

**To close NFR-1 and NFR-2:** repeat the full run on a Linux host with a local SSD, as in §6.

## 2. Setup

| Item | Value |
| :--- | :--- |
| Machine | Apple M-series laptop, macOS; Docker Desktop VM with 10 CPUs and 7.7 GiB, holding the whole stack *and* k6 |
| Stack | Throwaway compose project (`COMPOSE_PROJECT_NAME=loadcheck`), started with `make load-up`: Postgres 17, Kafka 4.1.0, Redis, one ledger-api, ledger-worker, fraud-engine, Loki, Alloy, Grafana |
| Profile | `make load` (`PROFILE=full`): **mixed** 300/s for 30 min (60% debit, 35% credit, 5% void, amounts 1–1,000 IDR) across 10,000 accounts; **hot** 10 accounts at 20/s each (200/s), at the same time; then a **spike** of mixed load at 600/s for 5 min. In total, 500 writes/s for 30 minutes |
| Accounts | Users inserted directly into Postgres, because 10,000 bcrypt-12 registrations would take most of an hour. Accounts are opened and funded with 100,000,000 IDR each through ledger-api |
| Fraud thresholds | Raised to 10⁹ (`make load-up`). With the real thresholds (more than 3 top-ups in 5 min, more than 30 debits in 1 h), each mixed account gets about 3 top-ups per 5 minutes and about 65 debits per hour. Most accounts would be frozen within minutes, and the hot ones within seconds. The run would then measure `account_frozen` 422s rather than writes. The fraud engine still consumes every event: 0 `fraud.decision` |
| Measurement | k6 latency per request (client side, inside the compose network); ledger-api access log `latency_ms` from Loki (server side); outbox lag sampled every 0.5 s from `ledger.outbox`; Postgres wait events sampled every second from `pg_stat_activity` |

## 3. Runs

| Run | Postgres | Requests (rate) | Dropped | Steady p50 / p99 | p99 all phases (max) | Outbox lag p50 / p99 / max | Reconciler (full sweep) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `1794141574` | stock: `shared_buffers=128MB`, `max_wal_size=1GB`, 5 min checkpoints | 1,085,561 (511.8/s) | 14,465 | 4.3 / 563 ms | 1,729 ms (6.7 s) | 11 / 402 / 1,639 ms | clean, 1,075,551 tx in 146 s |
| `1794144048` | tuned (`make load-up`): `1GB`, `8GB`, 15 min, `wal_compression=lz4` | 1,091,560 (514.4/s) | 8,462 | 4.6 / 1,419 ms | 1,394 ms (3.6 s) | 11 / 980 / 2,537 ms | clean, 1,081,550 tx in 164 s |
| `1794146549` (diagnostic) | tuned + `synchronous_commit=off`; 20 + 2 min | 692,013 (516.5/s) | 10 | 2.4 / 50 ms | 58 ms (0.7 s) | 8 / 58 / 321 ms | clean, 682,003 tx in 42 s |

Every run had a 100.00% success ratio (all 2xx, or a business 422), no 5xx, and 0 `processing_timeout`.

### How latency changed during the run (server-side p99, 2-minute windows, ms)

| Minute of the steady phase | 0–2 | 2–4 | 4–6 | 6–8 | 8–10 | 10–12 | 12–14 | 14–16 | 16–18 | 18–20 | 20–22 | 22–24 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| stock | 41 | 58 | 77 | 155 | 167 | 155 | 669 | 1,210 | 240 | 1,603 | 397 | 484 |
| tuned | 44 | 37 | 73 | 91 | 219 | 92 | 70 | 88 | 1,187 | 2,133 | 2,397 | 1,971 |
| diagnostic | 22 | 34 | 18 | 19 | 180 | 48 | 46 | 71 | 98 | 32 | 76 | 98 |

With durable commits, latency starts within target. It climbs to seconds once the database starts writing data pages back to disk (§4.2). The diagnostic run stays flat.

## 4. Findings

### 4.1 Every write committed one at a time behind `NOTIFY` (fixed in #36)

**What happened:**
- Each write called `pg_notify` inside its transaction to wake the outbox relay.
- A transaction that has notified takes a database-wide lock at `COMMIT` (`AccessExclusiveLock` on the `pg_database` object) and holds it through the WAL flush.

**Evidence** (quick profile, before #36):
- Steady p99 was 302–397 ms on every run.
- Up to 30 backends queued on `Lock:object`.
- `pg_locks` showed one holder with about 11 waiters.

**Fix:** ledger-api now sends one coalesced `NOTIFY` per 10 ms, after the commit (`outbox.Notifier`, TRD §7.3).
- **Effect:** the `Lock:object` waits disappeared, and quick-profile p99 fell to 63–112 ms in two of three runs.
- **Cost:** about 6 ms of extra outbox lag at p50.

### 4.2 Commits wait on the disk flush, which this machine does slowly under load (environment)

After #36, the dominant wait is `LWLock:WALWrite`, behind one `IO:WalSync`. That is group commit: the committing backends queue behind a single WAL flush.

| | stock | tuned |
| :--- | ---: | ---: |
| Backend-samples waiting on `LWLock:WALWrite` | 3,832 | 5,195 |
| Snapshots with an `IO:WalSync` in progress | 767 of 1,681 | 806 of 1,735 |

So a WAL flush was in progress in nearly half of all the one-second snapshots.

**Why we think it's the disk:**
- **Diagnostic run.** Turning off the commit flush (`ALTER SYSTEM SET synchronous_commit = off`, throwaway database only) took steady p99 from 1,419 ms to 50 ms. Throughput stayed the same (516/s) and the latency timeline went flat. Nothing else changed.
- **Device flushes.** The VM's disk (`vda`, `/proc/diskstats`) took **60 ms per device flush** on average during the diagnostic run, while writing 26 MB/s. Over the VM's whole uptime, which includes the durable-commit runs, the average was **314 ms** (18,700 flushes in 5,874,624 ms). An idle `pg_test_fsync` reports 78 µs per `fdatasync`. Under sustained load, Docker Desktop's virtual disk turns each flush into a host-level flush that takes tens to hundreds of milliseconds. On a server-class SSD, a flush takes well under 1 ms.
- **When it got worse.** Latency degrades once data pages start being written back:
  - **Stock settings:** checkpoints ran every minute (41 requested against 1 timed), and backends wrote 2.3 M dirty pages themselves.
  - **Tuned settings:** the first checkpoint triggered by WAL volume came at about 4 GB of WAL, roughly 13 minutes in. That matches the jump in the timeline above.

  Either way, the page writes compete with WAL flushes on the same slow device.

**What this rules out:**
- **Not the reconciler.** A quick run with `RECONCILE_INTERVAL=1h` had the same p99 (302 ms). During the full runs, all 33–34 incremental reconcile runs were clean.
- **Not Postgres memory or checkpoint settings.** The tuned settings cut requested checkpoints from 41 to 3, full-page images from 1.97 M to 0.22 M and backend page writes from 2.34 M to 0.26 M, and raised the cache hit ratio to 99.7%. Latency got worse, not better. `make load-up` keeps the tuned settings because production would use them, but they don't fix this.

### 4.3 Each write produces about 11 KB of WAL, and the outbox is the largest table (follow-up)

| | Tuned run |
| :--- | :--- |
| WAL written | 12 GB for 1.09 M requests, about 11 KB each (diagnostic run: 11.4 KB) |
| `ledger.outbox` | 3,682 MB |
| `ledger.events` (Nov) | 1,388 MB |
| `ledger.idempotency_keys` | 896 MB |
| `ledger.journal_entries` (Nov) | 578 MB |
| `ledger.transactions` | 446 MB |

Each write writes its events twice (to `events` and to the outbox envelope). On top of that, the relay's `UPDATE … SET published_at` rewrites each whole outbox row, payload included, and the old version stays as a dead tuple until vacuum runs. Published rows are kept for 3 days (`OUTBOX_RETENTION`).

This WAL volume is what the slow flushes in §4.2 have to absorb. It's also worth reducing on any disk. Options, not yet decided:
- Delete outbox rows once they're published, instead of marking them.
- Record publication in a separate narrow table.
- A lower `fillfactor`, so the update can stay on the same page (a HOT update).

### 4.4 Correctness held under load

Correctness held in all three runs:
- **Success ratio:** 100.00% across 2.81 M load requests.
- **No stuck transactions:** no `processing_timeout`, and nothing left PENDING after the sweeper.
- **Reconciler:** a clean full sweep after every run, over up to 1.08 M transactions and 10,010 ledgers, including the 10 hot accounts with about 36,000 postings each.
- **Voids:** about 5% of requests voided an earlier settled transaction. 53,118 voids settled in the tuned run.

## 5. Limits of this test

- **Shared machine.** k6, the observability stack and the services all share one VM's 10 CPUs. In a quick-profile probe, Postgres averaged about 75% of one CPU, ledger-api 37% and k6 16%.
- **Single replicas.** One ledger-api and one Postgres; there are no replicas.
- **Client-side latency.** k6 measures inside the compose network, so its latency includes container networking, not just server time.
- **Fraud thresholds raised** (see §2).

## 6. How to run it

```bash
COMPOSE_PROJECT_NAME=loadcheck make load-up        # throwaway project: keeps the load data out of your stack
COMPOSE_PROJECT_NAME=loadcheck make load           # full profile, about 45 minutes including setup and the reconciler sweep
COMPOSE_PROJECT_NAME=loadcheck make load PROFILE=quick   # about 2 minutes, to check the setup
COMPOSE_PROJECT_NAME=loadcheck docker compose down -v    # remove it afterwards
```

The run writes these files to `load-results/<run>/`:
- `k6.txt` (k6's summary and thresholds) and `summary.json`;
- `lag.txt` (the outbox lag samples);
- `report.txt` (the NFR report and the reconciler result).

k6 exits 99 when a threshold fails.

To verify NFR-1 and NFR-2, run the full profile on a Linux host with a local NVMe or SSD, and with k6 on another machine if possible. Pass `LEDGER_URL` to the k6 container to point it at the API.
