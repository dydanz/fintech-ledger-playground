# Development Plan: Immutable Ledger & Event-Sourced Balance System

| | |
| :--- | :--- |
| **Status** | Draft for review |
| **Date** | 2026-11-06 |
| **Source** | [TRD](<TRD - Immutable Ledger & Event-Sourced Balance System.md>), [BRD](<BRD - Immutable Ledger & Event-Sourced Balance System.md>) |
| **Repo** | `github.com/dydanz/fintech-ledger-playground` (renamed from `random-learning-loki-kafka`) |

This plan breaks TRD §14 into seven phases.

- **One phase = one GitHub issue.**
- **One issue = one or more PRs.** Where a phase is too large to review in one go, it is split into stacked PRs.
- Every PR is small enough to review in one sitting, and leaves `main` green and deployable.

TRD section numbers are cited as `§n`.

---

## 1. Phase overview

| Phase | Issue title | PRs | Depends on | Size | TRD exit criteria |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **P0** | `[Ledger P0] Shared foundations` | 3 (parallel) | — | S | — (enablers only) |
| **P1** | `[Ledger P1] Core ledger: schema, domain, credit & debit` | 6 (2 parallel + stack of 4) | P0 | L | §14 P1 |
| **P2** | `[Ledger P2] Void, freeze, idempotency cache, recovery` | 4 | P1 | M | §14 P2 |
| **P3** | `[Ledger P3] Event streaming: outbox relay to Kafka, dashboards, alerts` | 3 (stack) | P1 (P2 for the worker) | M | §14 P3 |
| **P4** | `[Ledger P4] Fraud engine` | 2 (stack) | P2, P3 | M | §14 P4 |
| **P5** | `[Ledger P5] Integrity: reconciler, projection rebuild, partitions` | 3 | P2 (worker) | M | §14 P5 (part) |
| **P6** | `[Ledger P6] Retention, load and resilience` | 3 | P3, P5 | M | §14 P5 (part), NFR-1/2/5/8/9 |

TRD §14's P5 bundled integrity, retention and performance. This plan splits it into P5 and P6 so each issue stays reviewable. The TRD's other phases map one to one.

### 1.1 Dependency graph

```
P0 ──▶ P1 ──┬──▶ P2 ──┬──────────▶ P4
            │         │            ▲
            └──▶ P3 ──┼────────────┘
                      │
                      └──▶ P5 ──▶ P6
                 (P3 also feeds P6)
```

- P2 and P3 can run in parallel after P1. P3.2 needs the `ledger-worker` job runner from P2.4. If P3 starts first, P3.2 rebases onto P2.4.
- P5 can start as soon as P2.4 (the worker) is merged.

---

## 2. Working conventions

### 2.1 Issues

- **Title:** `[Ledger Pn] <name>`. **Labels:** `ledger`, `phase:Pn`, plus `blocked` while an open question gates it.
- **Body:** §4 holds the body for each issue. The PR table, acceptance checklist and blocking questions are copied as is.
- An optional tracking issue `[Ledger] Delivery tracker` lists P0–P6 as a task list.

### 2.2 PRs

- **Branches.** Branch names follow `.claude/rules/git-workflow.md` (`<type>/<short-kebab-desc>`). A PR that doesn't depend on another branches from a freshly pulled `main`.
- **Stacked PRs.**
  - PR *n+1* branches from PR *n*'s branch and is opened with `gh pr create --base <PR n branch>`.
  - When PR *n* merges, rebase PR *n+1* onto `main` (`git rebase --onto origin/main <PR n branch>`) and retarget it with `gh pr edit --base main`.
  - Merge strictly from the bottom of the stack up.
  - `.claude/rules/git-workflow.md` rule 3 says all work starts from `main`. Stacked PRs are the one exception, because the first branch of a stack still starts from `main`. §5 proposes recording this in the rule.
- **PR body.**
  - `Part of #<issue>` on every PR.
  - `Closes #<issue>` only on the last PR of the phase.
  - `Stacked on #<PR>` when stacked.
- **Never merged without review.** Nobody merges their own PR unless the repo owner asks.

### 2.3 Definition of done (every PR)

1. `make vet` and `make test` (`-race`) pass. `make test-integration` passes for PRs that touch SQL or Kafka.
2. Logging rule DoD (`.claude/rules/observability-logging.md` §7):
   - New events are emitted.
   - Failure, denied and partial paths are asserted in tests.
   - Catalog rows are added in the same PR.
   - Error-level events have a panel in `deploy/grafana/dashboards/ledger.json`.
3. Nothing from TRD §11's never-log list (`reference`, `metadata`, raw idempotency keys, tokens) appears in logs.
4. New env vars are added to `internal/config`, `.env.example` and README with the TRD §12.2 defaults.
5. If the implementation departs from the TRD, the TRD is updated in the same PR.
6. The README and `scripts/smoke-ledger.sh` (from P1) are updated when the user-visible API changes.

---

## 3. Open questions that gate work

| TRD Q | Blocks | Proceed on TRD default? |
| :--- | :--- | :--- |
| Q1 internal auth model | P1.5 (`ClientAuth`) | Yes. Service tokens are behind an interface, so mTLS can be added later without changing callers. |
| Q2 overdraft products | P1.4 | Yes (default limit 0, allow-list) |
| Q3 void floor / void on frozen | P2.2 | **Confirm before P2.2 merges.** It changes the decision logic and tests. |
| Q5 currencies | P1.1 (seeded system ledgers) | Yes (`IDR`) |
| Q6 unfreeze control | P2.3 | Yes (`ops` only) |
| Q7 debit-volume thresholds | P4.1 | Yes, configurable. Real values are needed before production, not before merge. |
| Q8 retention jurisdiction / legal hold | P6.1 | **Confirm before P6.1 starts.** |
| Q9 account auto-open on register | P1.5 | Yes (explicit `POST /internal/v1/accounts`) |
| Q11 migration runner | P5.1 (migration 005) | **Decided in P5:** golang-migrate (P5.4, merged before P5.1). |

---

## 4. Phases

### P0: Shared foundations

**Goal.** Land the cross-cutting changes the ledger needs, without any ledger code yet, so P1 PRs only contain ledger logic.

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 0.1 | `feat/trace-generation` (`main`) | `api.RequestContext` generates a W3C `traceparent` (random 16-byte trace id, 8-byte span id) when none or an invalid one arrives, and echoes it in a `traceparent` response header (§8). This also changes auth-api, which is intended. | Generated `trace_id`/`span_id` appear in logs and the header; a valid inbound header is honoured; an invalid one is replaced |
| 0.2 | `chore/multi-binary-build` (`main`) | Dockerfile `ARG CMD=api` selects `cmd/$CMD`; compose `api` passes `CMD=api`. Makefile `test-integration` (`go test -race -tags integration ./...`). `internal/testutil/pgtest` helper that connects to the compose Postgres via `TEST_DATABASE_URL` and skips when unset. Adds `github.com/google/uuid`. | `docker compose build` and `make up` still pass; `make test-integration` runs (no tests yet) |
| 0.3 | `docs/ledger-logging-domains` (`main`) | `observability-logging.md`: add domains `ledger`, `fraud` and `outbox`; the new allowed fields (`account_id`, `ledger_id`, `transaction_id`, `amount`, `currency`, `client_id`/`actor`, `rule`, `pending`, `lag_ms`); and the never-log additions (§11). Catalog rows are **not** added here. Each phase adds its own, as the rule requires. | — |

**Acceptance.**
- [ ] Existing auth-api tests and `scripts/smoke.sh` pass unchanged.
- [ ] A request without `traceparent` gets one back, and its `trace_id` is in the access log.
- [ ] `make test-integration` is wired into the README.

---

### P1: Core ledger (schema, domain, credit and debit)

**Goal.**
- Accounts with sub-ledgers.
- `CREDIT` and `DEBIT` end to end through Tx1 and Tx2 (§7.1).
- Postgres-backed idempotency.
- Outbox rows written for every event. Nothing relays them until P3.
- User read endpoints.

**Out of scope.** The Redis idempotency cache, void, freeze endpoints and sweeper (all P2), and the relay (P3).

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 1.1 | `feat/ledger-schema` (`main`) | `migrations/002_ledger.{up,down}.sql` exactly as §5. A `DO` block creates monthly partitions for the current month + 3, each with local unique indexes on `(ledger_id, ledger_sequence)` and `(stream_id, stream_version)`. Seeds the `topup_clearing` and `settlement` system ledgers for `IDR`. Mounted in compose. README notes that `make reset` is required. **Records the §5.1 deferred-constraint-trigger check result in the PR description**, and adds the trigger if PG 17 supports it. | Integration: UPDATE, DELETE and TRUNCATE on `events` and `journal_entries` raise errors; `ledgers_no_negative` rejects a below-floor balance; a second account for the same user fails; `transactions_single_void_key` rejects a second active void |
| 1.2 | `feat/ledger-domain` (`main`, parallel to 1.1) | `internal/ledger`: `Money` with checked arithmetic, transaction and account types, event types and payloads (§4.5), `Apply` and `Fold` with `ErrIllegalTransition` (§4.4). No I/O. | Table-driven tests for every legal and illegal transition; overflow; a `testing/quick` property test showing the fold equals the projection (§13.1) |
| 1.3 | `feat/ledger-repositories` (stacked on 1.1; rebased on `main` once 1.2 merges) | `internal/repository/ledger_*.go`: accounts, ledgers, transactions, journal (`Post` writes both lines in one statement), events, idempotency keys and outbox insert + `NOTIFY`. All take `pgx.Tx`. `TxRunner` in `internal/service`. `internal/outbox` envelope encoding (§7.3). Repositories don't log. | Integration test per repository, including lock queries (`FOR UPDATE`, `FOR SHARE`) and the `ledger_sequence` assignment |
| 1.4 | `feat/ledger-service-credit-debit` (stacked on 1.3) | `service.LedgerService`: `OpenAccount`, `OpenLedger`, `Credit`, `Debit`, and `Process` (Tx2) with lock order `transactions → accounts → ledgers`. Decision rules for frozen, insufficient funds and missing ledger. The account `status` check is included now, so P2 only adds the freeze endpoints. PG idempotency per §6.4: replay, 409 and 422. The `LEDGER_PROCESS_TIMEOUT` → 202 path. Catalog rows: `ledger.account_open`, `ledger.credit`, `ledger.debit`, `ledger.idempotency`. | Fakes-based service tests for every §6.4/§6.5 row in scope, with log assertions; integration double-spend test (§13.3); PG-only idempotency race test |
| 1.5 | `feat/ledger-api-internal` (stacked on 1.4) | `cmd/ledger-api` (config, retry, graceful shutdown, `healthcheck`); `internal/config` ledger vars; `api.ClientAuth` with `LEDGER_CLIENTS`; `NewLedgerRouter`; internal endpoints for accounts, ledgers, credits, debits and GETs (§6.2); compose `ledger-api` on `:8081`; `ledger.json` dashboard skeleton with panels for the P1 error events; `scripts/smoke-ledger.sh`. Catalog row `ledger.client_auth`. | HTTP tests: 401 and 403 with logs, validation (§6.3), `DisallowUnknownFields`, envelope with `transaction_id`, the 202 path |
| 1.6 | `feat/ledger-user-reads` (stacked on 1.5) | `/api/v1/ledger/account`, `/transactions` (keyset on `(created_at, id)`, `limit ≤ 100`), and `/transactions/{id}` (404 if not the caller's), using the existing `Auth` middleware and Redis sessions. | Another user's transaction returns 404; pagination stability; balance is read-your-writes |

**Acceptance (issue checklist).**
- [ ] TRD §14 P1 exit: §13.1 and §13.2 in scope pass, plus the double-spend and append-only integration tests.
- [ ] `scripts/smoke-ledger.sh` covers: open account, credit, debit, `insufficient_funds`, idempotent replay, mismatched-key 422, and user balance read.
- [ ] Every transition writes an `events` row and an `outbox` row carrying the request's `trace_id`.
- [ ] The §5.1 deferred-trigger finding is recorded in the TRD.

---

### P2: Void, freeze, idempotency cache and recovery

**Goal.**
- `VOID` (FR-04) and `FREEZE`/`UNFREEZE` (FR-09).
- The Redis idempotency fast path (FR-06).
- The `ledger-worker` binary, with the sweeper and the idempotency purge.

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 2.1 | `feat/ledger-idem-cache` (`main`) | `internal/idempotency` fingerprint (canonical JSON) and Redis cache: a fast-path replay, backfill on PG conflict, a best-effort set after commit, and PG-only fallback when Redis is down with a `dependency` warn. | Fingerprint stability; miniredis replay; Redis down → still correct (§13.2) |
| 2.2 | `feat/ledger-void` (`main`) | `POST /internal/v1/transactions/{id}/void`: lock order including the original, negated lines (§4.3), `TransactionVoided` on the original's stream, the original projection set to `VOIDED`, `not_voidable`, and the Q3 floor rule. Catalog row `ledger.void`. | Void of credit and void of debit; void of a void → `not_voidable`; integration void race (§13.3) |
| 2.3 | `feat/ledger-freeze` (stacked on 2.2) | Freeze and unfreeze endpoints and transaction (§7.2), idempotent when already in the target state; `unfreeze` limited to `ops`. Catalog rows `ledger.freeze` and `ledger.unfreeze`. | Integration freeze race (§13.3): debits blocked after the freeze commits while credits settle; a void on a frozen account is allowed (Q3) |
| 2.4 | `feat/ledger-worker-sweeper` (`main`) | `cmd/ledger-worker`: a job runner (one `pg_advisory_lock` leader per job, interval, graceful stop, `healthcheck`) running the sweeper (§7.5) and the idempotency purge. Compose service. Catalog row `ledger.recover`, plus a dashboard panel. | Integration: Tx2 killed after Tx1 → terminal state within `LEDGER_SWEEP_AFTER + interval`; `processing_timeout` after max age; two workers → one leader |

**Acceptance.**
- [ ] TRD §14 P2 exit: idempotency, void and freeze race tests pass.
- [ ] NFR-7 locally: no PENDING older than 60 s after injected Tx2 failures.
- [ ] Q3 and Q6 answered or the defaults confirmed in the issue.

---

### P3: Event streaming (outbox relay to Kafka, dashboards, alerts)

**Goal.** Every state transition reaches Kafka effectively-once and in order per stream (FR-07). The ledger dashboard and alerting are complete for everything built so far.

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 3.1 | `feat/kafka-infra` (`main`) | Compose `kafka` (`apache/kafka:4.1.0`, KRaft) and `kafka-init` (`ledger.events.v1`, 12 partitions); `internal/kafka` franz-go producer and consumer wrappers; `dependency.connect` with `dependency=kafka`; `make kafka-topics`. | Integration produce/consume round trip; startup retry logs |
| 3.2 | `feat/ledger-outbox-relay` (stacked on 3.1; needs P2.4) | Relay job in `ledger-worker` (§7.3): `LISTEN`/poll, batch, `acks=all`, set `published_at`, and no cursor. Outbox purge. Catalog rows `outbox.publish` and `outbox.lag`. A minimal k6 script in `scripts/load/` for the lag check. | Integration relay crash (§13.3): duplicate published, deduplicated by `event_id`; per-ledger ordering under concurrent load; late-committing lower id is still published |
| 3.3 | `feat/ledger-dashboard-alerts` (stacked on 3.2) | The complete `ledger.json` (§11.2). `deploy/grafana/provisioning/alerting/ledger.yaml` with success ratio, outbox lag and stuck PENDING (§11.3). The reconcile, partition and fraud alerts land with their phases. | Grafana starts with the alert rules loaded; each alert fires against synthetic log lines |

**Acceptance.**
- [ ] TRD §14 P3 exit: the relay crash test passes; outbox lag < 1 s at 300 TPS (short k6 run).
- [ ] With Kafka stopped, writes continue and the outbox drains after recovery (an early version of the §13.5 chaos test).

---

### P4: Fraud engine

**Goal.** Velocity rules evaluated in real time from Kafka, freezing accounts within 2 s p99 (FR-08, FR-09).

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 4.1 | `feat/fraud-rules` (`main`) | `internal/fraud`: `topup_velocity` and `debit_volume` as pure functions over a `WindowStore` interface, plus a Redis ZSET implementation. Event time only (§7.4). Config vars. | Window edges; out-of-order and duplicate events; per-currency limits |
| 4.2 | `feat/fraud-engine` (stacked on 4.1) | `cmd/fraud-engine`: consumer group, `read_committed`, manual commits, `fraud:seen` dedupe, a freeze client (idempotency key `fraud:<rule>:<account>:<window_start>`, trace continued, retry up to `FRAUD_FREEZE_RETRY`, no offset commit on failure). Compose service plus a `LEDGER_CLIENTS` entry. Catalog row `fraud.decision`; dashboard panel; "fraud freeze failing" alert. | End-to-end: 4 top-ups in 5 minutes → frozen within 2 s p99, then a debit returns `account_frozen`; ledger-api down → retried, offset not committed; one `trace_id` across the three services |

**Acceptance.**
- [ ] TRD §14 P4 exit criterion passes.
- [ ] The Q7 thresholds are configurable, with production values tracked in the issue.

---

### P5: Integrity (reconciler, projection rebuild, partitions)

**Goal.** Continuous proof that projections equal the fold of the source of truth (FR-03, §7.6), a reviewed repair path, and partitions that never run out (§7.7).

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 5.1 | `feat/ledger-reconciler` (`main`) | Reconciler job: checks 1–4, a watermark for incremental runs, and a nightly full sweep. Catalog row `ledger.reconcile`; "reconcile failure" alert. | Random mixed workload → clean; injected projection drift (direct UPDATE on `ledgers`) → `balance_mismatch`; sequence gap → `sequence_gap` |
| 5.2 | `feat/ledger-rebuild-projections` (`main`) | `ledger-worker rebuild-projections`: dry-run diff by default, `--apply` behind an explicit flag, with logging. | Rebuild reproduces projections exactly (§13.3) |
| 5.3 | `feat/ledger-partitions` (`main`) | Partition maintenance job keeping the current month + 3 ahead, with local unique indexes. Catalog row `ledger.partition`; "partitions running out" alert. | Injected clock creates future partitions; fewer than 2 ahead → failure event |
| 5.4 | `feat/migration-runner` (`main`) | Q11: golang-migrate as a one-shot compose service, replacing the init-script mounts; existing volumes are baselined automatically. Lands first, so 5.1's migration 005 applies without `make reset`. | Empty database → 001–004 applied; a volume at 001 → baselined at 1, then 002–004 applied with data kept; a volume at 004 → no change; `down -all` then `up` round trip |

**Acceptance.**
- [ ] Reconciler checks 1–4 run every `RECONCILE_INTERVAL` and alert on any mismatch.
- [x] Q11 (migration runner) is decided and recorded in the TRD.

---

### P6: Retention, load and resilience

**Goal.** The 7-year archival (NFR-9), and proof of the throughput, latency, success-ratio and resilience targets (NFR-1, 2, 5, 6, 8).

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 6.1 | `feat/ledger-archiver` (`main`) | Archiver job (§7.8): checkpoints, Parquet export, manifest, read-back verify, detach and drop, legal hold. MinIO in compose with an Object Lock bucket for local drills. Catalog row `ledger.archive`. | Archive drill on a backdated partition: export verified, partition dropped, reconciler still clean through the checkpoints; a legal-hold account → skipped |
| 6.2 | `test/ledger-load` (`main`) | k6 scenarios from §13.4 (300 TPS mixed for 30 minutes, 10 hot accounts, a 600 TPS spike), `make load`, and a results write-up in `spec/`. | NFR-1, 2, 5 and 8 met; reconciler clean afterwards |
| 6.3 | `docs/ledger-chaos-runbook` (`main`) | Runbook for §13.5 with the recorded results of each fault. | Each fault behaves as specified |

**Acceptance.**
- [ ] TRD §14 P5 exit: §13.4 passes, and the archive drill completes.
- [ ] The §13.5 chaos runbook has been executed and its results recorded.
- [ ] Q8 answered before 6.1 starts.

---

## 5. Before P0

1. **Land the specs.** The BRD, TRD and this plan are uncommitted on `feat/journaling-balance`. Merge them first, through that branch or a `docs/ledger-spec` PR, so issues can link to them on `main`.
2. **Allow stacked PRs in the git rule.** Add to `.claude/rules/git-workflow.md`: "A stacked PR may branch from the PR below it. The bottom of every stack branches from a freshly pulled default branch."
3. **Create the issues.** Create the seven issues (and the optional tracker) from §4 with `gh issue create`, with labels `ledger` and `phase:Pn`.
