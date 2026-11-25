# Money-Out (Bank Transfer) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

| | |
| :--- | :--- |
| **Status** | Draft for review (M0). Q1 answered: **H1 approved**. Execution: **native**, with a manual gate after every phase. |
| **Date** | 2026-11-25 |
| **Spec** | [TRD: Money-Out](<2026-11-25 TRD - Money-Out Bank Transfer.md>) (cited `T§n`), the brief [`PROMPT-money-out`](<../2026-11-25 PROMPT-money-out.md>) (`P§n`), the bank contracts [`bank-api-stubs`](<../2026-11-25 bank-api-stubs.md>) |
| **Repo** | `github.com/dydanz/fintech-ledger-playground` |

**Goal:** send money from a ledger balance to any Indonesian bank account through four bank adapters, without changing the core engine.

**Architecture:** four layers, each in this repo:
- the core's new outbound port, `payout-adapter`;
- one router and journal, `money-out-middleware` (MOM), with its own database and Kafka journal;
- one stateless adapter per bank, `bank-middleware-a…d`;
- test-only bank simulators.

They talk over Kafka, except MOM → bank middleware, which is HTTP signed with Ed25519 (T§3, ADR-001/002).

**Tech Stack:** Go 1.26, Gin (`api.NewBaseEngine`), pgx v5, golang-migrate, franz-go, `crypto/ed25519`. The repo's `internal/outbox`, `internal/worker`, `internal/logger` and `internal/startup` are reused.

This plan follows the brief's phases (P§6) as M0–M6.
- **One phase = one GitHub issue.**
- **One issue = one or more PRs**, each small enough to review in one sitting, leaving `main` green.
- After each phase: all tests run, a summary, open questions, and **a stop for your go-ahead** (P§6).

---

## Global Constraints

- **No change to existing core code, schemas, routes, posting rules or tests.** The core gets only additive files: new binary, new package, new migrations that insert or create (T§4) (P§0 #2).
- **No new scope.** Improvements go to your questions list (T§12.3), never into code (P§0 #3).
- **Money is `int64` minor units inside, decimal strings on the wire.** IDR has exponent 0 (L§4.1 A2). No `float64` anywhere near money (P§0 #6, P§8).
- **Never send a transfer to a second bank unless the first attempt has clearly `FAILED` with `retry_elsewhere = true`.** `UNKNOWN` resolves only by status inquiry (P§0 #5, P§3.2).
- **No bank-specific field, code or concept in the core, the adapter or MOM code.** Only in `internal/bankmw/stub*`, the simulators and `config/money-out/routing.yaml` (P§8).
- **The repo's conventions apply:**
  - layering: handlers → service (logging) → repository (no logging);
  - the error envelope;
  - the `event`/`outcome`/`reason` logging schema, with new events added to the catalog in the same PR;
  - one Dockerfile `ARG CMD`;
  - `//go:build integration` tests;
  - branch plus PR;
  - a `## Test evidence` section in every PR.
- **Never log:** full account numbers (last 4 digits only), tokens, signatures, nonces, keys, the AAA nonce or the execution token (T§9).
- **Bulk and batch endpoints are out of scope:** S:B bulk, S:D bulk and OTP approval (P§1.3).

## Review Focus

Failure modes the brief implies but its 16 scenarios (P§5) don't test, most likely first. Each has a test in the task named.
1. **A late bank event for an attempt that's no longer current.** For example, a `SUCCEEDED` for attempt 1 after MOM failed over to attempt 2. It must not finish the transfer silently, and it must never let both attempts look like the outcome. Expected: recorded in the journal, the transfer goes to `NEEDS_MANUAL_REVIEW` (a possible double payment), and the event fires an alert. Test: `TestLateEventForSupersededAttemptParksForReview` (M4.2).
2. **Malformed money on the wire:** `"1e6"`, `"-5"`, `"2500000.5"` for IDR, `"2500000.00000"`, an amount above `LEDGER_MAX_AMOUNT`, or a currency other than IDR. Expected: rejected before anything is stored, with `invalid_input`. Test: `TestParseMoneyRejects` (M1.1).
3. **A result redelivered to the adapter after the void has already succeeded** (Kafka redelivery, or a restart before the offset commit). Expected: the same void key replays, with no second void, and the same ack is re-sent. Test: `TestRedeliveredFailedResultVoidsOnce` (M5.2).
4. **A routing config reload that fails validation** (bad YAML, an unknown rule, a negative limit). Expected: the previous chain stays live, and the failure is logged once. Test: `TestInvalidReloadKeepsPreviousChain` (M4.1).
5. **Requests at the edge of the signature window:** exactly ±5 min, the nonce cache full, or a nonce reused just after the window. Expected: inside the window accepted, outside rejected. A cache past its size cap never evicts a nonce still inside the window: it grows past the soft cap and logs once. Test: `TestVerifierWindowEdges` (M1.2).

---

## 1. Phase overview

| Phase | Issue title | PRs | Depends on | Size | Exit criteria |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **M0** | `[Money-out M0] Discovery and plan` | this docs PR | — | S | You approve the TRD and this plan, and answer Q1–Q10 (T§12.2) |
| **M1** | `[Money-out M1] Contracts, MOM core, state machine, idempotency, outbox` | 3 (stack) | M0 | L | MOM accepts commands, rejects conflicts, journals every transition through its outbox, and exposes `GET /transfers/{id}` |
| **M2** | `[Money-out M2] Simulators and the sync bank middlewares (A, B)` | 3 | M1.1, M1.2 | L | A and B pass their mapping tables against their simulators, including fault injection |
| **M3** | `[Money-out M3] Bank middlewares D and C (async)` | 2 | M2.1 | L | D settles asynchronously. C's handshake survives a dropped webhook and a restart without double-executing. |
| **M4** | `[Money-out M4] Routing, retry, failover, breakers, inquiry` | 3 | M1, M2 | L | P§5 scenarios 4–7, 11, 12 and 15 pass |
| **M5** | `[Money-out M5] Core integration: payout-adapter` | 2 | M1 | M | Scenario 13: exactly one terminal result per transfer, and the debit is finalized or voided |
| **M6** | `[Money-out M6] Reconciliation and the full e2e suite` | 2 | M4, M5 | M | All 16 scenarios pass in CI, with ARCHITECTURE, RUNBOOK, BMW READMEs, OpenAPI and AsyncAPI |

```
M0 ─▶ M1.1 contracts ─▶ M1.2 signing ─▶ M2.1 simulators+bmw runtime ─▶ M2.2 bmw-a ─▶ M2.3 bmw-b
        │                                         └──▶ M3.1 bmw-d ─▶ M3.2 bmw-c
        └─▶ M1.3 MOM core ─▶ M4.1 routing ─▶ M4.2 dispatch/retry/inquiry ─▶ M4.3 breakers
                     └─▶ M5.1 adapter debit+command ─▶ M5.2 results+acks ─▶ M6.1 recon ─▶ M6.2 e2e+docs
```

## 2. Working conventions

- **Branches:** `feat/money-out-m<phase>-<desc>`, from a fresh `main`.
- **Integration tests:** they run against isolated compose projects only, never the default stack's Postgres.
- **Definition of done for every PR:**
  - `make vet test` passes;
  - `make test-integration` passes;
  - from M2 on, `make test-money-out-e2e` passes for the scenarios that phase covers;
  - new events are in the catalog, with panels for error-level ones;
  - no existing test was changed;
  - `## Test evidence` uses real output.
- **Proof that the core is unchanged:** every PR shows `git diff --stat main -- internal/ledger internal/service internal/repository internal/api migrations/00[1-7]*` and that it's empty.

## 3. Questions that gate work

| # | Gates | Question (T§12.2) |
| :--- | :--- | :--- |
| Q1 | M5 (and the T§2.3 mapping) | **Answered: H1** (debit to `payout_clearing`, void on failure) |
| Q3, Q4 | M4.2 | Exit rules for `NEEDS_MANUAL_REVIEW` |
| Q5 | M4.1 | Rail preference order |
| Q7 | M2, M3 | The assumed bank codes in T§11 |
| Q8 | M3.2 | S:C webhook authenticity |
| Q10 | M5.1 | `POST /internal/v1/payouts` as the request entry point |

---

## 4. Phases

### M1: Contracts, MOM core, state machine, idempotency, outbox

Task-level, because none of it depends on Q1.

#### Task M1.1: Canonical contracts and money

**Files:**
- Create: `internal/moneyout/contract/money.go`, `status.go`, `envelope.go`, `events.go`
- Test: `internal/moneyout/contract/*_test.go`

**Interfaces:**
- Produces:
  - `ParseMoney(value, currency string) (ledger.Money, error)` and `FormatMoney(ledger.Money) string`: two decimals, using the currency's exponent;
  - `type Status string`, constants `Accepted`, `Processing`, `Succeeded`, `Failed`, `Unknown`;
  - `type Reason string`, the P§2.4 set, and `DefaultRetryElsewhere(Reason) bool`;
  - `type BankResult struct{Status; Reason; RetryElsewhere bool; ReachedBank bool; BankReference, BankRawCode string}`;
  - `type Envelope struct` (fields as T§1.3, `StreamType` `"transfer"`), with `NewEvent(eventType, streamID string, version int64, payload any) Envelope`;
  - the payload types `TransferRequested`, `TransferSucceeded`, `TransferFailed`, `TransferAcknowledged`, `BankStatusReported`, `TransferRequest` (MOM → BMW, P§2.3).

- [ ] **Step 1: Write the failing tests.**
  - `TestParseMoneyAccepts`: `("2500000.00","IDR") → 2500000`; `("2500000","IDR") → 2500000`.
  - `TestParseMoneyRejects` (Review Focus 2): `"1e6"`, `"-5"`, `"0"`, `"2500000.5"`, `"2500000.001"`, `" 1"`, `"1,000"`, an amount above `1_000_000_000_000`, and currency `"USD"` when not configured. Each returns an error.
  - `TestFormatMoney`: `2500000 IDR → "2500000.00"`.
  - `TestDefaultRetryElsewhere`: the table from T§3.6.
  - `TestEnvelopeRoundTrip`: JSON field names exactly as T§1.3.
- [ ] **Step 2:** `go test ./internal/moneyout/contract/` fails (undefined).
- [ ] **Step 3: Implement.** `ParseMoney` uses a strict regexp `^[1-9][0-9]*(\.[0-9]{1,2})?$` and integer arithmetic only, then checks the core's `ledger.ValidateAmount`.
- [ ] **Step 4:** the tests pass.
- [ ] **Step 5: Commit.** `feat(money-out): canonical contracts and money parsing`

#### Task M1.2: Request signing and verification

**Files:**
- Create: `internal/moneyout/signing/signer.go`, `verifier.go`, `noncecache.go`
- Test: `internal/moneyout/signing/*_test.go`

**Interfaces:**
- Consumes: `contract.TransferRequest`.
- Produces:
  - `NewSigner(kid string, key ed25519.PrivateKey) *Signer`, with `(*Signer).Sign(method, path string, body []byte, now time.Time) (http.Header, error)`, which sets `X-MOM-Timestamp`, `X-MOM-Nonce` and `X-MOM-Signature`;
  - `NewVerifier(keys map[string]ed25519.PublicKey, skew time.Duration, cacheSize int) *Verifier`, with `(*Verifier).Verify(r *http.Request, body []byte, now time.Time) error`, returning `ErrBadSignature`, `ErrStale` or `ErrReplay`;
  - signed bytes exactly as T§3.5.

- [ ] **Step 1: Write the failing tests.**
  - `TestSignVerifyRoundTrip`.
  - `TestTamperedBodyRejected` (P§5 #8): one byte changed gives `ErrBadSignature`.
  - `TestReplayRejected`: the same headers twice give `ErrReplay` the second time.
  - `TestOtherKeyRejected`: signed with a key whose `kid` is unknown, or known with a different key, gives `ErrBadSignature`.
  - `TestVerifierWindowEdges` (Review Focus 5): ±5 min accepted; ±5 min 1 s gives `ErrStale`; with `cacheSize=2`, three nonces inside the window leave all three rejected on replay. So the cache holds every nonce for the window and grows past `cacheSize` rather than evict one still inside it. `cacheSize` is a soft cap: it logs once when exceeded.
  - `TestKeyRotation`: two `kid`s are accepted at once.
- [ ] **Step 2:** the tests fail.
- [ ] **Step 3: Implement.** The JWS detached compact form is `header..signature`. The nonce cache is a map plus a min-heap on expiry, swept on insert.
- [ ] **Step 4:** the tests pass, with `-race`.
- [ ] **Step 5: Commit.** `feat(money-out): Ed25519 request signing with replay protection`

#### Task M1.3: MOM skeleton: database, state machine, idempotency gate, journal outbox

This task carries three sub-commits, each with its own test cycle: state machine, store, service and API.

**Files:**
- Create:
  - `internal/moneyout/transfer/state.go` (states, transition table, `Apply`);
  - `migrations/money_out/001_init.up.sql` and `.down.sql` (the T§5 tables, CHECKs, the partial unique index on open attempts);
  - `internal/moneyout/repository/store.go`;
  - `internal/moneyout/service/intake.go`;
  - `internal/moneyout/api/handlers.go`;
  - `cmd/money-out-middleware/main.go`.
- Modify:
  - `docker-compose.yml`: add database `money_out` (init script), services `migrate-money-out` and `money-out-middleware`;
  - `Makefile`: `migrate-money-out`;
  - `internal/config/config.go`: a new `LoadMoneyOut()` (additive);
  - `.env.example`;
  - `.claude/rules/observability-logging.md`: the catalog rows `transfer.receive` and `transfer.state_change`.
- Create (docs): `spec/money-out/adr/001-transport.md`, `002-signing.md`, `003-outbox-consistency.md`, `004-holds.md` (T§10; 004 records your answer to Q1).
- Test: `internal/moneyout/transfer/state_test.go`, `internal/moneyout/repository/store_test.go` (integration), `internal/moneyout/service/intake_test.go`, `internal/moneyout/api/handlers_test.go`.

**Interfaces:**
- Consumes: `contract.Envelope`, `contract.TransferRequested`; `internal/outbox` (relay, notifier); `internal/worker.Runner`.
- Produces:
  - `type State string` with the ten states from T§6.1, `Apply(from State, to State) error`, and `ErrIllegalTransition`;
  - `Store.InTx(ctx, func(Tx) error) error`;
  - on `Tx`: `InsertTransfer(Transfer) (created bool, err error)`, `GetTransfer(id) (Transfer, error)`, `CompareAndSetState(id string, version int64, to State, journal ...contract.Envelope) error` (returns `ErrVersionConflict`), `InsertAttempt(Attempt) error`, `MarkProcessed(eventID, topic string) (first bool, err error)`;
  - `Intake.Receive(ctx, contract.Envelope) (Transfer, Outcome, error)`, with `Outcome` one of `Created`, `Duplicate`, `Conflict`;
  - `GET /transfers/{transfer_id}` (P§3.6);
  - consumer group `money-out-middleware` on `money-out.commands.v1`.

- [ ] **Step 1: State machine tests.**
  - `TestLegalTransitions`: every arrow in P§3.4 passes `Apply`.
  - `TestIllegalTransitionsRejected`: every other `(from, to)` pair of the 10 × 10 table returns `ErrIllegalTransition`.
  - `TestTerminalStatesHaveNoExit`: `SUCCEEDED` and `FAILED` have no exit. `NEEDS_MANUAL_REVIEW → SUCCEEDED | FAILED` is pending Q3/Q4, so the test lists it explicitly.
- [ ] **Step 2:** fails, then implement `state.go`, then passes. Commit: `feat(money-out): transfer state machine`.
- [ ] **Step 3: Store integration tests** (`//go:build integration`, against `TEST_MONEY_OUT_DATABASE_URL`).
  - `TestInsertTransferIsUnique`: the second insert returns `created=false`.
  - `TestCompareAndSetRejectsStaleVersion`.
  - `TestOneOpenAttemptPerTransfer`: a second open attempt violates the index.
  - `TestStateChangeAndJournalCommitTogether`: a forced error after the outbox insert leaves neither the state nor the outbox row.
  - `TestMarkProcessedOnce`.
- [ ] **Step 4:** fails, then implement the migration and `store.go`, then passes. Commit: `feat(money-out): MOM schema, store and outbox`.
- [ ] **Step 5: Intake and API tests.**
  - `TestDuplicateCommandSamePayload`: `Duplicate`, no new row, no new journal event.
  - `TestSameIDDifferentAmountIsConflict` (P§5 #3): `Conflict`, journal event `TransferRejected`, state unchanged.
  - `TestReceiveJournalsReceived`: outbox holds `TransferReceived` with `stream_version 1`, keyed by `transfer_id`.
  - `TestConsumerDedupesOnEventID`: the same envelope twice gives one row.
  - `TestGetTransfer`: 200 with state and attempts; unknown gives 404 `transfer_not_found`.
  - Log assertions on `transfer.receive` (`outcome` `success` / `denied` `conflict`).
- [ ] **Step 6:** fails, then implement intake, the consumer (manual offset commit after the transaction), the handlers, and main with startup retry, the `healthcheck` subcommand, `/healthz` and `/readyz`, and the outbox relay job. Then passes.
- [ ] **Step 7: Verify.** `make vet test`; `make test-integration` on an isolated compose project; `docker compose up money-out-middleware` reaches healthy. Commit: `feat(money-out): MOM intake, idempotency gate and transfer API`.

### M2: Simulators and the sync bank middlewares

| PR | Contents | Tests |
| :--- | :--- | :--- |
| **2.1** `feat/money-out-m2-simulators` | `internal/banksim/stuba…stubd` and `cmd/bank-simulator`, implementing S:A–S:D exactly, including the (A) codes from T§11. **Fault injection** per request, through a control route `POST /_sim/faults {match, action: delay|timeout|status|pending_then_success|pending_then_fail|drop_webhook, times}`. `internal/bankmw`: the BMW runtime (signed-request server from M1.2, the `Bank` interface `Transfer(ctx, contract.TransferRequest) contract.BankResult` and `InquireStatus(ctx, transferID, requestID) contract.BankResult`, the Kafka emitter, masking helpers). Compose profile `money-out`. | Each simulator's signature checks reject a wrong signature. Fault actions behave as named. The runtime rejects tampered, replayed and foreign-key requests (P§5 #8). |
| **2.2** `feat/money-out-m2-bmw-a` | `internal/bankmw/stuba`: SNAP RSA-2048 B2B token (cached until `expiresIn`), HMAC-SHA512 `X-SIGNATURE` over the SNAP string-to-sign, inquiry → transfer → status. `cmd/bank-middleware-a`, README with the T§11.1 table. | One test per row of T§11.1 against the simulator; token refresh on `4011800`; the masked account number in logs |
| **2.3** `feat/money-out-m2-bmw-b` | `internal/bankmw/stubb`: OAuth Basic → bearer, HMAC-SHA256 `X-BCA-*` (T§12.1 #2), beneficiary inquiry → transfer → status. `cmd/bank-middleware-b`, README with the T§11.2 table. | One test per row of T§11.2 |

### M3: Async bank middlewares

| PR | Contents | Tests |
| :--- | :--- | :--- |
| **3.1** `feat/money-out-m3-bmw-d` | `internal/bankmw/stubd`: kebab-case envelope, HMAC-SHA512 over the minified body (T§12.1 #3), acct-validate → funding-check → remit-single. Then `ACCEPTED`, and a poll of remit-status (T§12.1 #4) emitting `PROCESSING` / terminal events. | T§11.4 rows; `IN_CLEARING → SETTLED` produces events in order; a lost event is recovered by `InquireStatus` |
| **3.2** `feat/money-out-m3-bmw-c` | `internal/bankmw/stubc`: initiate (`ACCEPTED`), webhook routes `/callbacks/bank-c/ack-1` and `/ack-2` (session matching, T§11.3), the AAA hash and HMAC exactly as S:C, execute with the issued token, the status-poll fallback. Sessions in memory with deadlines. | AAA hash test vector built from the contract's example inputs. P§5 #9: a dropped ACK-2 recovers by poll. P§5 #10: a restart mid-handshake gives poll or `UNKNOWN`, and the simulator sees `execute` at most once. Unknown or expired session webhooks get `404` and a log line. |

### M4: Routing, retry, failover, breakers, inquiry

| PR | Contents | Tests |
| :--- | :--- | :--- |
| **4.1** `feat/money-out-m4-routing` | `internal/moneyout/routing`: the `Rule` interface and chain, the six rules from T§6.4, a YAML loader with file-watch reload, and the decision stored on the transfer. `config/money-out/routing.yaml` holds the T§11.4 capabilities and the P§3.1 limits. | Per-rule tables. P§5 #11: 300,000,000 goes to SKNBI/RTGS, never BIFAST. `TestInvalidReloadKeepsPreviousChain` (Review Focus 4). |
| **4.2** `feat/money-out-m4-dispatch` | Dispatcher job (compare-and-set claim, call after commit), same-node retry (only `reached_bank=false`), `UNKNOWN` → inquiry job, `ATTEMPT_FAILED` → next candidate, `FAILED` when candidates run out, `NEEDS_MANUAL_REVIEW` at the inquiry deadline. Consumer for `money-out.bank-events.v1`. `TransferSucceeded`/`TransferFailed` go to the outbox in the same transaction. | P§5 #2, #4, #5, #6, #7, #12. `TestLateEventForSupersededAttemptParksForReview` (Review Focus 1). `TestUnknownNeverFailsOver`. |
| **4.3** `feat/money-out-m4-breakers` | `internal/moneyout/breaker` on the `breakers` table (compare-and-set), the counting rules from T§6.3, the routing rule `breaker_open`, half-open probes, `BreakerChanged` journal events, and `GET /bank-middlewares/health` | P§5 #15. `TestBusinessFailuresDontCount` (`INVALID_ACCOUNT`). `TestOpenBreakerDoesNotStopInquiries`. |

### M5: Core integration (H1)

| PR | Contents | Tests |
| :--- | :--- | :--- |
| **5.1** `feat/money-out-m5-adapter` | `migrations/008_payout_clearing.up.sql` (the SYSTEM ledger row, IDR) and `009_payout_adapter.up.sql` (schema `payout`: `payouts`, `outbox`, `processed_events`). `internal/payout` and `cmd/payout-adapter`: `POST /internal/v1/payouts` → debit through ledger-api (`Idempotency-Key: payout:<transfer_id>`, `reference = transfer_id`) → on `SETTLED`, `TransferRequested` to the outbox. On a 422 refusal, no command. On 202/5xx, retry with the same key. A `payout` client in `.env.example` `LEDGER_CLIENTS`. | Insufficient funds gives no command. A retry after a 503 makes one debit. The existing ledger test suite passes, unchanged. |
| **5.2** `feat/money-out-m5-results` | Consumer of `money-out.results.v1`: `SUCCEEDED` finalizes (no entry, T§2.3); `FAILED` voids (`payout-release:<transfer_id>`); `TransferAcknowledged` goes to the outbox. MOM consumes `money-out.core-acks.v1` and sets `core_ack=done`. | P§5 #13. `TestRedeliveredFailedResultVoidsOnce` (Review Focus 3). `payout_clearing`'s balance is 0 after a mix of successes and failures settles. |

### M6: Reconciliation, the full e2e suite and docs

| PR | Contents | Tests |
| :--- | :--- | :--- |
| **6.1** `feat/money-out-m6-recon` | The `recon` job and CLI (T§8), `recon_runs`, `recon_exceptions`, `GET /reconciliation/{date}`, the `StatementSource` interface (TODO, no implementation) | P§5 #16, both directions. Re-running a date is idempotent. It never changes a terminal state. |
| **6.2** `feat/money-out-m6-e2e` | `make test-money-out-e2e`: a Go e2e suite against an isolated compose project (Kafka, both databases, MOM, adapter, 4 BMWs, 4 simulators), with all 16 P§5 scenarios, including #14 (MOM killed between commit and publish). Docs: `ARCHITECTURE.md` (Mermaid sequence diagrams for a sync bank, S:C, timeout-then-inquiry, failover, outbox relay and recon; the state machine; routing and breakers; how to add a bank), `RUNBOOK.md`, `openapi/*.yaml`, `asyncapi.yaml`, the BMW READMEs (each started in its own M2/M3 PR, finished here). CI job. | All 16 scenarios green in CI. The definition of done in P§8 is checked line by line in the PR. |

---

## 5. Risks

| Risk | Mitigation |
| :--- | :--- |
| H1's `SUCCEEDED` has no core entry of its own | Reconciliation check 2 (T§8) matches each terminal transfer to its debit or void through `core_ack`, so nothing is unaccounted for |
| The (A) bank codes are wrong for a real bank | They live only in each BMW's mapping table and its simulator, and the README lists them. Changing them touches one package. |
| S:C's unauthenticated webhooks (T§12.1 #5) | Session and stage matching, short deadlines, a logged reject. Q8 decides more. |
| A late `SUCCEEDED` after failover (a double payout) | Review Focus 1: parked for review and alerted, never auto-resolved |
| The e2e suite is slow or flaky (Kafka, async webhooks) | Waits poll on state with deadlines, never fixed sleeps. Isolated compose project per run. |
