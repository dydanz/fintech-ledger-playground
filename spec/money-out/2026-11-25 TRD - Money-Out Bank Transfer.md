# TRD: Money-Out (Bank Transfer)

| | |
| :--- | :--- |
| **Status** | **Phase 0, draft for review.** Approved 2026-11-25 (#71). Q1: **H1** (§2.2). Q2–Q10: **the recommendations in §12.2 accepted**. |
| **Date** | 2026-11-25 |
| **Brief** | [`spec/2026-11-25 PROMPT-money-out.md`](<../2026-11-25 PROMPT-money-out.md>), cited as `P§n` |
| **Bank contracts** | [`spec/2026-11-25 bank-api-stubs.md`](<../2026-11-25 bank-api-stubs.md>), cited as `S:A`, `S:B`, `S:C` and `S:D` |
| **Core design** | [TRD: Immutable Ledger](<../2026-11-06 TRD - Immutable Ledger & Event-Sourced Balance System.md>), cited as `L§n` |

This document is the Phase 0 report the brief asks for (P§6). It covers:
- what the core engine is today;
- how money-out fits it without changing it;
- the contracts, state, storage, topics and per-bank mappings;
- every gap and question that needs your answer before work starts.

Decisions are marked **Recommended** when they're mine and still need your approval, and **Fixed** when the brief or the repo already decides them.

---

## 1. The core engine today (discovery)

### 1.1 Stack and layout

| Area | What exists | Money-out follows it |
| :--- | :--- | :--- |
| Language | Go 1.26, one module `fintech-ledger-playground` | **Fixed:** same module, no new language |
| HTTP | Gin, with the shared base engine (`api.NewBaseEngine`): `RequestContext`, `AccessLog`, `Recovery`, `BodyLimit`, the JSON error envelope `{"error":{"code","message","request_id"}}` | Every new service uses `api.NewBaseEngine` |
| Postgres | pgx v5, golang-migrate, numbered `migrations/NNN_*.up/down.sql`, guard triggers, CHECK constraints | MOM gets its own database and migration folder (§5) |
| Kafka | franz-go, idempotent producer, `acks=all`; consumers commit offsets manually after processing | Same client and settings |
| Outbox | `outbox` table, relay is the `worker.Runner` leader (Postgres advisory lock), `LISTEN`/`NOTIFY` wake, at-least-once (L§7.3) | MOM reuses the same pattern and the same packages where possible (`internal/outbox`, `internal/worker`) |
| Config | `internal/config`, env vars with defaults | Same |
| Logging | `slog` JSON, the event schema and catalog in `.claude/rules/observability-logging.md` | Mandatory for every new event (§9) |
| Tests | Fakes, `go test -race`, `//go:build integration` against compose Postgres and Kafka; `kfake` for Kafka edge cases | Same, plus the e2e suite (P§5) |
| Image | One `Dockerfile` with `ARG CMD`, distroless, read-only rootfs | New binaries use it unchanged |
| Git and PRs | Branch plus PR per piece of work; a `## Test evidence` section in every PR | Same (Development Plan §2) |

### 1.2 How the core moves money

- **Amounts:** `int64` in the currency's minor unit, and IDR has exponent 0 (L§4.1, assumption A2). `"2500000.00"` in a bank API is `2500000` in the core. A non-zero fraction can't be represented and is rejected (§3.1).
- **Primitives** on the internal HTTP API (L§6.2), each authenticated by a client token whose allowed operations are set in `LEDGER_CLIENTS`:

  | Primitive | Effect on the journal (L§4.3) |
  | :--- | :--- |
  | `POST /internal/v1/accounts/{id}/debits` | User ledger `−amount`, counterparty system ledger `+amount`. Refused with `insufficient_funds` or `account_frozen`. Idempotent on `Idempotency-Key`. |
  | `POST /internal/v1/transactions/{id}/void` | A new `VOID` transaction that negates the original's two lines. The original becomes `VOIDED`. This is the core's only reversal, and it's a compensating entry: nothing is edited or deleted. |
  | `POST /internal/v1/accounts/{id}/credits` | User ledger `+amount`, counterparty `−amount` |

- **Counterparties** are SYSTEM ledgers looked up by code and currency (`tx.GetSystemLedger`). `topup_clearing` and `settlement` exist. A new code, for example `payout_clearing`, works without code changes once its row exists, because `migrations/002` only checks the code's format.
- **No holds.** L§1.2 lists "two-phase authorisation/capture holds" as out of scope: "funds being verified or locked" is met by row locking during `PROCESSING`. **The brief's reserve → finalize → release (P§2.1) has no primitive in the core.** See §2.2.

### 1.3 How the core journals

| Aspect | Core convention (L§4.4, L§4.5, L§7.3) |
| :--- | :--- |
| Source of truth | `ledger.events` (append-only, guarded by triggers) and `ledger.journal_entries`; the tables `transactions`, `ledgers` and `accounts` are projections |
| Streams | `transaction` (id = transaction id) and `account`. `(stream_id, stream_version)` is unique and strictly increasing. |
| Transaction states | `PENDING → PROCESSING → SETTLED | FAILED`, and `SETTLED → VOIDED`. Illegal transitions return `ErrIllegalTransition`. |
| Event names | PascalCase past tense: `TransactionRequested`, `TransactionSettled`, … |
| Envelope on Kafka | `event_id, event_type, schema_version, stream_type, stream_id, stream_version, account_id, ledger_id, occurred_at, trace_id, payload`. Headers: `traceparent`, `event_type`, `schema_version`. |
| Ordering and dedupe | Consumers order on `(stream_id, stream_version)` and deduplicate on `event_id`, never on Kafka offsets across streams |
| Corrections | Only by a new compensating transaction (`VOID`). Nothing is updated or deleted. |
| Reference fields | `transactions.reference` (≤ 64 characters, caller text), `metadata` (JSON ≤ 4 KiB), `requested_by` (client id), `trace_id`. The idempotency key is stored only as a SHA-256. |
| Timestamps | RFC 3339 UTC with milliseconds, from Postgres `now()` |

### 1.4 How things talk to the core

The core publishes **out** through Kafka (`ledger.events.v1`, keyed by `account_id`). Everything **in** is HTTP on `/internal/v1/*`. The fraud engine is the precedent for an outside service acting on the core: it consumes Kafka, calls `POST …/freeze` with its own client token and a deterministic `Idempotency-Key`, retries transient errors, and commits offsets only after the call (L§7.4).

---

## 2. Fitting money-out to the core

### 2.1 The core's outbound port (additive)

The `do_transfer` command (P§2.1) has no source account, and the core has no notion of a payout. So something must turn "pay this beneficiary from this account" into a debit plus a command. **Recommended:** a new binary in this repo, **`payout-adapter`**, which is the core's outbound port and inbound result consumer. It changes no existing code, schema, route or behaviour.

```
caller ──POST /internal/v1/payouts──▶ payout-adapter ──debit (existing API)──▶ ledger-api
                                          │  outbox → money-out.commands.v1 (do_transfer)
                                          ▼
                                  money-out-middleware ──▶ bank-middleware-<bank> ──▶ bank
                                          │  money-out.results.v1 (SUCCEEDED / FAILED)
                                          ▼
                                    payout-adapter ──void (existing API) on FAILED──▶ ledger-api
                                          │  money-out.core-acks.v1 (the core journal entry it maps to)
                                          ▼
                                  money-out-middleware records the acknowledgement (P§3.4a #4)
```

- **API:** `POST /internal/v1/payouts {transfer_id, account_id, amount, currency, beneficiary{bank_code, account_no}, metadata?}`. The caller generates `transfer_id`, which is the idempotency key, and authenticates with the same client-token scheme as ledger-api.
- **Its own table** (`payout.payouts`, in a new schema of the core database, additive migration) records the request and its stage. The adapter's own outbox emits the command only after the debit has settled (§2.3).
- **Client grant:** a `payout` client in `LEDGER_CLIENTS` with `debit, void, read`. That's configuration, not code.

### 2.2 Q1: how to "hold" without a hold primitive (answered: H1)

P§2.1 asks the core to reserve the amount, finalize it on `SUCCEEDED`, and release it on `FAILED`, using existing primitives, and to stop and ask if they don't exist. They don't. The options:

| | Option | Core change | On `do_transfer` | On `SUCCEEDED` | On `FAILED` |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **H1** (**Recommended**) | **Debit into a clearing ledger, void on failure** | None in code. One additive migration inserts the SYSTEM ledger `payout_clearing` (IDR). | `DEBIT` user `−amount`, `payout_clearing +amount`. The money leaves the spendable balance at once, which is the effect a hold has. | **No core entry.** The debit is already final. The adapter acknowledges with the debit's transaction id. | `VOID` of that debit, which restores the balance as a compensating entry |
| H2 | Add real holds to the core | New domain logic, schema and API in the core | `HOLD` | `CAPTURE` | `RELEASE` |
| H3 | Debit only after success | None | Nothing | `DEBIT` | Nothing |

- **H1 meets the safety goal.** The amount can't be spent twice while in flight, `payout_clearing`'s balance is exactly the money in flight, and the core's reconciler keeps checking the double entry. **It departs from P§3.4a #3 in one place:** `SUCCEEDED` has no journal entry of its own, because the debit already was the final debit. Every other row of the mapping holds (§2.3).
- **H2** gives the exact reserve/finalize/release mapping. But it changes the core, which P§0 #2 forbids and L§1.2 deliberately left out.
- **H3** lets the balance be spent while the bank is paying, so the core could pay out money it no longer has. It isn't safe.

**Answered 2026-11-25: H1 approved**, with `SUCCEEDED` mapped to "the debit is final, no new entry". The core stays unchanged.

### 2.3 MOM state ↔ core journal mapping (P§3.4a, H1)

| MOM state or event | Core journal entry (existing mechanism) | Trigger | Reference fields linking the two |
| :--- | :--- | :--- | :--- |
| `do_transfer` emitted | `DEBIT`, counterparty `payout_clearing`, `SETTLED` | The adapter, before writing the command to its outbox. Insufficient funds or a frozen account ends it here: no command is sent and the caller gets the core's refusal. | `Idempotency-Key: payout:<transfer_id>`; `reference = <transfer_id>`; `metadata.transfer_id`; core `transaction_id` stored in `payout.payouts` and in the command's `metadata.core_transaction_id` |
| `RECEIVED` … `PROCESSING`, `UNKNOWN`, attempts, `NEEDS_MANUAL_REVIEW` | **None** (P§3.4a #5) | — | Each MOM journal event carries `transfer_id` and `core_transaction_id` |
| `SUCCEEDED` | **None (H1):** the debit is final | The adapter consumes the result, marks the payout done, and acknowledges | ack carries `core_transaction_id` (the debit) |
| `FAILED` | `VOID` of the debit | The adapter consumes the result and voids | `Idempotency-Key: payout-release:<transfer_id>`; ack carries the void's `transaction_id` |
| Manual: succeeded, then the bank returns the money | `VOID` of the debit (or a `CREDIT` from `payout_clearing` if already voided elsewhere) | RUNBOOK, by ops through the adapter | new key `payout-correction:<transfer_id>:<n>` |
| Manual: failed and released, then the bank turns out to have paid | `DEBIT` from the user, counterparty `payout_clearing` | RUNBOOK | `payout-correction:<transfer_id>:<n>`. **Q2:** this can be refused with `insufficient_funds` if the user has since spent the money. How should that be handled? |

A transfer is **complete** only when MOM has recorded the adapter's acknowledgement (P§3.4a #4). Until then MOM shows it as `SUCCEEDED`/`FAILED` with `core_ack = pending`, and reconciliation (§8) flags any ack older than a threshold.

---

## 3. Contracts between layers

### 3.1 Money

- On the wire (commands, results, MOM → BMW): `{"value": "2500000.00", "currency": "IDR"}` (**Fixed**, P§2.1), a decimal string.
- Inside every service: `int64` minor units, using the core's `ledger.Money` type and its currency exponents. Parsing rejects floats, exponents, more decimals than the currency allows, and a non-zero fraction for IDR (exponent 0). A single `contract.ParseMoney` and `contract.FormatMoney` does this. No `float64` touches money anywhere (P§8).
- At the bank edge, each BMW renders what its contract wants: `"2500000.00"` for S:A and S:B, and the integer `2500000` for S:C (`remit_amt`) and S:D (`tx-amount`).

### 3.2 Core → MOM: `do_transfer` on `money-out.commands.v1`

The brief's command, inside the core's event envelope (§1.3), so both journals share conventions:

```json
{"event_id": "uuid", "event_type": "TransferRequested", "schema_version": 1,
 "stream_type": "transfer", "stream_id": "<transfer_id>", "stream_version": 1,
 "occurred_at": "2026-11-25T02:15:00.000Z", "trace_id": "…",
 "payload": {"command": "do_transfer", "transfer_id": "uuid",
             "amount": {"value": "2500000.00", "currency": "IDR"},
             "beneficiary": {"bank_code": "014", "account_no": "0201245681"},
             "requested_at": "2026-11-25T02:15:00.000Z",
             "metadata": {"core_transaction_id": "uuid", "remark": "…"}}}
```

It's keyed by `transfer_id`. `metadata` passes through untouched. MOM treats `core_transaction_id` as an opaque reference, the way `bank_reference` is opaque to it.

### 3.3 MOM → core: results on `money-out.results.v1`

`TransferSucceeded` or `TransferFailed`, payload `{transfer_id, status, reason?, bank_middleware, rail, bank_reference?, attempts, completed_at}`. Terminal only (P§2.2). Exactly one is published per transfer: it's written to the outbox in the same transaction as the terminal state.

### 3.4 Core → MOM: acknowledgements on `money-out.core-acks.v1`

`TransferAcknowledged`, payload `{transfer_id, result_event_id, core_entry: "DEBIT_FINAL" | "VOID", core_transaction_id}`. MOM records it on the transfer (`core_ack`).

### 3.5 MOM → BMW: signed HTTP request

**Recommended:** HTTP, not Kafka (ADR-001, §10). Two routes on every BMW:
- `POST /internal/v1/transfers`: the body is P§2.3's request. It answers with the canonical status (§3.6). Sync banks give the final status here.
- `POST /internal/v1/transfers/status`: `InquireStatus`, body `{transfer_id, request_id}`. It answers with the same canonical model.

**Signing (Recommended: Ed25519, JWS detached, RFC 7515 Appendix F; ADR-002):**
- **Headers:** `X-MOM-Timestamp` (RFC 3339), `X-MOM-Nonce` (128-bit random, base64url) and `X-MOM-Signature` (a detached JWS, `{"alg":"EdDSA","kid":"<key id>"}`).
- **Signed bytes:** `METHOD \n PATH \n X-MOM-Timestamp \n X-MOM-Nonce \n base64url(SHA-256(body))`. The body must also match its own `issued_at` and `nonce`.
- **What a BMW rejects** with `401 invalid_signature`, without calling the bank:
  - a missing or invalid signature, or an unknown `kid`;
  - a timestamp outside `MOM_SIGNATURE_SKEW` (default ±5 min);
  - a nonce already seen within the window. Seen nonces are kept in an in-memory LRU with a TTL equal to the window.
- **Keys:** MOM holds the private key (`MOM_SIGNING_KEY`, from env or a secret file). Each BMW holds only the public keys (`MOM_PUBLIC_KEYS`, `kid=key` pairs), so keys can rotate with two `kid`s live at once.
- **Network:** the BMW internal routes accept only signed requests. The only other inbound route on a BMW is its own bank's webhook (S:C).

### 3.6 BMW → MOM: canonical status

The model is P§2.4's: `ACCEPTED | PROCESSING | SUCCEEDED | FAILED | UNKNOWN`. With `FAILED` comes `reason` (P§2.4's set) and `retry_elsewhere`. Every status also carries `bank_reference` and `bank_raw_code`, both opaque to MOM. The same model is used:
- in the HTTP response;
- in `InquireStatus`;
- in async events on `money-out.bank-events.v1` (`BankStatusReported`, keyed by `transfer_id`, carrying `request_id`).

BMWs publish those events straight to Kafka, because they have no database. If a publish is lost, MOM's inquiry scheduler still resolves the attempt (§6.3).

The `retry_elsewhere` defaults (overridable per bank in its mapping):

| Reason | `retry_elsewhere` |
| :--- | :--- |
| `INVALID_ACCOUNT`, `ACCOUNT_CLOSED`, `NAME_MISMATCH` | false (the beneficiary is the problem) |
| `LIMIT_EXCEEDED`, `INSUFFICIENT_SOURCE_FUNDS`, `RAIL_UNAVAILABLE`, `CUTOFF`, `AUTH_ERROR` | true (it's our side or this bank's) |
| `BANK_REJECTED`, `OTHER` | per mapping row; default false |

---

## 4. Services and repo layout

All in this module and repo, so adding a bank is a new `cmd/bank-middleware-<bank>` plus a package and MOM configuration (P§1.2).

| Path | What |
| :--- | :--- |
| `cmd/payout-adapter`, `internal/payout` | The core's outbound port and result consumer (§2.1) |
| `cmd/money-out-middleware` | MOM: HTTP ops API, consumers, and `worker.Runner` jobs (outbox relay, dispatcher, inquiry scheduler, reconciliation) |
| `internal/moneyout/transfer` | State machine: states, transition table, `Apply`, `ErrIllegalTransition`, as in `internal/ledger` |
| `internal/moneyout/routing` | Rule interface, chain, config loader with reload |
| `internal/moneyout/breaker` | Per `(bank_middleware, rail)` breaker on the shared database |
| `internal/moneyout/service`, `internal/moneyout/repository`, `internal/moneyout/api` | MOM layering: service, repository and handlers, as in the core |
| `internal/moneyout/contract` | Canonical models, money parsing, envelope and event types. Shared by the adapter, MOM and the BMWs, and free of bank concepts. |
| `internal/moneyout/signing` | JWS sign and verify, nonce cache |
| `internal/bankmw` | The shared BMW runtime: signed-request server, the `Bank` interface (`Transfer`, `InquireStatus`), Kafka status emitter, masking logger helpers |
| `internal/bankmw/stuba` … `stubd`, `cmd/bank-middleware-a` … `-d` | One package and one binary per bank |
| `cmd/bank-simulator`, `internal/banksim/stuba` … `stubd` | Test-only simulators (one binary, `SIM_BANK=a…d`) with fault injection (§11) |
| `migrations/money_out/` | MOM's schema, applied by a second migrate service to its own database |
| `migrations/008_payout_clearing.up.sql`, `009_payout_adapter.up.sql` | Additive core migrations: the `payout_clearing` SYSTEM ledger row (IDR), and schema `payout` with the adapter's tables and outbox |
| `config/money-out/routing.yaml` | Routing rules, rail limits, windows, BMW capabilities, kill switches, breaker and retry settings |
| `spec/money-out/` | This TRD, the Development Plan, `ARCHITECTURE.md`, `RUNBOOK.md`, `adr/`, `openapi/`, `asyncapi.yaml` |

**Existing behaviour that changes: none.** No existing Go file, route, table, trigger or test is modified. The only edits to existing files are additive: new services in `docker-compose.yml`, new targets in the `Makefile`, new rows in `.env.example` and the logging catalog, and new dashboard panels.

---

## 5. MOM storage (database `money_out`)

Its own Postgres database on the compose server, user `money_out`, migrated from `migrations/money_out/` (P§1.2: own database).

| Table | Columns (main) | Notes |
| :--- | :--- | :--- |
| `transfers` | `transfer_id` PK, `payload_hash`, `amount BIGINT`, `currency`, `bank_code`, `account_no`, `metadata JSONB`, `state`, `version`, `routing JSONB` (candidates and why each was chosen or skipped), `current_attempt`, `core_ack` (`none / pending / done`), `core_transaction_id`, `created_at`, `updated_at` | The unique `transfer_id` is the idempotency gate. Every state change is `UPDATE … WHERE transfer_id = $1 AND version = $2` (P§3.2). CHECK on `state`, and `amount > 0`. |
| `attempts` | `(transfer_id, attempt_no)` PK, `request_id` UNIQUE, `bank_middleware`, `rail`, `status`, `reason`, `retry_elsewhere`, `bank_reference`, `bank_raw_code`, `reached_bank`, `next_inquiry_at`, `inquiry_deadline`, timestamps | At most one open attempt per transfer: a partial unique index on `transfer_id WHERE status IN ('DISPATCHING','ACCEPTED','PROCESSING','UNKNOWN')` |
| `outbox` | as `ledger.outbox`: `id`, `topic`, `key`, `event_id`, `payload`, `headers`, `published_at` | Written in the same transaction as each state change (P§3.5) |
| `processed_events` | `event_id` PK, `topic`, `processed_at` | Consumer dedupe (bank events, core acks, commands) |
| `breakers` | `(bank_middleware, rail)` PK, `state`, `failures`, `window_start`, `opened_at`, `probes_in_flight`, `version` | Shared across MOM instances (P§3.3) |
| `recon_runs`, `recon_exceptions` | the run per business date, totals; exceptions `(run_id, transfer_id, type, mom_value, external_value)` | P§3.7. Append-only. |

The full history lives in the journal topic (§7); the database holds current state.

---

## 6. MOM behaviour

### 6.1 State machine (P§3.4)

States: `RECEIVED, ROUTED, DISPATCHING, ACCEPTED, PROCESSING, UNKNOWN, ATTEMPT_FAILED, SUCCEEDED, FAILED, NEEDS_MANUAL_REVIEW`. The transition table is exactly the brief's diagram, and `transfer.Apply` returns `ErrIllegalTransition` for anything else. Names shared with the core (`PROCESSING`, `FAILED`) mean the same thing.

Two places need your answer:
- **Q3:** can a reconciliation or ops action move a transfer *out of* `NEEDS_MANUAL_REVIEW`, or is it terminal with a compensating core entry made by hand? I've drawn `NEEDS_MANUAL_REVIEW → SUCCEEDED | FAILED`, only through an ops command that records who resolved it, and only after the bank's answer is known.
- **Q4:** if a status inquiry returns `SUCCEEDED` after a transfer was parked as `NEEDS_MANUAL_REVIEW`, should MOM finish it automatically? I recommend yes, since the bank's answer is authoritative.

### 6.2 Idempotency and dispatch (P§3.2)

- **Same command, same payload** (by SHA-256 of the canonical payload): the current state is returned and nothing else happens. **Same `transfer_id`, different payload:** `conflict`. It's journaled as `TransferRejected`, and no bank is called.
- **Who dispatches:** a dispatcher job claims a transfer in `ROUTED` or `ATTEMPT_FAILED` by moving it to `DISPATCHING` with compare-and-set, in the same transaction that inserts the attempt and the journal event.
- **When the call happens:** the HTTP call is made only after that transaction commits (P§3.5).
- **Why two instances can't double-dispatch:** the version check and the partial unique index make it impossible (P§5 #12).

### 6.3 Retry, inquiry, failover (P§3.3)

- **Same-node retry:** only when the request provably never reached the bank. That covers connection refused, DNS failure, a BMW refusing before the bank call (`401 invalid_signature` is *not* retried: it's a bug), and the BMW's own auth refresh. Exponential backoff with jitter, `max_attempts` per BMW from config. The BMW reports `reached_bank: false` in these cases, so MOM can tell.
- **Ambiguous** (timeout, 5xx, `UNKNOWN`): the attempt becomes `UNKNOWN` with `next_inquiry_at`. The inquiry job calls `InquireStatus` every `inquiry.interval` until `inquiry.max_duration`:
  - `SUCCEEDED`: done.
  - `FAILED`, or "doesn't exist": the attempt is clearly failed, and failover is allowed.
  - Still unknown at the deadline: `NEEDS_MANUAL_REVIEW`, with nothing terminal sent to the core.
- **Failover:** to the next candidate, only after `ATTEMPT_FAILED` with `retry_elsewhere = true`. `UNKNOWN` never fails over.
- **Breakers** follow P§3.3, keyed by `(bank_middleware, rail)`:
  - **What counts:** pre-bank connection failures, `RAIL_UNAVAILABLE`, `BANK_REJECTED`, `AUTH_ERROR` and timeouts count. Business refusals don't.
  - **Open:** skips the candidate for new dispatches only. Inquiries continue regardless.
  - **Half-open:** allows `probes` dispatches.
  - **State:** stored in `breakers` and changed with compare-and-set, so it's shared across instances. Every change is a journal event and is visible in `/bank-middlewares/health`.

### 6.4 Routing (P§3.1)

Interface: `Rule.Apply(ctx, Request, []Candidate) ([]Candidate, []Decision)`. The chain is built from `routing.yaml` and reloaded when the file changes, with the old chain kept if the new one doesn't validate. The initial rules, in order:
1. `bmw_enabled` (kill switch);
2. `rail_capability` (which rails each BMW supports);
3. `amount_rail_limits` (`BIFAST ≤ 250,000,000`; `RTOL ≤ 50,000,000`; `RTGS ≥ 100,000,000`; `SKNBI` for larger non-urgent amounts; all in config);
4. `rail_window` (cut-offs, e.g. RTGS before 15:00 WIB);
5. `intrabank_preference` (beneficiary bank code → BMW whose source account is at that bank);
6. `breaker_open` (skip, at decision time).

Each rule records why a candidate was kept or dropped, and the decision is stored on the transfer. **Q5** is the default order of rails when several fit (§12).

---

## 7. Topics

| Topic | Key | Producer → consumer | Envelope `event_type`s | Retention |
| :--- | :--- | :--- | :--- | :--- |
| `money-out.commands.v1` | `transfer_id` | payout-adapter (outbox) → MOM | `TransferRequested` | 7 days |
| `money-out.results.v1` | `transfer_id` | MOM (outbox) → payout-adapter | `TransferSucceeded`, `TransferFailed` | 7 days |
| `money-out.core-acks.v1` | `transfer_id` | payout-adapter (outbox) → MOM | `TransferAcknowledged` | 7 days |
| `money-out.bank-events.v1` | `transfer_id` | BMWs (direct) → MOM | `BankStatusReported` | 7 days |
| `money-out.journal.v1` | `transfer_id` | MOM (outbox) → (audit, reconciliation, future readers) | every transition: `TransferReceived`, `TransferRouted`, `AttemptDispatching`, `AttemptAccepted`, `AttemptProcessing`, `AttemptUnknown`, `AttemptFailed`, `TransferSucceeded`, `TransferFailed`, `TransferNeedsReview`, `TransferRejected`, `BankStatusReceived`, `CoreAcknowledged`, `BreakerChanged`, `ReconExceptionRaised` | **Q6:** long-term retention. Locally 7 days, like `ledger.events.v1`. |

- **Envelope:** the core's (§1.3), with `stream_type = "transfer"` and `stream_id = transfer_id`, and per-transfer `stream_version`. Journal payloads add `attempt_no, from_state, to_state` (P§3.5).
- **Consumers:** they dedupe on `event_id` (via `processed_events` in MOM, and the adapter's own table) and commit offsets only after processing.
- **Breaker events** use `stream_type = "breaker"` and `stream_id = <bank_middleware>/<rail>`.

---

## 8. Reconciliation (P§3.7)

The job `recon` is a `worker.Runner` leader job at 01:00 WIB for D−1. It can be rerun with `money-out-middleware recon --date YYYY-MM-DD`. It does five checks:
1. **MOM vs bank:** `InquireStatus` for every attempt that `reached_bank` on D, within each BMW's look-back (default 31 days).
2. **MOM vs core:** every terminal transfer has a `core_ack` matching one core entry. MOM checks that the ack's `core_transaction_id` exists in the core, with the same amount, currency and `reference = transfer_id`, through ledger-api's `GET /internal/v1/transactions/{id}` (client `money-out`, `read` only). It also checks that every command received has a transfer row.
3. **DB vs journal:** reads `money-out.journal.v1` for D's transfers and compares the last `to_state` with the row.
4. **Stale items:** anything still in `ACCEPTED`, `PROCESSING`, `UNKNOWN` or `NEEDS_MANUAL_REVIEW` past `recon.stale_after`.
5. **Statements:** a `StatementSource` interface per BMW, with a TODO and no implementation. The contracts have no statement API.

It reports and flags and never moves money. Mismatches go into `recon_exceptions`, move a non-terminal transfer to `NEEDS_MANUAL_REVIEW`, and emit `ReconExceptionRaised`. Each run ends with one summary log line.

---

## 9. Observability

Every new critical operation gets one catalog event, with dashboard panels for error-level events (repo rule). New domains are `transfer`, `bank` and `payout`. Draft events:

| Event | Covers |
| :--- | :--- |
| `transfer.receive` | Command accepted, duplicate or conflict |
| `transfer.state_change` | Every transition, with `from_state`, `to_state`, `attempt_no` |
| `transfer.dispatch` | The call to a BMW, with `bank_middleware`, `rail`, `request_id`, `duration_ms` |
| `transfer.inquiry` | A status inquiry |
| `transfer.breaker` | A breaker transition |
| `transfer.recon` | A reconciliation run and its exceptions |
| `bank.request_verify` | A BMW refusing a signature, a stale timestamp or a replayed nonce (C1) |
| `bank.call` | One per bank API call, with `bank`, `step`, `bank_raw_code`, `duration_ms` |
| `bank.webhook` | An S:C webhook: accepted, unknown session, or expired |
| `payout.request` | The adapter's debit |
| `payout.result_applied` | Finalize or void |

**Never logged:** full account numbers (only the last 4 digits), tokens, signatures, nonces, keys, `aaa_challenge_nonce`, `execution_token`, or the core's `reference`/`metadata` text.

---

## 10. ADRs (to write in M1, in `spec/money-out/adr/`)

| ADR | Decision (**Recommended**) | Main reason |
| :--- | :--- | :--- |
| 001 Transport | Core ↔ MOM and BMW → MOM: Kafka. MOM → BMW: signed HTTP. | The core already integrates by Kafka out and HTTP in (§1.4). Sync banks return the final status in the HTTP response. The BMW stays stateless, and the dispatch-after-commit rule is simple to enforce. |
| 002 Signing | Ed25519 detached JWS, timestamp and nonce, `kid` rotation | Asymmetric, so a BMW can't forge requests. Small and fast. Standard library (`crypto/ed25519`), no new dependency. |
| 003 Consistency | The transactional outbox in MOM and the adapter, and consumer dedupe on `event_id` | The same at-least-once and dedupe model as L§7.3 |
| 004 Holds | H1 (§2.2), **approved** | No core change |

---

## 11. Per-bank status mapping (draft; goes into each BMW README)

Rows marked **(A)** are assumptions: the contract shows only the success path for them. The simulators will implement exactly these rows, so each one needs your confirmation or a correction (Q7).

### 11.1 `bank-middleware-a` (S:A, BI-SNAP, sync)

| Step and bank answer | Canonical | Reason, `retry_elsewhere` |
| :--- | :--- | :--- |
| Account inquiry `2001600` | continue | — |
| Account inquiry `4041611` invalid account **(A)** | FAILED | `INVALID_ACCOUNT`, false |
| Transfer `2001800` | SUCCEEDED | — |
| Transfer HTTP 202 `2021800`, 403 `4031816`, HTTP 500, 504, timeout | UNKNOWN | — (S:A lists these as Pending) |
| Transfer `4031802` exceeds limit **(A)** | FAILED | `LIMIT_EXCEEDED`, true |
| Transfer `4031814` insufficient source funds **(A)** | FAILED | `INSUFFICIENT_SOURCE_FUNDS`, true |
| Transfer `4011800` unauthorized | pre-bank; refresh the token, then retry once | `AUTH_ERROR`, true if it repeats |
| Status `latestTransactionStatus` `00` | SUCCEEDED | — |
| Status `01`, `02`, `03` (initiated, paying, pending) **(A, from SNAP)** | PROCESSING | — |
| Status `06` failed **(A)** | FAILED | `BANK_REJECTED`, true |
| Status `07` not found **(A)** | FAILED (doesn't exist) | `OTHER`, true |

### 11.2 `bank-middleware-b` (S:B, Corporate H2H, sync)

| Step and bank answer | Canonical | Reason, `retry_elsewhere` |
| :--- | :--- | :--- |
| Beneficiary inquiry `AccountStatus: ACTIVE` | continue | — |
| `AccountStatus: CLOSED` **(A)**, `DORMANT` **(A)** | FAILED | `ACCOUNT_CLOSED`, false |
| Transfer `Status: Success` | SUCCEEDED | — |
| Transfer error body `ErrorCode` **(A, schema not in contract)** | FAILED per code | per code |
| Transfer timeout or 5xx | UNKNOWN | — |
| Status `SettlementState: SETTLED` | SUCCEEDED | — |
| Status `PENDING` **(A)** | PROCESSING | — |
| Status `REJECTED` with `FailureReason` **(A)** | FAILED | mapped from `FailureReason`; default `BANK_REJECTED` |
| Status not found **(A)** | FAILED (doesn't exist) | `OTHER`, true |

### 11.3 `bank-middleware-c` (S:C, async multi-ACK)

The whole handshake runs inside the BMW. MOM sees `ACCEPTED → PROCESSING → terminal`.

| Step | Canonical | Notes |
| :--- | :--- | :--- |
| Initiate `202 INIT_ACCEPTED` | ACCEPTED (sync response) | Session kept in memory: `bank_session_id`, `pg_trx_ref`, deadlines |
| ACK-1 webhook | PROCESSING (event) | The BMW computes `SHA-256(pg_trx_ref|dest_bank_code|bank_acct_no|remit_amt|aaa_challenge_nonce)` in `canonical_fields_order`, and `HMAC-SHA256(aaa_secret, hex digest)`, then calls aaa-verify |
| aaa-verify `AAA_PASSED` | (no event) | — |
| ACK-2 `acct_valid: false` **(A)** | FAILED | `INVALID_ACCOUNT` (`ACCOUNT_CLOSED` when `acct_status` says closed), false |
| ACK-2 `request_valid: false` **(A)** | FAILED | `BANK_REJECTED`, true |
| ACK-2 valid | (no event) | Execute before `execution_deadline` with the single-use `execution_token` |
| Execute `final_state: SETTLED` | SUCCEEDED | `bank_reference = core_settlement_id` |
| Execute timeout or 5xx | UNKNOWN | Then the status poll |
| Deadline passed **before** execute was sent | FAILED | `OTHER`, true: funds can't have moved |
| Status poll `final_state: SETTLED` | SUCCEEDED | — |
| Status poll other `final_state` **(A: values not in the contract)** | per value, else UNKNOWN | — |
| Restart with no session in memory | poll status; if it can't be determined, UNKNOWN | Never re-executes: `execute` is sent only by the process holding the token |

Webhooks are accepted only for a live session in this process, matching `bank_session_id`, `pg_trx_ref` and (for ACK-1) the nonce stage, and before `expires_at`. The contract defines no webhook signature (Q8).

### 11.4 `bank-middleware-d` (S:D, multi-rail, sync submit, async settle)

| Step and bank answer | Canonical | Reason, `retry_elsewhere` |
| :--- | :--- | :--- |
| acct-validate `00 ACCOUNT_FOUND` | continue | — |
| acct-validate not found **(A: code not in the contract)** | FAILED | `INVALID_ACCOUNT`, false |
| funding-check `rail-window-open: false` | FAILED | `CUTOFF`, true |
| funding-check `available-liquidity < tx-amount` | FAILED | `INSUFFICIENT_SOURCE_FUNDS`, true |
| remit-single `00 SUBMITTED`, `tx-state: IN_CLEARING` | ACCEPTED, then PROCESSING | The BMW polls remit-status and emits events |
| remit-status `SETTLED` | SUCCEEDED | — |
| remit-status `REJECTED` / `RETURNED` **(A)** | FAILED | `BANK_REJECTED`, true |
| Timeout or 5xx | UNKNOWN | — |

**Rail capabilities, draft for routing:**

| BMW | Rails | Source |
| :--- | :--- | :--- |
| A | `BIFAST`, `RTOL` | (A): S:A shows only `channel: BIFAST` |
| B | `INTRABANK` (to 014), `RTOL` | (A): S:B's single transfer has no rail field |
| C | `BIFAST` | (A): S:C has no rail field |
| D | `BIFAST`, `SKNBI`, `RTGS` | explicit in S:D |

---

## 12. Gaps in the bank contracts, and questions

### 12.1 Contract gaps (what the plan assumes until you say otherwise)

1. **Failure paths are missing for S:A, S:B and S:D.** Only success responses are shown. The mapping in §11 uses SNAP-standard codes for S:A and invents codes for S:B and S:D, all marked (A).
2. **S:B's stringToSign isn't defined.** Assumed: BCA's published `METHOD:RelativeUrl:AccessToken:lowercase(hex(SHA-256(minify(body)))):Timestamp`.
3. **S:D's `canonicalBody` isn't defined.** Assumed: the minified JSON request body.
4. **S:D's `remit-status` is shown only for a `batch-ticket`.** For a single remittance, assumed: the same route with `data.pg-order-id`.
5. **S:C's webhooks carry no signature or authentication.** Only `X-BankC-Event`. The BMW can validate the session and stage (P§4), but anyone who learns a session id could forge an ACK. That's a real gap (Q8).
6. **S:C's execution token isn't computed.** The bank *issues* it in ACK-2 (P§2.5 says it is computed). Only the AAA hash and HMAC are computed.
7. **S:C debits a fee from our vault on top of the amount** (`debited_amt = remit_amt + fee_amt`). The beneficiary gets the full amount. Who bears the fee (Q9)?
8. **S:B's single transfer has no beneficiary bank code.** BCA's corporate transfer is BCA-to-BCA. So B is treated as an intrabank rail to 014, with an RTOL assumption (A).
9. **Source account per BMW** (`sourceAccountNo`, `SourceAccountNumber`, `source_vault_id`, `funding-acct-no`) is configuration. Values come from the contract examples.

### 12.2 Questions (all answered 2026-11-25: Q1 H1; Q2–Q10 as recommended)

| # | Question | Answer (the recommendation, accepted) |
| :--- | :--- | :--- |
| Q1 | Hold model (§2.2) | **Answered: H1** |
| Q2 | A manual correction that must re-debit a user who has since spent the money | Refuse automatic re-debit. RUNBOOK escalates; ops decide (overdraft client or write-off). |
| Q3 | Can `NEEDS_MANUAL_REVIEW` be resolved to `SUCCEEDED`/`FAILED` by an ops command? | Yes, with the actor recorded, after the bank's answer is known |
| Q4 | An inquiry finds the answer after the transfer was parked: finish automatically? | Yes |
| Q5 | Rail preference when several fit, and RTOL vs `RTO` naming (the contract says RTO) | BIFAST, then RTOL, then SKNBI, then RTGS for ≤ 250 M; RTGS for ≥ 100 M when urgent. **`RTOL`** everywhere, as the brief has it. |
| Q6 | Retention of `money-out.journal.v1` in production | Unlimited (it's the audit history), or tiered storage |
| Q7 | Confirm or correct every (A) row in §11 | — |
| Q8 | S:C webhook authenticity beyond session matching | Accept session matching for the POC; ask the bank for an HMAC header |
| Q9 | Who bears S:C's fee? | The aggregator, outside the user's ledger (not in this phase) |
| Q10 | Where the payout request comes from: a new `POST /internal/v1/payouts` on `payout-adapter` (§2.1)? | Yes |

### 12.3 Suggestions beyond the brief (questions only, not in the plan)

- **S1:** sign `BankStatusReported` events from BMWs too, so a compromised producer can't fake a `SUCCEEDED`.
- **S2:** a beneficiary name check against the inquiry result (`NAME_MISMATCH`). The command has no name today.
- **S3:** a daily limit per beneficiary or per source account, as a routing rule.
- **S4:** an alert on `payout_clearing`'s balance age: money in flight older than X hours.
- **S5:** mTLS between MOM and the BMWs, in addition to the signatures.
