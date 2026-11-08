# Technical Requirements Document (TRD): Immutable Ledger & Event-Sourced Balance System

| | |
| :--- | :--- |
| **Status** | Draft for review |
| **Date** | 2026-11-05 |
| **Source** | [BRD](<BRD - Immutable Ledger & Event-Sourced Balance System.md>) |
| **Repo** | `random-learning-loki-kafka` (Go module `auth-api`) |
| **Audience** | Engineers building and reviewing the ledger service in this repo |

This TRD turns the BRD into a buildable design for this repo. It reuses the existing stack and conventions: Go/Gin, pgx, go-redis, `slog` JSON logs → Alloy → Loki → Grafana, and the append-only trigger pattern from `auth_token_history`. It adds Kafka, the ledger services and a fraud engine.

Requirement IDs from the BRD (FR-xx, NFR) are cited inline. §15 maps each one to the section that satisfies it.

---

## 1. Scope

### 1.1 In scope (v1)

- One balance **account** per user (1:1), holding one **sub-ledger** per currency.
- `CREDIT`, `DEBIT` and `VOID` transactions with the full lifecycle `PENDING → PROCESSING → SETTLED | FAILED`, and `SETTLED → VOIDED`.
- An append-only event store and double-entry journal, both protected at the database level.
- Real-time balance through a projection written in the same ACID transaction as the journal.
- Idempotency on every mutating endpoint, using Redis as the fast path and Postgres as the authority.
- `trace_id` propagation through HTTP, Postgres rows, Kafka and logs.
- A transactional outbox that relays to Kafka.
- A fraud engine with two velocity rules that can `FREEZE` an account.
- Reconciliation, stuck-transaction recovery and 7-year archival to object storage.

### 1.2 Out of scope (v1)

- Transfers between users.
- FX between sub-ledgers.
- Two-phase authorisation/capture holds. The BRD's "funds being verified or locked" is met by row locking during `PROCESSING`, not by holds.
- Partial voids. A void always negates the full original amount.
- Statements, ML-based fraud scoring, and a customer-facing debit API.

### 1.3 Assumptions

Each item here is confirmed or overturned in §16.

- **A1.** Balance mutations are initiated by trusted internal services (payments, top-up gateway, ops). End users only read.
- **A2.** Amounts are integers in the currency's minor unit (`IDR` has exponent 0). No floating point anywhere.
- **A3.** The 7-year clock runs from a journal entry's `created_at`.

---

## 2. Conventions inherited from this repo

New code MUST follow these patterns that already exist in the repo. Reviewers check against this list.

| Area | Existing pattern | Applies to ledger as |
| :--- | :--- | :--- |
| Layout | `cmd/<bin>/main.go`, `internal/{api,service,repository,session,config,logger}` | New binaries under `cmd/`; new code in the same layered packages (§9) |
| Layering | Handlers → service (business rules, logging) → repository (SQL, no logging) | Unchanged. Repositories return wrapped errors only. |
| Interfaces | The service declares the store interfaces it needs (`UserStore`, `TokenHistory`) | `LedgerStore`, `IdempotencyCache`, `FreezeClient`, … declared in `internal/service` |
| Errors | `ValidationError`, sentinel `Err*`, `fail()` maps errors to codes | Same, with ledger codes (§6.5) |
| HTTP envelope | `{"error":{"code","message","request_id"}}` via `abortError` | Same envelope. Ledger errors add `transaction_id` when one exists. |
| Middleware | `RequestContext`, `AccessLog`, `Recovery`, `BodyLimit(1MiB)`, `Auth` | Reused as is. `ClientAuth` added for internal callers (§6.1). |
| Logging | `.claude/rules/observability-logging.md`: `event` / `outcome` / `reason`, `logger.FromContext(ctx)` | Mandatory. New events in §11.1 go into the catalog in the same PR. |
| Config | `internal/config`: env vars with defaults; `DATABASE_URL` required | New env vars in §12.2 |
| Startup | `retry` backoff to PG/Redis (~30s), graceful shutdown, `healthcheck` subcommand | Same for every new binary. Kafka is added as a dependency. |
| Schema | Numbered `migrations/NNN_*.up.sql` / `.down.sql`; guard triggers; CHECK constraints | `002_ledger.up.sql` (§5) |
| Image | Multi-stage build → `distroless/static:nonroot`, read-only rootfs | One Dockerfile with `ARG CMD` that selects the binary |
| Tests | Fakes and miniredis in `internal/api/api_test.go`; log assertions on `h.logs`; `go test -race` | Same, plus `//go:build integration` tests against the compose Postgres and Kafka (§13) |
| Git | `.claude/rules/git-workflow.md` | One `feat/ledger-*` branch and one PR per phase (§14) |

---

## 3. Architecture

### 3.1 Components

```
                 ┌────────────────────── internal callers (payments, top-up, ops) ─────┐
                 │  Bearer service token · Idempotency-Key · traceparent              │
                 ▼                                                                     │
 users ──▶ auth-api (existing)                                                          │
   │        sessions in Redis ◀──────────────┐                                         │
   │                                         │ session lookup (existing Auth mw)       │
   └──── GET balance/history ──▶ ledger-api ─┘◀────────────────────────────────────────┘
                                   │   ▲
                 Tx1/Tx2 (ACID)    │   │ POST /internal/v1/accounts/{id}/freeze
                                   ▼   │
                         ┌──────────────────┐        ┌───────────────┐
                         │ Postgres 17      │        │ Redis 7.4     │
                         │  schema ledger:  │        │  idem cache   │
                         │  events, journal │        │  fraud windows│
                         │  projections,    │        └───────────────┘
                         │  outbox          │               ▲
                         └──────────────────┘               │
                                   │ LISTEN/NOTIFY + poll   │
                                   ▼                        │
                         ledger-worker ── relay ──▶ Kafka ledger.events.v1 ──▶ fraud-engine
                          (relay, sweeper,                (key = account_id)        │
                           reconciler, partitions,                                  │
                           archiver)                                                │
                                                                                    │
                   all containers: JSON stdout ─▶ Alloy ─▶ Loki ─▶ Grafana (dashboards + alerts)
```

| Binary | Role | Scaling |
| :--- | :--- | :--- |
| `cmd/ledger-api` | HTTP API. Runs Tx1 and Tx2 on the request path. | Stateless, N replicas |
| `cmd/ledger-worker` | Outbox relay, stuck-transaction sweeper, reconciler, partition maintenance, archiver, idempotency purge | N replicas. Each job holds a `pg_advisory_lock`, so exactly one leader runs each job. |
| `cmd/fraud-engine` | Kafka consumer group `fraud-engine`. Evaluates rules and calls freeze. | Up to the topic's partition count (12) |
| `cmd/api` (existing) | auth-api. Unchanged apart from the shared `RequestContext` change (§8). | — |

### 3.2 Source of truth vs projections

The BRD forbids mutable state as the source of truth, and FR-03 allows projections for reads. The design separates the two strictly:

| Kind | Tables | Mutability | Can be rebuilt from |
| :--- | :--- | :--- | :--- |
| **Source of truth** | `ledger.events`, `ledger.journal_entries` | INSERT only. UPDATE, DELETE and TRUNCATE are blocked by trigger and by grants. | — |
| **Projections** | `ledger.accounts` (status, version), `ledger.ledgers` (balance, last_sequence), `ledger.transactions` (status) | Updated only in the same transaction that appends the event or journal entry they reflect | `ledger-worker rebuild-projections` (left-fold of events and journal) |
| **Operational** | `ledger.idempotency_keys`, `ledger.outbox`, `ledger.checkpoints` | Mutable and purgeable. Not ledger data. | — |

Because projections commit atomically with the journal, the balance a caller reads is never stale (FR-03, NFR consistency). The reconciler (§7.6) continuously proves that each projection equals the fold of the source of truth.

---

## 4. Domain model

### 4.1 Money

- `amount` is `int64` in minor units. Request amounts are `1 ≤ amount ≤ LEDGER_MAX_AMOUNT` (default `1_000_000_000_000`), so the sum of two amounts can never overflow `int64`.
- `currency` is ISO 4217 and must be in `LEDGER_CURRENCIES` (default `IDR`).
- `internal/ledger/money.go` holds `Money{Amount int64; Currency string}`. Arithmetic is checked and returns `ErrOverflow` instead of wrapping.

### 4.2 Accounts and sub-ledgers (1:1)

- `ledger.accounts.user_id` is `UNIQUE`, which enforces the 1:1 user ↔ account rule at the database level.
- An account holds one **user ledger** per currency (`UNIQUE (account_id, currency)`). The default currency ledger is opened together with the account. Other currencies are opened explicitly (§6.2). A credit to a currency the account has no ledger for is denied with `ledger_not_found`. Ledgers are never opened implicitly.
- **System ledgers** (`kind = 'SYSTEM'`) are the double-entry counterparties, for example `topup_clearing` and `settlement`, one per currency. They have no account and no balance check. Their balance is the `SUM` of their journal lines and is not stored in a locked row, so they never become a write hotspot.

### 4.3 Double-entry journal

- Every settled transaction writes **exactly two** journal lines with signed amounts: the user ledger line and the system ledger line. They sum to zero.
- A positive amount increases that ledger's balance.

| Type | User ledger line | System ledger line |
| :--- | :--- | :--- |
| `CREDIT` | `+amount` | `−amount` on the counterparty (default `topup_clearing`) |
| `DEBIT` | `−amount` | `+amount` on the counterparty (default `settlement`) |
| `VOID` of T | `−(T's user line)` | `−(T's system line)` |

- User-ledger lines also carry `ledger_sequence` (gapless, starting at 1 per ledger) and `balance_after`. This gives every balance mutation a total order and a running balance that auditors can check line by line.

### 4.4 Transaction state machine

```
          ┌─────────┐   Tx2 lock    ┌────────────┐  checks pass   ┌─────────┐   void settled   ┌────────┐
 request ─▶ PENDING ├──────────────▶│ PROCESSING ├───────────────▶│ SETTLED ├─────────────────▶│ VOIDED │
          └─────────┘               └─────┬──────┘                └─────────┘                  └────────┘
                                          │ checks fail / processing_timeout
                                          ▼
                                     ┌────────┐
                                     │ FAILED │
                                     └────────┘
```

- Allowed transitions are exactly the arrows above. `internal/ledger.Apply(state, event)` returns `ErrIllegalTransition` for anything else, and the fold is used both at runtime and by the reconciler.
- `PROCESSING` and the terminal event (`SETTLED` or `FAILED`) commit in the **same** Tx2. Seeing a transaction in `PENDING` for longer than `LEDGER_SWEEP_AFTER` is therefore the recovery signal (§7.5).
- A `FAILED` transaction is terminal. Retrying needs a new idempotency key, which creates a new transaction.
- `VOIDED` applies to the **original** transaction. The void itself is a new transaction of type `VOID` that goes through its own `PENDING → … → SETTLED`.

### 4.5 Events

Each event belongs to one stream. `(stream_id, stream_version)` is unique and strictly increasing, and the version is assigned while holding the stream's row lock.

| Stream | `stream_id` | Events (`event_type`) | Payload (beyond the envelope) |
| :--- | :--- | :--- | :--- |
| `transaction` | transaction id | `TransactionRequested` | `type, amount, currency, counterparty, original_transaction_id?, reference?, requested_by` |
| | | `TransactionProcessing` | `ledger_id` |
| | | `TransactionSettled` | `journal_entry_ids[], ledger_sequence, balance_after, type, amount, currency` (the last three added in P4.1, additive within `schema_version` 1, for the fraud engine) |
| | | `TransactionFailed` | `reason` (§6.5 codes) |
| | | `TransactionVoided` | `void_transaction_id` |
| `account` | account id | `AccountOpened` | `user_id` |
| | | `LedgerOpened` | `ledger_id, currency, overdraft_limit` |
| | | `AccountFrozen` | `reason, actor, rule?` |
| | | `AccountUnfrozen` | `reason, actor` |

The BRD's `TOPUP_REQUESTED → TOPUP_PROCESSING → TOPUP_SETTLED` maps to `TransactionRequested`/`Processing`/`Settled` with `type = CREDIT`. Keeping one event family for all types keeps the fold and the consumers uniform.

---

## 5. Data model (`migrations/002_ledger.up.sql`)

Everything lives in schema `ledger` in the existing Postgres. IDs are UUIDv7, generated in the app with `github.com/google/uuid` `NewV7`, so they sort by time and index well.

> **`migrations/002_ledger.up.sql` is authoritative.** The SQL below is the design sketch. The migration adds, beyond it: named CHECKs for every enum and format (`trace_id`, currency, event type per stream type), composite FKs `(ledger_id, currency)` so a journal line or transaction can't carry a currency different from its ledger's, the per-partition TRUNCATE guards and the `journal_entries_balanced` trigger from §5.1, `ledger.ensure_partitions()`, and the `idempotency_keys` change below.

```sql
BEGIN;
CREATE SCHEMA ledger;

-- ---------- projections ----------
CREATE TABLE ledger.accounts (
    id          UUID        PRIMARY KEY,
    user_id     UUID        NOT NULL UNIQUE REFERENCES public.users (id) ON DELETE RESTRICT,  -- 1:1
    status      TEXT        NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'FROZEN')),
    version     BIGINT      NOT NULL DEFAULT 0,          -- = last account-stream version
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX accounts_updated_idx ON ledger.accounts (updated_at);  -- migration 005: reconciler scope (§7.6)

CREATE TABLE ledger.ledgers (
    id              UUID        PRIMARY KEY,
    kind            TEXT        NOT NULL CHECK (kind IN ('USER', 'SYSTEM')),
    account_id      UUID        REFERENCES ledger.accounts (id) ON DELETE RESTRICT,
    code            TEXT,                                  -- SYSTEM only, e.g. 'topup_clearing'
    currency        CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    overdraft_limit BIGINT      NOT NULL DEFAULT 0 CHECK (overdraft_limit >= 0),
    balance         BIGINT      NOT NULL DEFAULT 0,        -- USER only; SYSTEM balance = SUM(journal)
    last_sequence   BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ledgers_kind_shape CHECK (
        (kind = 'USER'   AND account_id IS NOT NULL AND code IS NULL) OR
        (kind = 'SYSTEM' AND account_id IS NULL     AND code IS NOT NULL)),
    -- FR-02 backstop: even a buggy service cannot commit a balance below the overdraft floor.
    CONSTRAINT ledgers_no_negative CHECK (kind = 'SYSTEM' OR balance >= -overdraft_limit)
);
CREATE UNIQUE INDEX ledgers_user_currency_key   ON ledger.ledgers (account_id, currency) WHERE kind = 'USER';
CREATE UNIQUE INDEX ledgers_system_currency_key ON ledger.ledgers (code, currency)       WHERE kind = 'SYSTEM';

CREATE TABLE ledger.transactions (
    id                      UUID        PRIMARY KEY,
    account_id              UUID        NOT NULL REFERENCES ledger.accounts (id),
    ledger_id               UUID        REFERENCES ledger.ledgers (id),   -- set in Tx2
    counterparty_ledger_id  UUID        NOT NULL REFERENCES ledger.ledgers (id),
    type                    TEXT        NOT NULL CHECK (type IN ('CREDIT', 'DEBIT', 'VOID')),
    status                  TEXT        NOT NULL CHECK (status IN ('PENDING','PROCESSING','SETTLED','FAILED','VOIDED')),
    amount                  BIGINT      NOT NULL CHECK (amount > 0),
    currency                CHAR(3)     NOT NULL,
    original_transaction_id UUID        REFERENCES ledger.transactions (id),
    failure_reason          TEXT,
    reference               TEXT        CHECK (char_length(reference) <= 64),
    metadata                JSONB       CHECK (pg_column_size(metadata) <= 4096),
    requested_by            TEXT        NOT NULL,                          -- client_id (§6.1)
    version                 BIGINT      NOT NULL,                          -- = last transaction-stream version
    trace_id                CHAR(32)    NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at              TIMESTAMPTZ,
    CONSTRAINT transactions_void_has_original CHECK ((type = 'VOID') = (original_transaction_id IS NOT NULL))
);
CREATE INDEX transactions_account_created_idx ON ledger.transactions (account_id, created_at DESC, id DESC);
CREATE INDEX transactions_pending_idx         ON ledger.transactions (created_at) WHERE status = 'PENDING';
CREATE INDEX transactions_updated_idx         ON ledger.transactions (updated_at);  -- migration 005: reconciler scope (§7.6)
-- At most one void per original that is in flight or settled. A FAILED void can be retried.
CREATE UNIQUE INDEX transactions_single_void_key ON ledger.transactions (original_transaction_id)
    WHERE type = 'VOID' AND status IN ('PENDING', 'PROCESSING', 'SETTLED');

-- ---------- source of truth (append-only, partitioned monthly) ----------
CREATE TABLE ledger.journal_entries (
    id              UUID        NOT NULL,
    transaction_id  UUID        NOT NULL,
    ledger_id       UUID        NOT NULL REFERENCES ledger.ledgers (id),
    amount          BIGINT      NOT NULL CHECK (amount <> 0),       -- signed
    currency        CHAR(3)     NOT NULL,
    ledger_sequence BIGINT,                                         -- USER lines only, gapless per ledger
    balance_after   BIGINT,                                         -- USER lines only
    trace_id        CHAR(32)    NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at),
    CONSTRAINT journal_user_fields CHECK ((ledger_sequence IS NULL) = (balance_after IS NULL))
) PARTITION BY RANGE (created_at);
CREATE INDEX journal_ledger_seq_idx ON ledger.journal_entries (ledger_id, ledger_sequence);
CREATE INDEX journal_tx_idx         ON ledger.journal_entries (transaction_id);

CREATE TABLE ledger.events (
    event_id       UUID        NOT NULL,
    stream_type    TEXT        NOT NULL CHECK (stream_type IN ('transaction', 'account')),
    stream_id      UUID        NOT NULL,
    stream_version BIGINT      NOT NULL CHECK (stream_version > 0),
    event_type     TEXT        NOT NULL,
    account_id     UUID        NOT NULL,
    schema_version SMALLINT    NOT NULL DEFAULT 1,
    payload        JSONB       NOT NULL,
    trace_id       CHAR(32)    NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);
CREATE INDEX events_stream_idx ON ledger.events (stream_id, stream_version);

-- Generic append-only guard (same pattern as auth_token_history_guard, with no permitted UPDATE).
CREATE FUNCTION ledger.append_only_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is append-only (% not allowed)', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
$$;
CREATE TRIGGER journal_entries_guard_row  BEFORE UPDATE OR DELETE ON ledger.journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger.append_only_guard();
CREATE TRIGGER journal_entries_guard_trunc BEFORE TRUNCATE ON ledger.journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger.append_only_guard();
CREATE TRIGGER events_guard_row  BEFORE UPDATE OR DELETE ON ledger.events
    FOR EACH ROW EXECUTE FUNCTION ledger.append_only_guard();
CREATE TRIGGER events_guard_trunc BEFORE TRUNCATE ON ledger.events
    FOR EACH STATEMENT EXECUTE FUNCTION ledger.append_only_guard();

-- ---------- operational ----------
CREATE TABLE ledger.idempotency_keys (
    client_id           TEXT        NOT NULL,
    key_hash            CHAR(64)    NOT NULL,          -- sha256(Idempotency-Key); the raw key is never stored
    request_fingerprint CHAR(64)    NOT NULL,          -- sha256(method|route|canonical JSON body)
    operation           TEXT        NOT NULL,          -- account_open | ledger_open | credit | debit | void | freeze | unfreeze
    resource_id         UUID        NOT NULL,          -- transaction, account or ledger id the key produced
    response_status     SMALLINT,                      -- NULL while in flight
    response_body       BYTEA,                         -- exact bytes replayed (migration 003; JSONB would reorder keys)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (client_id, key_hash)
);
CREATE INDEX idempotency_expires_idx ON ledger.idempotency_keys (expires_at);

CREATE TABLE ledger.outbox (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL,                 -- account_id
    payload      JSONB       NOT NULL,                 -- Kafka envelope (§7.3)
    headers      JSONB       NOT NULL,                 -- traceparent, event_type, schema_version
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON ledger.outbox (id) WHERE published_at IS NULL;

CREATE TABLE ledger.checkpoints (                      -- opening balances for archived ranges (§7.8)
    ledger_id      UUID        NOT NULL REFERENCES ledger.ledgers (id),
    as_of_sequence BIGINT      NOT NULL,
    balance        BIGINT      NOT NULL,
    archive_uri    TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ledger_id, as_of_sequence)
);

-- Initial partitions (current month + 3). ledger-worker creates the rest ahead of time (§7.7).
-- System ledgers for each currency in LEDGER_CURRENCIES are seeded here.
COMMIT;
```

### 5.1 Integrity guarantees and where they are enforced

| Guarantee | Primary enforcement | Backstop |
| :--- | :--- | :--- |
| Journal and events immutable | `append_only_guard` triggers | Prod grants: the app role has no UPDATE, DELETE or TRUNCATE on these tables (§5.2) |
| No balance below the floor (FR-02) | Tx2 check under `FOR UPDATE` on the ledger row | `ledgers_no_negative` CHECK |
| 1 user ↔ 1 account | `accounts.user_id UNIQUE` | — |
| One void per original | `transactions_single_void_key` and the original's row lock | Reconciler |
| Gapless `ledger_sequence` and unique stream versions | Assigned as `last+1` while holding the projection row lock | Per-partition unique indexes; reconciler gap and duplicate scan |
| Each transaction has exactly 2 lines, one currency, summing to 0 | `journal_entries_balanced`: a `DEFERRABLE INITIALLY DEFERRED` constraint trigger that checks at COMMIT. `JournalRepo.Post` inserts both lines in one statement, sharing `created_at`. | Reconciler `unbalanced_transaction` check (catches a stray line with a different `created_at`) |
| A line's or transaction's currency equals its ledger's | Composite FKs `(ledger_id, currency)` → `ledgers (id, currency)` | — |
| Duplicate requests | `idempotency_keys` primary key | Redis cache is only an optimisation |

**Partitioning caveat.** Postgres can't enforce a unique index across partitions unless it includes the partition key. Global uniqueness of `(ledger_id, ledger_sequence)` and `(stream_id, stream_version)` therefore rests on serialized assignment under the row lock. Each monthly partition also gets a local unique index, and the reconciler scans for gaps or duplicates at partition boundaries.

**P1 implementation check (resolved in P1.1).** PG 17.11 supports a `DEFERRABLE` constraint trigger on the partitioned `journal_entries`, so the commit-time check is in the database (`journal_entries_balanced`). Its lookup filters on the shared `created_at`, which prunes to one partition.

**TRUNCATE bypass (found in P1.1).** Row-level triggers on a partitioned table are cloned to every partition, but statement-level TRUNCATE triggers are not: `TRUNCATE ledger.journal_entries_y2026m11` skipped the parent's guard and deleted the rows. `ledger.ensure_partitions()` therefore adds a TRUNCATE guard to every partition it creates, and an integration test covers it. `DROP TABLE` or `DETACH PARTITION` on a partition is DDL and isn't blocked by triggers; in production only `ledger_migrator` and `ledger_archiver` hold the privilege (§5.2).

### 5.2 Roles

In compose, all services share the existing `auth` role. That's acceptable for local development because the guard triggers still apply. In production:

| Role | Grants |
| :--- | :--- |
| `ledger_migrator` | Owner. DDL only, used by migrations. |
| `ledger_app` (ledger-api, ledger-worker) | `SELECT, INSERT` on `events` and `journal_entries`. `SELECT, INSERT, UPDATE` on projections. `SELECT, INSERT, UPDATE, DELETE` on `outbox` and `idempotency_keys`. |
| `ledger_archiver` | `DETACH`/`DROP PARTITION` on archived partitions only. Used by the archiver job (§7.8). |
| `ledger_readonly` | `SELECT`, for the reconciler and the read replica |

---

## 6. API

All routes reuse `RequestContext → AccessLog → Recovery → BodyLimit`. Error responses use the existing envelope.

### 6.1 Authentication

- **User routes** (`/api/v1/ledger/*`): the existing `api.Auth` middleware with the same Redis session store as auth-api. An account is resolved from the `user_id` in the session. A user can never pass an `account_id`, which removes IDOR by construction.
  - A transaction that belongs to another account returns 404 `transaction_not_found`, the same as one that doesn't exist, so ids can't be probed.
  - `next_cursor` is opaque (base64url of the last row's `created_at` and `id`). Its absence marks the last page.
  - Redis isn't part of ledger-api readiness. A Redis outage returns 503 on the user routes only; the internal write path keeps serving.
- **Internal routes** (`/internal/v1/*`): new `api.ClientAuth` middleware.
  - Callers send `Authorization: Bearer <service token>`. The middleware SHA-256 hashes it and looks it up in `LEDGER_CLIENTS`, which maps `client_id` to a token hash and a set of allowed operations.
  - Operations are `account_open`, `credit`, `debit`, `void`, `freeze`, `unfreeze` and `read`.
  - A missing or unknown token returns 401 `unauthorized`. An operation not allowed for the client returns 403 `forbidden`. Both emit `ledger.client_auth` (§11.1).
  - Internal routes are bound to the internal network only. In production they sit behind mTLS (§16 Q1).
- `client_id` is recorded as `transactions.requested_by` and as `actor` on freeze events.

### 6.2 Endpoints

| Method | Path | Op | Idempotency-Key | Success |
| :--- | :--- | :--- | :--- | :--- |
| POST | `/internal/v1/accounts` `{user_id}` | `account_open` | required | 201 created, or 200 with the existing account (1:1) |
| POST | `/internal/v1/accounts/{account_id}/ledgers` `{currency, overdraft_limit?}` | `account_open` | required | 201 |
| POST | `/internal/v1/accounts/{account_id}/credits` | `credit` | required | 201 SETTLED, or 202 PENDING |
| POST | `/internal/v1/accounts/{account_id}/debits` | `debit` | required | 201 SETTLED, or 202 PENDING |
| POST | `/internal/v1/transactions/{transaction_id}/void` `{reason}` | `void` | required | 201 SETTLED (the void transaction), or 202 |
| POST | `/internal/v1/accounts/{account_id}/freeze` `{reason, rule?}` | `freeze` | required | 200 (idempotent if already frozen) |
| POST | `/internal/v1/accounts/{account_id}/unfreeze` `{reason}` | `unfreeze` | required | 200 |
| GET | `/internal/v1/accounts/{account_id}` and `?user_id=` | `read` | — | 200 |
| GET | `/internal/v1/transactions/{transaction_id}` | `read` | — | 200 |
| GET | `/api/v1/ledger/account` | user | — | 200: status and balances per currency |
| GET | `/api/v1/ledger/transactions?currency=&limit=&cursor=` | user | — | 200, keyset pagination on `(created_at, id)`, `limit ≤ 100` |
| GET | `/api/v1/ledger/transactions/{transaction_id}` | user | — | 200, or 404 if it isn't the caller's |

`overdraft_limit > 0` may only be set by a client that has the `account_open` operation and is listed in `LEDGER_OVERDRAFT_CLIENTS`. Otherwise it is forced to 0 (FR-02, §16 Q2).

### 6.3 Credit/debit request and response

```http
POST /internal/v1/accounts/0192f3c4-…/debits
Authorization: Bearer <service token>
Idempotency-Key: pay-7f3a9c-2026-11-05T10:00:00Z
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
Content-Type: application/json

{"amount": 150000, "currency": "IDR", "counterparty": "settlement", "reference": "ORD-88123", "metadata": {"channel": "qris"}}
```

```json
HTTP/1.1 201 Created
Location: /internal/v1/transactions/0192f3c5-…

{"transaction": {
  "id": "0192f3c5-…", "type": "DEBIT", "status": "SETTLED",
  "account_id": "0192f3c4-…", "ledger_id": "0192f3c4-…", "amount": 150000, "currency": "IDR",
  "balance_after": 350000, "ledger_sequence": 42,
  "reference": "ORD-88123", "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
  "created_at": "2026-11-05T10:00:00.012Z", "settled_at": "2026-11-05T10:00:00.019Z"}}
```

Validation rules:
- `amount` is an integer within §4.1 limits.
- `currency` must be in `LEDGER_CURRENCIES`.
- `counterparty` must be a configured system ledger code for that currency. It defaults per type (§4.3).
- `reference` is at most 64 characters.
- `metadata` is a JSON object of at most 4 KiB.
- `Idempotency-Key` is 1–255 characters matching `[A-Za-z0-9._:-]`.
- Unknown JSON fields are rejected (`DisallowUnknownFields`).

### 6.4 Idempotency semantics (FR-06)

| Situation | Response |
| :--- | :--- |
| First use of a key | Normal processing |
| Same key and same fingerprint, completed | The stored status and body replayed byte for byte, plus header `Idempotent-Replayed: true` |
| Same key and same fingerprint, still in flight | 409 `request_in_progress`, `Retry-After: 1` |
| Same key, different fingerprint | 422 `idempotency_key_reused` |
| Missing or invalid header on a mutating route | 400 `idempotency_key_required` |
| Key older than `IDEMPOTENCY_TTL` (default 24h, then purged) | Treated as a new key |

Business failures such as `insufficient_funds` are stored and replayed like successes, because the outcome is final.

### 6.5 Error codes

| HTTP | `code` | When | Creates a FAILED transaction? |
| :--- | :--- | :--- | :--- |
| 400 | `invalid_input`, `idempotency_key_required` | Validation | no |
| 401 / 403 | `unauthorized` / `forbidden` | `ClientAuth` / `Auth` | no |
| 404 | `account_not_found`, `ledger_not_found`, `transaction_not_found` | Lookups | `ledger_not_found` does, at processing time |
| 409 | `request_in_progress` | §6.4 | no |
| 409 | `ledger_exists` | Opening a second ledger in a currency | no |
| 422 | `insufficient_funds` | Debit or void would breach the floor | yes |
| 422 | `account_frozen` | Debit on a frozen account | yes |
| 422 | `not_voidable` | Original isn't SETTLED, already voided, or is itself a VOID | yes |
| 422 | `idempotency_key_reused` | §6.4 | no |
| 202 | — | Tx2 didn't finish within `LEDGER_PROCESS_TIMEOUT`. The sweeper completes it (§7.5). | PENDING |
| 503 | `ledger_unavailable` | Postgres unreachable before Tx1 commits | no |

---

## 7. Processing

### 7.1 Write path (credit, debit, void)

```
ledger-api handler
  │ validate → fingerprint
  │ Redis GET idem:ledger:<client_id>:<key_hash> ── hit, fingerprint match → replay (no PG)
  ▼
Tx1  (READ COMMITTED, short)
  INSERT idempotency_keys … ON CONFLICT DO NOTHING RETURNING
    └─ conflict → SELECT existing → §6.4 (replay / 409 / 422), backfill Redis if completed
  INSERT transactions (status=PENDING, version=1)
  INSERT events TransactionRequested (stream_version=1)
  INSERT outbox; NOTIFY ledger_outbox
COMMIT   ← the request is now durable (zero data loss); the client may safely retry from here
  ▼
Tx2  (READ COMMITTED, lock_timeout & statement_timeout = LEDGER_PROCESS_TIMEOUT)
  SELECT … FROM transactions WHERE id=$tx FOR UPDATE          -- status must still be PENDING; else return it
  SELECT status FROM accounts WHERE id=$acct FOR SHARE        -- blocks against a concurrent freeze
  SELECT … FROM ledgers WHERE account_id=$acct AND currency=$cur AND kind='USER' FOR UPDATE
  [VOID] SELECT … FROM transactions WHERE id=$orig FOR UPDATE -- must be SETTLED, type ≠ VOID
  append TransactionProcessing (v2)
  decide:
    DEBIT  and account FROZEN                     → FAILED account_frozen
    DEBIT  and balance − amount < −overdraft      → FAILED insufficient_funds
    VOID   and reversal would breach the floor    → FAILED insufficient_funds   (§16 Q3)
    VOID   and original not voidable              → FAILED not_voidable
    ledger missing                                → FAILED ledger_not_found
  on SETTLED:
    INSERT 2 journal lines (user line: ledger_sequence = last_sequence+1, balance_after)
    UPDATE ledgers SET balance, last_sequence
    append TransactionSettled (v3); projection → SETTLED
    [VOID] append TransactionVoided on the original's stream; original projection → VOIDED
  on FAILED:
    append TransactionFailed (v3); projection → FAILED, failure_reason
  UPDATE idempotency_keys SET response_status, response_body
  INSERT outbox rows for every event appended; NOTIFY ledger_outbox
COMMIT
  ▼
Redis SET idem:… (response, PX = remaining TTL)  -- best effort, after commit
respond 201 / 422
```

- **Idempotency cache (implemented in P2.1).**
  - Redis holds only completed responses: `idem:ledger:<client_id>:<key_hash>` → fingerprint, operation, status and the exact body bytes.
  - Each entry expires at its `idempotency_keys.expires_at`. `CompleteIdempotencyKey` returns the row, so the TTL is computed from that.
  - A hit whose fingerprint or operation differs is ignored, and Postgres decides (422).
  - A 202 PENDING response is never cached. `Process` caches the response once the transaction is terminal, whether the request path or the sweeper ran it.
  - Every Redis call is bounded at 100 ms. A failure logs `dependency.call` at warn, and the request carries on with Postgres alone.
- **Void (implemented in P2.2).**
  - The body is `{"reason"}`, 1–64 characters. The reason is stored as the void's `reference`, so it is returned in responses and never logged.
  - Account, currency, amount and counterparty ledger all come from the original. The fingerprint covers the original's id and the reason.
  - A second void while one is in flight or settled hits `transactions_single_void_key` in Tx1. It is denied with 422 `not_voidable` and creates no transaction.
  - An original that isn't voidable is decided in Tx2 under the original's row lock: not SETTLED, already a VOID, or a mismatch. That records a FAILED VOID with `not_voidable`.
  - A FAILED void doesn't block a later one.
- **Lock order** is always `transactions → accounts → ledgers`. For a void it is the void transaction, then the account, then the ledger, then the original transaction. Every path follows this order, so the design can't deadlock. A transaction touches exactly one user ledger, and system ledgers are never locked.
- **Strict consistency (NFR).** Concurrent debits on the same ledger serialize on the `ledgers` row lock. Each one sees the committed balance of the previous one, so double-spending is impossible. The `ledgers_no_negative` CHECK is the last line of defence.
- **Freezes take effect immediately (FR-09).** Freeze takes `accounts FOR UPDATE`, which conflicts with the `FOR SHARE` every Tx2 holds. Once the freeze commits, every later debit sees `FROZEN`. A debit already holding the lock finishes first, which is the correct serial order. Credits keep settling, and voids are allowed on frozen accounts (§16 Q3).
  - **Finding (P2.3): row locks don't queue fairly.** On PG 17, a new `FOR SHARE` request is granted while a `FOR UPDATE` is waiting, as long as it's compatible with the current holders. Under steady Tx2 traffic on one account, a freeze could be starved past its `lock_timeout`, and that is exactly the fraud-burst case.
  - **Fix.** `LockAccount` first takes a transaction-scoped advisory lock on the account: shared for `FOR SHARE`, exclusive for `FOR UPDATE`. Advisory locks are heavyweight and queued in arrival order. Once a freeze is waiting, later Tx2s wait behind it and then see `FROZEN`.
  - **Lock order.** The order is unchanged, because the advisory lock is taken right before the account row lock. `TestFreezeIsNotStarvedByLaterTx2s` covers this.
- **Exactly-once mutation.** A transaction id moves from PENDING to a terminal state only once, because the row lock is held and the status is re-checked. The idempotency key maps a client retry to that same transaction id.

### 7.2 Freeze and unfreeze

```
Tx: SELECT … FROM accounts WHERE id=$acct FOR UPDATE
    already in target state → no event, return 200 (idempotent)
    append AccountFrozen | AccountUnfrozen (version+1) {reason, actor=client_id, rule?}
    UPDATE accounts SET status, version
    INSERT idempotency_keys (operation=freeze|unfreeze, resource_id=account_id, response) ; INSERT outbox ; NOTIFY
COMMIT
```

`unfreeze` is restricted to the `ops` client by default (§16 Q6). It is enforced at config load: `LEDGER_CLIENTS` may grant `unfreeze` only to clients listed in `LEDGER_UNFREEZE_CLIENTS` (default `ops`), and ledger-api refuses to start otherwise.

Implemented in P2.3:
- **Bodies.** `freeze` takes `{reason, rule?}` and `unfreeze` takes `{reason}`. `reason` is 1–64 characters, stored in the event and never logged. `rule` must be one of `topup_velocity` or `debit_volume`.
- **Response.** Both return 200 `{"account": …}`.
- **Logging.** The `ledger.freeze` and `ledger.unfreeze` events carry `actor`, `rule` and `changed`. An idempotent no-op logs `changed=false`.

### 7.3 Outbox relay → Kafka (FR-07)

- **Topic.** `ledger.events.v1`, keyed by `account_id`, so all of one account's events go to the same partition in order.
  - 12 partitions, comfortably more than 300 TPS needs. Replication factor 3 and `min.insync.replicas=2` in production; 1 locally.
  - Retention is 7 days. Kafka is a feed, not the archive.
- **Envelope.**
  ```json
  {"event_id": "0192…", "event_type": "TransactionSettled", "schema_version": 1,
   "stream_type": "transaction", "stream_id": "0192…", "stream_version": 3,
   "account_id": "0192…", "ledger_id": "0192…", "occurred_at": "2026-11-05T10:00:00.019Z",
   "trace_id": "4bf9…", "payload": {…}}
  ```
  Kafka headers carry `traceparent`, `event_type` and `schema_version`.
- **Relay loop.**
  - The relay is the single leader holding advisory lock `ledger_outbox_relay`.
  - It wakes on `LISTEN ledger_outbox` or every `OUTBOX_POLL_INTERVAL` (default 200ms).
  - Each wake it runs `SELECT … WHERE published_at IS NULL ORDER BY id LIMIT OUTBOX_BATCH_SIZE` (default 500) and produces the batch.
  - After the broker acknowledges with `acks=all`, it sets `UPDATE … SET published_at = now()`.
  - It doesn't keep a cursor, so a row that commits late (a lower `id` committing after a higher one) is still picked up.
- **Ordering.** Two transactions on the same ledger serialize on the row lock, and each inserts its outbox rows while holding it. Their outbox ids are therefore in commit order, and a single relay publishes them in that order. Consumers order and deduplicate on `(stream_id, stream_version)`. They never rely on Kafka offset order across streams.
- **Producer.** `github.com/twmb/franz-go` with idempotent production enabled (the default), `acks=all` and `linger 5ms`.
- **Exactly-once, honestly stated.** A DB commit and a Kafka write can't be atomic. The relay guarantees **at-least-once** publishing: a crash between produce and `UPDATE` re-publishes the batch. Combined with consumer deduplication on `event_id`, this gives **effectively-once processing** (NFR reliability). The ledger itself, which is Postgres, is exactly-once by §7.1.
- **Purge.** Published rows older than `OUTBOX_RETENTION` (default 3 days) are deleted in batches.
- **Implemented in P3.2** (`outbox.Relay`, `repository.OutboxRepo`, the `outbox_relay` and `outbox_purge` jobs in ledger-worker):
  - **Leadership.** `worker.Runner` now pings each leader's lock connection every second and cancels the job's context the moment that fails. The long-running relay therefore stops before another replica can lead. The remaining overlap is at most about one second of duplicates, which consumers drop on `event_id`.
  - **LISTEN.** The relay holds its own connection for `LISTEN`. If that connection fails, it falls back to polling and re-subscribes.
  - **Publish timeout.** Each batch is bounded by `OUTBOX_PUBLISH_TIMEOUT` (default 10 s). franz-go retries delivery indefinitely by default, so without the bound a Kafka outage would block the relay silently. The chaos check found exactly that: 70 s down with no log line.
  - **Failures.** Failed publishes back off from `OUTBOX_POLL_INTERVAL` to 5 s. `outbox.publish` is logged at warn, and at error once failing for over a minute. Batches are never abandoned.
  - **Purge.** It walks the primary key from the oldest row and deletes rows published before the cutoff. It stops at the first row that isn't eligible, so it needs no index on `published_at`, but an unpublished row at the head blocks it until it's relayed.
  - **Startup.** ledger-worker waits for Kafka at startup, logging `dependency.connect` with `dependency=kafka`, as it does for Postgres.

### 7.4 Fraud engine (FR-08, FR-09)

- **Consuming.**
  - Consumer group `fraud-engine` with `isolation.level=read_committed`.
  - Offsets are committed manually, after the event has been processed.
  - Duplicates are dropped with Redis `SET fraud:seen:<event_id> 1 NX EX 604800`.
  - Rules use the event's `occurred_at`, not wall-clock time, so a replay produces the same decisions.

| Rule | Input events | Logic (configurable) | Default |
| :--- | :--- | :--- | :--- |
| `topup_velocity` | `TransactionSettled` with `type=CREDIT` | Redis ZSET `fraud:topup:<account_id>`, score = `occurred_at` ms. Trim to the window, then `ZCARD > N`. | `N=3`, window `5m` |
| `debit_volume` | `TransactionSettled` with `type=DEBIT` | Redis ZSET `fraud:debit:<account_id>:<currency>` (member `tx_id:amount`). Trigger when the window sum > limit or the count > max. | `1h`, limit per currency from config, `max 30` |

- **Rules (implemented in P4.1, `internal/fraud`).**
  - **Input.** The rules read `TransactionSettled`, whose payload now carries `type`, `amount` and `currency`. Voids, other events and earlier settlements without those fields are ignored.
  - **Windows.** A window is half-open, `(end − w, end]`. Because events can arrive out of order, a late event is judged against every window that contains it, including windows ending at later events.
  - **Storage.** Points are stored for 2 × the window, relative to each event's time, and the key expires after 2 × the window idle. An event up to one window late is still judged correctly.
  - **Duplicates.** ZSET members are `<transaction_id>:<amount>`, so a replayed event counts once.
  - **Decision.** It carries the start of the offending window (the earliest event in it). That makes the freeze idempotency key stable across replays.
- **Freezing.** When a rule fires, the engine calls `POST /internal/v1/accounts/{id}/freeze`.
  - Client `fraud-engine`, with `Idempotency-Key: fraud:<rule>:<account_id>:<window_start>`.
  - The trace continues from the `traceparent` header of the triggering event: same `trace_id`, new span.
  - The call is retried with backoff for up to `FRAUD_FREEZE_RETRY` (default 30s). After that the engine emits `fraud.decision` with outcome `failure` and **does not commit the offset**, so the event is re-evaluated.
- **Latency target.** p99 under 2s from the triggering commit to the freeze commit: relay wake + consume + one API call. Debits after the freeze commit are blocked strictly (§7.1). Debits that settle inside that window are an accepted risk of asynchronous detection and appear in the `AccountFrozen` audit context.
- **Implemented in P4.2** (`cmd/fraud-engine`, `fraud.Engine`, `fraud.HTTPFreezer`):
  - **Batches.** A polled batch is processed in order and committed only when every event in it is handled.
  - **Dedupe.** `fraud:seen:<event_id>` is checked first and set only after an event is fully handled. A crash between the freeze and the mark replays an idempotent freeze rather than losing one.
  - **Idempotency key.** It is `fraud:<rule>:<account_id>:<window_start_ms>`, with the rule's `_` written as `-`, because `Idempotency-Key` allows only `[A-Za-z0-9._:-]`.
  - **Tracing.** The engine logs each event with the event's `trace_id` and a new `span_id`, and sends the same pair as `traceparent`. `AccountFrozen` therefore carries the triggering credit's `trace_id`.
  - **Failures.**
    - *Transient* (network, 5xx, 409, 429): retried with backoff. Each `FRAUD_FREEZE_RETRY` that passes logs `fraud.decision` `failure` `freeze_call_failed`, and the event is never skipped.
    - *Permanent* (other 4xx, e.g. the token or its `freeze` grant): logged as `freeze_rejected` and skipped, because retrying can't fix them. Both trigger the "fraud freeze failing" alert.
  - **Dependencies.** Redis and Kafka errors are retried until they succeed (`dependency.call`).
  - **Measured.** Commit → freeze took p50 about 22 ms and max about 22 ms over 10 accounts in the integration test. `fraud.decision` logs `detect_ms`.

### 7.5 Stuck-transaction sweeper

- Every `LEDGER_SWEEP_INTERVAL` (default 5s), the sweeper runs:
  ```sql
  SELECT id FROM transactions
  WHERE status = 'PENDING' AND created_at < now() - LEDGER_SWEEP_AFTER
  ORDER BY created_at
  FOR UPDATE SKIP LOCKED
  LIMIT 100
  ```
  `LEDGER_SWEEP_AFTER` defaults to 5s. For each row it re-runs Tx2.
- After `LEDGER_PENDING_MAX_AGE` (default 15m) the transaction is moved to FAILED `processing_timeout`, still through PROCESSING.
- Each recovery emits `ledger.recover`.
- **Implemented in P2.4** (`cmd/ledger-worker`, `service.Recovery`, `internal/worker`):
  - **Finding the key.** The sweeper lists candidates without locking them. `Process` locks each one and re-checks that it's PENDING, so a transaction the request path finishes in the meantime is skipped quietly. It finds each transaction's idempotency key by `resource_id`, using the index from migration 004. If the key is gone, the transaction is still completed; only the stored response is skipped.
  - **Expiry.** It moves through PROCESSING to FAILED `processing_timeout` without reading or changing a balance. The stored response is a 500 with `processing_timeout`, which a client retry replays.
  - **Config check.** `LEDGER_SWEEP_AFTER` must be at least `LEDGER_PROCESS_TIMEOUT`, so the sweeper never races a Tx2 that's still running.
  - **Leadership.** Each job (`sweeper`, `idempotency_purge`) is led by the holder of a session `pg_advisory_lock`. Followers retry every 5 s. A leader whose connection dies loses the lock, and another replica takes over. Leadership changes log `app.leader`.
  - **Purge.** The idempotency purge (`IDEMPOTENCY_PURGE_INTERVAL`, default 1m) deletes expired keys in batches of 1000 and logs `ledger.purge` with `deleted`.
  - **No Redis.** The worker doesn't use Redis. A response the sweeper completes is cached by ledger-api on its first replay.

### 7.6 Reconciler

Every `RECONCILE_INTERVAL` (default 1m), for ledgers touched since the last watermark, plus a full sweep nightly, the reconciler checks:

1. `ledgers.balance == checkpoint.balance + SUM(journal.amount WHERE ledger_sequence > checkpoint.as_of_sequence)`.
2. `ledger_sequence` is gapless and unique from 1 (or from the checkpoint) to `last_sequence`, and `balance_after` matches the running sum.
3. Each settled transaction has exactly 2 lines summing to 0. `SUM(amount)` over all ledgers per currency is 0.
4. Each transaction projection's `status` and `version` equal the fold of its event stream.

Any mismatch emits `ledger.reconcile` with outcome `failure`, which pages (§11.3). The reconciler never auto-corrects. Fixes go through a VOID or a reviewed rebuild.

**Implementation (P5.1).** The reconciler is the ledger-worker job `reconciler` (`service.Reconciler`, reads in `repository.ReconcileRepo`).
- **Snapshots.** Each batch of 500 ids is read in one `REPEATABLE READ READ ONLY` transaction, so a projection row and the journal lines or events it's compared with come from the same commit point. It takes no locks and doesn't block writes.
- **Incremental runs.**
  - **Scope.** A run checks:
    - transactions with `updated_at` after the watermark (migration 005 indexes it);
    - the USER ledgers those transactions post to;
    - accounts with `updated_at` after the watermark.
  - **Watermark.** The watermark is the previous successful run's start, read from the Postgres clock (the one that stamps `updated_at`, so worker clock skew doesn't matter), minus an overlap: `max(1m, 2 × LEDGER_PROCESS_TIMEOUT)`. `updated_at` is a transaction's start time, so the overlap covers rows committed after a run began. A failed run (`pg_error`) leaves the watermark unchanged.
  - **Verified points.** Per ledger, the worker remembers the last `(sequence, balance)` it found consistent, and incremental runs read only the journal after that point.
- **Full sweep.** It runs on the leader's first run, then every 24 h ("nightly"). It covers:
  - every transaction, USER ledger and account, each ledger from its latest checkpoint;
  - the whole-journal parts of check 3: every currency's lines sum to 0, and no line belongs to a transaction that isn't SETTLED or VOIDED.

  The state lives in the leader's memory, so after a failover the new leader begins with a full sweep.
- **Carried mismatches.** Whatever a run found broken is checked again on every later run until it's clean, so the alert keeps firing until the data is repaired. If a direct write changes a row without touching `updated_at`, the incremental runs don't see it; the next full sweep does.
- **Repair: `ledger-worker rebuild-projections` (P5.2).** This is the reviewed repair path. It recomputes each projection row from the source of truth: accounts and transactions from the fold of their streams, and USER ledgers from the latest checkpoint plus the journal.
  - **What it rebuilds:**
    - accounts: `user_id`, `status`, `version`;
    - transactions: everything the stream determines, including `counterparty_ledger_id` (resolved from the code) and `settled_at` (the `TransactionSettled` event's time, which is Tx2's `now()`);
    - ledgers: `balance`, `last_sequence`.

    `reference` and `metadata` are caller data and are left alone.
  - **Dry run by default.** It logs one `ledger.rebuild` line per differing column and rolls back. It writes only with `--apply`, and `--account <id>` limits it to one account.
  - **Concurrency.** Each row is rebuilt in its own transaction under the lock the write path takes on it (the account's advisory lock plus `FOR UPDATE`, or the transaction or ledger row `FOR UPDATE`). That makes it exact while ledger-api is serving.
  - **What it can't fix.** It never touches events or the journal. A sequence gap or an unbalanced journal can't be "rebuilt" away; it needs a VOID or a migration reviewed by hand. A stream that doesn't fold fails that row with `invalid_stream`.
  - **Exit codes:** 0 nothing differed (or `--apply` wrote everything), 3 a dry run found differences, 1 a row failed.
- **Check 4** covers transactions (status, version, type, amount, currency, account, ledger, failure reason, original, requested_by) and accounts (status, version, user, and the set of USER ledgers), each folded with `ledger.FoldTransaction` / `FoldAccount`. A payload that doesn't decode, or a stream that doesn't fold, is itself `projection_drift`.

### 7.7 Partition maintenance

The worker keeps monthly partitions created at least 3 months ahead for `journal_entries` and `events`, each with its local unique indexes (§5.1). If fewer than 2 future partitions exist, it logs `ledger.partition` with outcome `failure`, which raises an alert. There is no DEFAULT partition, because one would make adding partitions later require a scan.

**Implementation (P5.3).** The ledger-worker job `partitions` (`service.Partitions`, hourly) works by the UTC month.
- **Creating.** It calls `ledger.ensure_partitions(<current month>, 4)` from migration 002. That function is idempotent and adds each partition's local unique index and TRUNCATE guard.
- **Counting.** It then counts, per table, the attached partitions whose lower bound is at or after next month, read from `pg_inherits` and the partition bounds.
- **Logging.** If the smaller count is under 2, it logs `insufficient_future_partitions` with the `table`. If creating failed while enough months remain, it logs `pg_error`. Otherwise it logs a success with `created` and `future_months`.
- **A pitfall.** A non-partition table squatting on a partition's name makes `ensure_partitions` skip that month, which this check catches.

### 7.8 Retention and archival (NFR data retention)

For each monthly partition older than `LEDGER_ARCHIVE_AFTER` (default 7 years), the archiver, as `ledger_archiver`:

1. Writes `checkpoints` rows: the balance at the last `ledger_sequence` inside the partition, for every ledger with lines in it.
2. Exports the partition to Parquet at `s3://<bucket>/ledger/<table>/<yyyy-mm>/part-*.parquet`, with a `manifest.json` holding the row count, SHA-256 per file, min/max ids and the checkpoint ids. The bucket has **Object Lock in compliance mode**, or a GCS bucket lock, with indefinite retention.
3. Reads the export back and verifies the row count and checksums.
4. Runs `ALTER TABLE … DETACH PARTITION … CONCURRENTLY`, then drops it. This is the only path that removes ledger rows. It is DDL by a dedicated role, never a DELETE, and every step emits `ledger.archive`.
5. Honors `LEDGER_LEGAL_HOLD` (a list of account ids or a global flag). A partition containing a held account is skipped.

After archival, the fold is `checkpoint + later lines`. Reconciler check 1 already uses that form.

**Implementation (P6.1).** The archiver is the ledger-worker job `archiver` (`service.Archiver`, daily; Postgres access in `repository.ArchiveRepo`; storage in `internal/archive`). §16 Q8 was answered with these defaults: 7 years from `created_at`, legal hold from config.
- **Eligibility.** A monthly partition of `journal_entries` or `events` is eligible once its range ended at least `LEDGER_ARCHIVE_AFTER` ago. Months go oldest first, journal before events. If any step fails, the run stops, so later months wait.
- **Before anything.** The bucket must have Object Lock enabled in **compliance** mode with a default retention (`archive.S3.CheckWORM`); otherwise the run logs `bucket_not_worm` and archives nothing. With `ARCHIVE_CREATE_BUCKET=true` (local stacks only), a missing bucket is created that way, with `ARCHIVE_RETENTION_DAYS` of retention. In production, use a pre-made bucket with indefinite (e.g. 36500-day) retention.
- **Legal hold.** `LEDGER_LEGAL_HOLD` is a comma-separated list of account ids, or `*`. A month with a held account's journal lines (via its USER ledgers) or events is skipped, and logged as `denied` / `legal_hold` with `held_accounts`. `*` archives nothing.
- **The steps.** Each step is logged as `ledger.archive` with its `step`.
  1. `checkpoint`: for every ledger with sequenced lines in the journal partition, record its balance at the last sequence inside it (idempotent). SYSTEM ledgers get no checkpoint; nothing reads their journal sum, and every currency's journal still sums to 0 because a transaction's two lines share a partition.
  2. `export`: write Parquet files (zstd, at most 100,000 rows each) to `<bucket>/ledger/<table>/<yyyy-mm>/part-NNNN.parquet`, then `manifest.json` last. The manifest holds the row count, min and max id, and the checkpoints, plus key, URI, rows, bytes, SHA-256 and id range for each file.
  3. `verify`: read the manifest and every file back. Check each SHA-256, decode each file and match its row count and id range, and match the total against the manifest and against `count(*)` of the partition. Then record the month in `ledger.archives` as `verified` (migration 006).
  4. `detach`: run `DETACH PARTITION … CONCURRENTLY`, or `FINALIZE` an interrupted detach.
  5. `drop`: drop the table and mark the month `dropped`, in one transaction.
- **Resuming.** A month left `verified` (for example, the worker died before the detach) resumes at the detach on the next run, without exporting again.
- **The reconciler and rebuild.** Archived rows are proven by their manifest and read-back, not re-checked.
  - **Check 3:** skipped for a transaction whose `settled_at` falls in an archived journal month.
  - **Check 4:** skipped for transactions and accounts created before the end of the latest archived events month, because their streams may have lost early events.
  - **`rebuild-projections`:** skips those rows and reports them as `skipped_archived`.
  - **Ledgers:** checks 1 and 2 and the rebuild start from the latest checkpoint.
- **Departures from the design above.**
  - **No dedicated role.** The archiver connects with the worker's own role, not a separate `ledger_archiver`. Detaching and dropping need ownership of the partitioned tables, and locally one role owns everything. In production, run the worker's archiver with a role that owns `journal_entries` and `events`, and keep ledger-api's role DML-only.
  - **Local store.** The compose stand-in for S3 is the Versity S3 gateway (`versity/versitygw`, posix backend with a versioning directory), because MinIO no longer publishes container images. It enforces compliance-mode retention: a locked object version can't be deleted, even with the governance bypass.

---

## 8. Traceability (FR-05)

- **Shared change to `api.RequestContext`.** When no valid `traceparent` arrives, generate one: a random 16-byte trace id and an 8-byte span id. Echo it as a `traceparent` response header. Today the middleware only extracts trace ids. This change is required and also benefits auth-api.
- `trace_id` is written to `transactions`, `events` and `journal_entries`, and to every outbox row and Kafka header.
- The sweeper and reconciler reuse the transaction's stored `trace_id` in their log context, so a recovered transaction's logs join its original trace.
- The fraud engine continues the trace from the event header into the freeze call.
- **Lookup.** In Grafana, `{service=~"ledger-.*|fraud-engine"} | json | trace_id="…"` shows the whole lifecycle across the three services. In SQL, `SELECT … FROM ledger.events WHERE trace_id = …`.

---

## 9. Code layout

```
cmd/
  ledger-api/main.go        # config, logger, pgxpool, redis, router, graceful shutdown, healthcheck
  ledger-worker/main.go     # relay | sweeper | reconciler | partitions | archiver | purge (advisory-lock leaders)
  fraud-engine/main.go
internal/
  ledger/                   # pure domain, no I/O: Money, types, events, Apply/Fold, state machine
  idempotency/              # fingerprint (canonical JSON), Redis cache
  outbox/                   # envelope encoding, relay loop
  kafka/                    # franz-go producer/consumer wrappers
  fraud/                    # rules (pure, taking a WindowStore interface), evaluator
  repository/ledger_*.go    # pgx: accounts, ledgers, transactions, journal, events, idempotency, outbox
  service/ledger.go         # LedgerService: Open, Credit, Debit, Void, Freeze, Unfreeze, Process(Tx2)
  service/reconcile.go
  api/ledger_router.go      # NewLedgerRouter(LedgerDeps)
  api/ledger_handlers.go
  api/client_auth.go        # ClientAuth middleware
migrations/002_ledger.up.sql, 002_ledger.down.sql
deploy/grafana/dashboards/ledger.json
deploy/grafana/provisioning/alerting/ledger.yaml
```

- `Process(ctx, txID)` is the single Tx2 implementation, shared by the request path and the sweeper.
- `internal/startup` holds what every `cmd/*` binary shares: `Retry` (emits `dependency.connect`), `ConnectPostgres`, `Serve` (graceful shutdown, `app.lifecycle`) and `Healthcheck`. HTTP services share one base engine (middleware chain, JSON 404/405, `/healthz`, `/readyz`).
- `ClientAuth` doesn't add `client_id` to the request logger: the service already puts it on its events, and `AccessLog` adds it to the request record, so no record carries the key twice.
- In-transaction operations are the `repository.LedgerTx` interface; `LedgerRepo.InTx(ctx, timeout, fn)` runs `fn` in one READ COMMITTED transaction with `lock_timeout`/`statement_timeout` set, and returns `repository.ErrTimeout` when either fires. The interface lives in `repository` (not `service`) because the callback's parameter type must be nameable by both and `service` already imports `repository`. The service declares a `LedgerStore` interface with `InTx` and the pool-level reads, so service tests can use fakes, as the existing tests do.
- `LedgerTx.AppendEvent` writes the `events` row, its `outbox` row and the `NOTIFY` together, so no transition can be recorded without being published (FR-07).
- New dependencies: `github.com/google/uuid` (UUIDv7), `github.com/twmb/franz-go`, and `github.com/parquet-go/parquet-go` (archiver, P5) with an S3 client.

---

## 10. Non-functional requirements

| ID | Requirement | Target | How it is met | How it is verified |
| :--- | :--- | :--- | :--- | :--- |
| NFR-1 | Throughput | ≥ 300 TPS sustained mixed writes; 600 TPS for 5 min | Stateless api replicas, two short PG transactions per request, no locked hotspot rows (system ledgers unlocked), pgxpool (`max_conns` 32 per replica) | k6 test (§13.4) |
| NFR-2 | Write latency | p99 ≤ 150 ms; p50 ≤ 30 ms at 300 TPS | Single-row locks, indexes on every lock path, `LISTEN/NOTIFY` keeps the relay off the hot path | `http.request` `latency_ms` on ledger routes |
| NFR-3 | Read latency | Balance p99 ≤ 30 ms | Primary-key read of the `ledgers` projection on the **primary**, for read-your-writes. History can go to a read replica. | Same |
| NFR-4 | Consistency | No double-spend; balance ≥ floor at all times | §7.1 locking plus the CHECK constraint; CP rather than AP: a PG outage returns 503, never accepts unsafely | Concurrency integration tests (§13.3); reconciler |
| NFR-5 | Success ratio | ≥ 99.99% | Formula below | Dashboard and alert (§11.3) |
| NFR-6 | Durability | RPO 0 for committed writes | `synchronous_commit=on` and a synchronous standby in prod; Kafka `acks=all`, RF 3, `min.insync.replicas=2`; Tx1 makes requests durable before processing | Chaos tests (§13.5) |
| NFR-7 | Recovery | No transaction PENDING > 60 s while PG is healthy | Sweeper (§7.5) | `ledger.recover` and the stuck-pending alert |
| NFR-8 | Event delivery | Outbox lag p99 < 1 s; alert at 5 s | Relay (§7.3) | `outbox.lag` |
| NFR-9 | Retention | Hot ≤ 7 years in PG; then indefinitely in WORM storage | §7.8 | Archive drill in P5 |
| NFR-10 | Horizontal scaling | Path to 10× without redesign | (1) api, worker and fraud replicas scale out; (2) a read replica for history and reconciler reads; (3) monthly partitions keep indexes bounded; (4) the next step is sharding by `account_id` (e.g. Citus), which works because a transaction touches one user ledger and system ledgers can be replicated per shard | Design review |

**Success ratio.** `1 − (system failures / valid requests)` over 5 minutes, where:

- **Valid requests** are mutating requests that pass authentication and validation.
- **System failures** are 5xx responses plus transactions that end `FAILED processing_timeout`.

Business outcomes such as `insufficient_funds`, `account_frozen` and `not_voidable` are correct results and count as successes.

---

## 11. Observability

Everything follows `.claude/rules/observability-logging.md`. That rule's domain list gains `ledger`, `fraud` and `outbox`, and the catalog rows below are added to the rule **in the P1 PR**.

New allowed log fields:
- `account_id`, `ledger_id`, `transaction_id`, `amount`, `currency`, `client_id`/`actor`, `rule`
- `pending` and `lag_ms` (outbox)

Never log: `reference`, `metadata`, the raw `Idempotency-Key`, service tokens or `Authorization` headers.

### 11.1 Event catalog additions

| `event` | Class | Level by outcome | Allowed `reason` values | Emitted in |
| :--- | :--- | :--- | :--- | :--- |
| `ledger.client_auth` | C1 | denied=warn | `missing_token`, `invalid_token`, `forbidden_operation` | `api.ClientAuth` |
| `ledger.account_open` | C2 | success=info, denied=warn, failure=error | `invalid_input`, `user_not_found`, `account_not_found`, `currency_unsupported`, `ledger_exists`, `pg_error` | `LedgerService.Open*`, `EnsureSystemLedgers` |
| `ledger.credit` / `ledger.debit` / `ledger.void` | C2 | success=info, denied=warn, failure=error, partial=error | `invalid_input`, `currency_unsupported`, `insufficient_funds`, `account_frozen`, `not_voidable`, `ledger_not_found`, `account_not_found`, `pg_error`, `processing_timeout`; partial: `tx2_timeout`, `tx2_error` (Tx1 committed, Tx2 did not: 202, left to the sweeper) | `LedgerService.Process` (one event per terminal state); validation and Tx1 outcomes in `Credit`/`Debit` |
| `ledger.idempotency` | C1 | success=info (replay; `source` = `cache` or `postgres`), denied=warn | `key_reused`, `in_progress` | `LedgerService` (Redis fast path, or the claim in Tx1) |
| `dependency.call` | C4 | failure=warn | `timeout`, `refused`, `unknown` | `LedgerService` idempotency cache (`dependency=redis`, `operation`); the request continues on Postgres |
| `ledger.freeze` / `ledger.unfreeze` | C1 | success=info, denied=warn, failure=error | denied: `invalid_input`, `account_not_found`; failure: `pg_error`, `timeout` | `LedgerService.Freeze/Unfreeze` (includes `actor`, `rule`) |
| `ledger.recover` | C3 | success=warn, failure=error | `stuck_pending`, `processing_timeout`; failure: `pg_error`, `timeout` | sweeper |
| `ledger.purge` | C2 | success=info, failure=error | `pg_error` | idempotency purge |
| `app.leader` | C5 | success=info, failure=warn | `pg_error`, `connection_lost` | `worker.Runner` |
| `ledger.reconcile` | C3 | success=info (summary), failure=error | `balance_mismatch`, `sequence_gap`, `unbalanced_transaction`, `projection_drift`; run level: `mismatches_found`, `pg_error` | reconciler (one event per mismatch, with `check`, `field`, `expected`, `actual`; at most 100 per run, then a summary) |
| `ledger.partition` | C5 | success=info, failure=error | `insufficient_future_partitions`, `pg_error` | partitions job |
| `ledger.archive` | C2 | success=info, denied=warn (`legal_hold`), failure=error | `legal_hold`; `bucket_not_worm`, `bucket_unavailable`, `export_failed`, `verify_failed`, `detach_failed`, `pg_error` | archiver (one event per step, with `step`) |
| `outbox.publish` | C4 | failure=warn (retrying) / error (batch abandoned) | `kafka_unavailable`, `kafka_rejected`, `pg_error` | relay |
| `outbox.lag` | C4 | info; warn when `lag_ms` > 5000 | — | relay, every 30 s (`pending`, `lag_ms`) |
| `fraud.decision` | C1 | success=warn (freeze applied), failure=error | `freeze_call_failed` | fraud engine (includes `rule`, `account_id`) |
| `dependency.connect` | C4 | as existing | adds `dependency=kafka` | all new `cmd/*` |

### 11.2 Dashboard `deploy/grafana/dashboards/ledger.json`

Panels (complete as of P3.3, in rows Overview, Write path, Account controls and recovery, Event streaming, Logs; plus an alert list):
- TPS by `event` and `outcome`
- Success ratio (NFR-5)
- Denials by `reason`
- Write latency p50/p99 from `http.request` on `/internal/v1/*`
- Outbox `lag_ms` and `pending`
- PENDING recoveries
- Reconcile failures by `reason`
- Freezes by `rule`
- Kafka publish failures

Example:
```logql
sum by (outcome) (rate({service="ledger-api"} | json | event=~"ledger\\.(credit|debit|void)" [1m]))
```

### 11.3 Alerts

P3 adds `deploy/grafana/provisioning/alerting/ledger.yaml`: folder `ledger`, group `ledger`, evaluated every 30 s.
- **Severity.** Rules carry `severity` (`page` or `ticket`) and `service` labels. No contact point is provisioned; route on `severity` in each environment's notification policy.
- **Implemented in P3.3:** success ratio, outbox lag and stuck PENDING.
  - **Success ratio:** the §11 formula over ledger-api access logs (POST `/internal/v1/*`; valid means not 400, 401 or 403) plus ledger-worker `processing_timeout`. No traffic means `OK`.
  - **Outbox lag:** `max_over_time(lag_ms)`. `outbox.lag` doubles as the relay's heartbeat, so no data for 2 m is `Alerting`.
  - **Stuck PENDING:** any `ledger.recover` with `processing_timeout`.
  - **P4.2:** "fraud freeze failing" is `fraud.decision` failures > 0 in 5 m (page).
  - **P5.1:** "reconcile failure" is any `ledger.reconcile` failure in 5 m (page). This includes `pg_error`, because a run that can't complete proves nothing.
  - **P5.3:** "partitions running out" is any `ledger.partition` failure in 2 h (ticket). The job runs hourly.
  - **P6.1:** "archiver failing" is any `ledger.archive` failure in 25 h (ticket). The job runs daily. A legal-hold skip is `denied`, so it doesn't fire.
- **Verification.** `make check-alerts` (`scripts/check-alerts.sh`) checks four things:
  - the rules are provisioned;
  - every dashboard query runs on Loki;
  - every rule is quiet on a healthy stack;
  - each rule goes Pending/Firing on synthetic log lines.

| Alert | Condition | Severity |
| :--- | :--- | :--- |
| Ledger reconcile failure | any `event="ledger.reconcile", outcome="failure"` in 5 m | page |
| Success ratio breach | NFR-5 < 99.99 % over 5 m | page |
| Outbox lag | `lag_ms` > 5000 for 2 m | page |
| Stuck PENDING | `ledger.recover` with `processing_timeout` > 0 in 10 m | page |
| Partitions running out | `ledger.partition` failure | ticket |
| Fraud freeze failing | `fraud.decision` failure > 0 in 5 m | page |

Loki labels stay `level` and `service` only. Each new container sets `SERVICE_NAME` and the `logging: alloy` label, so `deploy/alloy/config.alloy` doesn't change.

---

## 12. Deployment

### 12.1 docker-compose additions

| Service | Image | Notes |
| :--- | :--- | :--- |
| `kafka` | `apache/kafka:4.1.0` | KRaft single node, `auto.create.topics.enable=false`, healthcheck, `logging: alloy` |
| `kafka-init` | same image, one-shot | Creates `ledger.events.v1` with 12 partitions, RF 1 and 7-day retention (`--if-not-exists`, so it's safe to re-run) |
| `ledger-api` | build with `CMD=ledger-api` | Port `8081`, `read_only`, `no-new-privileges`, depends on postgres, redis and kafka-init |
| `ledger-worker` | build with `CMD=ledger-worker` | — |
| `fraud-engine` | build with `CMD=fraud-engine` | No published port; `/healthz` and `/readyz` on `:8083`; depends on redis, kafka-init and ledger-api. Its token is the `fraud-engine` entry in `LEDGER_CLIENTS` (`freeze` only) |

- **Migrations (P5.4, §16 Q11).** The one-shot `migrate` service (`migrate/migrate:v4.20.1`) runs `up` over `./migrations`, and every service that uses Postgres depends on it completing. Postgres no longer mounts init scripts. `migrate-baseline` (the postgres image running `deploy/migrate/baseline.sh`) runs first: on a database created by the old init scripts it records the applied version (1–4) in `schema_migrations`; otherwise it does nothing.
- **System ledgers for every configured currency.** Migration 002 seeds `topup_clearing` and `settlement` for `IDR` only. `LedgerService.EnsureSystemLedgers` creates any missing ones for `LEDGER_CURRENCIES` at `ledger-api` startup; it's idempotent and safe across replicas.
- New Makefile targets: `test-integration`, `load` (k6), `kafka-topics`, `logs-ledger`.
- **Kafka listeners (P3.1).** Containers use `kafka:9092`. The host uses `127.0.0.1:9094` (the `HOST` listener), which is what integration tests reach through `TEST_KAFKA_BROKERS`. The controller listens on `:9093`, internal only. No binary depends on `kafka-init` until the relay in P3.2 does (`ledger-worker`).

### 12.2 Configuration (env)

| Var | Default | Binary |
| :--- | :--- | :--- |
| `LEDGER_HTTP_ADDR` | `:8081` | api |
| `LEDGER_CLIENTS` | — (required). Format `client_id:sha256hex:op,op;…` | api |
| `LEDGER_OVERDRAFT_CLIENTS` | empty | api |
| `LEDGER_UNFREEZE_CLIENTS` | `ops` | api |
| `LEDGER_CURRENCIES` | `IDR` | api, worker |
| `LEDGER_MAX_AMOUNT` | `1000000000000` | api |
| `LEDGER_PROCESS_TIMEOUT` | `2s` | api, worker |
| `IDEMPOTENCY_TTL` | `24h` | api, worker |
| `KAFKA_BROKERS` | `kafka:9092` (compose; `localhost:9094` outside it) | worker, fraud |
| `KAFKA_TOPIC_EVENTS` | `ledger.events.v1` | worker, fraud |
| `OUTBOX_POLL_INTERVAL` / `OUTBOX_BATCH_SIZE` / `OUTBOX_RETENTION` | `200ms` / `500` / `72h` | worker |
| `LEDGER_SWEEP_INTERVAL` / `LEDGER_SWEEP_AFTER` / `LEDGER_PENDING_MAX_AGE` | `5s` / `5s` / `15m` | worker |
| `IDEMPOTENCY_PURGE_INTERVAL` / `WORKER_HTTP_ADDR` | `1m` / `:8082` | worker |
| `OUTBOX_PUBLISH_TIMEOUT` | `10s` | worker |
| `RECONCILE_INTERVAL` | `1m` | worker |
| `LEDGER_ARCHIVE_AFTER` / `ARCHIVE_BUCKET_URI` / `LEDGER_LEGAL_HOLD` | `61320h` (7 years) / — (archiver off) / empty (account ids, or `*`) | worker |
| `ARCHIVE_S3_ENDPOINT` / `ARCHIVE_S3_ACCESS_KEY` / `ARCHIVE_S3_SECRET_KEY` / `ARCHIVE_S3_REGION` / `ARCHIVE_S3_SECURE` | — / — / — / `us-east-1` / `true` (compose: `archive-store:7070`, dev keys, `false`) | worker |
| `ARCHIVE_CREATE_BUCKET` / `ARCHIVE_RETENTION_DAYS` | `false` / `36500` (compose: `true` / `1`) | worker |
| `LEDGER_API_URL` / `FRAUD_CLIENT_TOKEN` | `http://ledger-api:8081` / — | fraud |
| `FRAUD_TOPUP_MAX` / `FRAUD_TOPUP_WINDOW` | `3` / `5m` | fraud |
| `FRAUD_DEBIT_WINDOW` / `FRAUD_DEBIT_MAX_COUNT` / `FRAUD_DEBIT_LIMITS` | `1h` / `30` / `IDR:50000000` | fraud |
| `FRAUD_FREEZE_RETRY` | `30s` | fraud |
| `FRAUD_CONSUMER_GROUP` / `FRAUD_HTTP_ADDR` | `fraud-engine` / `:8083` | fraud |

Plus the existing `SERVICE_NAME`, `DATABASE_URL`, `REDIS_*`, `LOG_LEVEL`, `TRUSTED_PROXIES` and `SHUTDOWN_TIMEOUT`. Secrets (`LEDGER_CLIENTS` hashes, tokens) come from `.env` and are never logged.

---

## 13. Testing

All tests run with `-race`. Every denied, failure and partial path has a log assertion on `event`, `outcome` and `reason` (Definition of Done item 5).

### 13.1 Unit (`go test ./...`)

- `internal/ledger`:
  - `Money` arithmetic and overflow.
  - `Apply` and `Fold` for every legal transition, and `ErrIllegalTransition` for every illegal one (table-driven).
  - Fold of a generated event list equals the expected projection (property test with `testing/quick`).
- `internal/idempotency`: canonical fingerprint stability, so key order and whitespace don't change the hash.
- `internal/fraud`: window logic using a fake `WindowStore` and event-time ordering, including out-of-order and duplicate events.

### 13.2 Service and API (fakes and miniredis, the existing harness style)

- Every row of §6.4 and §6.5.
- Redis down → writes still succeed through PG idempotency, with a `dependency` warn log.
- `ClientAuth` 401 and 403.
- A user can't read another user's transaction (404).

### 13.3 Integration (`//go:build integration`, compose Postgres and Kafka)

| Test | Expectation |
| :--- | :--- |
| Append-only guard | UPDATE, DELETE and TRUNCATE on `events` and `journal_entries` raise errors |
| Double-spend | Balance 1,000; 200 concurrent debits of 10 → exactly 100 SETTLED and 100 FAILED `insufficient_funds`; final balance 0; `ledger_sequence` gapless 1..100 |
| Idempotency race | 50 concurrent requests with the same key → 1 transaction; the others get a replay or 409 |
| Void race | 10 concurrent voids of one original → 1 SETTLED; the rest `not_voidable` or 409 |
| Freeze race | Debits running while a freeze commits → no SETTLED debit has a lock-acquisition time after the freeze commit; credits still settle |
| Double-entry | After a random mixed workload, reconciler checks 1–4 pass |
| Sweeper | Kill Tx2 after Tx1 (timeout injection) → the transaction reaches a terminal state within `LEDGER_SWEEP_AFTER + interval` |
| Relay crash | Crash between produce and `UPDATE` → duplicate published; the consumer processes it once |
| Rebuild | `rebuild-projections` from events and journal reproduces the projections exactly |

### 13.4 Load (k6, `make load`)

- 300 TPS mixed load (60% debit, 35% credit, 5% void) for 30 minutes across 10,000 accounts, plus 10 hot accounts at 20 TPS each.
- A 600 TPS spike for 5 minutes.
- Pass criteria:
  - NFR-1, NFR-2, NFR-5 and NFR-8 targets are met.
  - The reconciler reports clean afterwards.

### 13.5 Chaos (manual runbook in P5)

| Fault | Expected behaviour |
| :--- | :--- |
| Stop Redis | Writes continue; idempotency falls back to PG |
| Stop Kafka for 5 minutes | Writes continue; the outbox grows and drains after recovery with no loss |
| Stop PG | 503 `ledger_unavailable`; no partial state; the sweeper finishes any PENDING after recovery |
| Kill `ledger-worker` mid-batch | A new leader takes over within 10 s |

---

## 14. Delivery plan

Each phase is one `feat/ledger-<phase>` branch from a freshly pulled default branch, with one PR, per `.claude/rules/git-workflow.md`.

The PR-level breakdown (seven phases P0–P6, one GitHub issue each, with stacked PRs) is in the [Development Plan](<Development Plan - Immutable Ledger & Event-Sourced Balance System.md>). Where the two differ, the plan is current.

| Phase | Branch | Deliverables | Exit criteria |
| :--- | :--- | :--- | :--- |
| P1 Core ledger | `feat/ledger-core` | Migration 002 (schema, guards, partitions, seeded system ledgers); `internal/ledger`; repositories; `LedgerService` Open/Credit/Debit/Process; `ClientAuth`; user read endpoints; `RequestContext` trace generation; logging catalog update | §13.1, §13.2 and the double-spend and append-only integration tests pass |
| P2 Idempotency, void, freeze | `feat/ledger-void-freeze` | Full §6.4; Void; Freeze/Unfreeze; sweeper | Idempotency, void and freeze race tests pass |
| P3 Events | `feat/ledger-outbox-kafka` | Outbox, relay, Kafka in compose, `ledger.json` dashboard, alert provisioning | Relay crash test passes; lag < 1 s at 300 TPS |
| P4 Fraud | `feat/fraud-engine` | Fraud engine with both rules and the freeze integration | End-to-end: 4 top-ups in 5 minutes → frozen within 2 s p99, then a debit is denied `account_frozen` |
| P5 Hardening | `feat/ledger-ops` | Reconciler, partition job, archiver with checkpoints, k6 load test, chaos runbook | §13.4 passes; archive drill completes on a backdated partition |

---

## 15. Requirements traceability

| BRD item | Satisfied by |
| :--- | :--- |
| §3 1:1 account mapping, sub-ledgers | §4.2, `accounts.user_id UNIQUE`, `ledgers_user_currency_key` |
| §3 Immutable append-only ledger | §5 guard triggers, §5.2 grants |
| §3 Correction via voiding | §4.3 VOID lines, §7.1, `transactions_single_void_key` |
| §3 Event sourcing / left-fold | §3.2, §4.4–4.5, `ledger.Fold`, reconciler check 4, `rebuild-projections` |
| §3 Idempotency by design | §6.4, `idempotency_keys`, Redis cache |
| §4 States | §4.4 state machine, §4.5 events |
| FR-01 CREDIT / DEBIT | §6.2, §7.1 |
| FR-02 No negative balance / overdraft | §7.1 check under lock, `ledgers_no_negative`, `overdraft_limit`, §16 Q2 |
| FR-03 Real-time balance from journal, projections | §3.2, §7.6 check 1, NFR-3 |
| FR-04 VOID | §6.2, §7.1, §4.3 |
| FR-05 trace_id | §8 |
| FR-06 Idempotency against cache and storage | §6.4, §7.1 (Redis, then PG, before any state transition) |
| FR-07 Publish every transition | §7.1 (outbox row per event), §7.3 |
| FR-08 Fraud velocity rules | §7.4 |
| FR-09 FREEZE blocks DEBIT, allows CREDIT | §7.1 lock semantics, §7.2, §7.4 |
| NFR throughput & scalability | NFR-1, NFR-10 |
| NFR consistency | NFR-4, §7.1 |
| NFR reliability / exactly-once | NFR-5–7, §7.1, §7.3 (effectively-once, stated explicitly) |
| NFR data retention | §7.8, NFR-9 |

---

## 16. Open questions

Each question has a default the design uses until it is answered.

| # | Question | Default in this TRD |
| :--- | :--- | :--- |
| Q1 | How do internal callers authenticate in production: mTLS, gateway-issued JWT, or service tokens? | Hashed service tokens plus network isolation; mTLS in production |
| Q2 | Which overdraft products exist, who approves them, and how is `overdraft_limit` changed after opening? | Fixed at ledger open by allow-listed clients only; changing it is out of scope for v1 |
| Q3 | Can a void of a CREDIT push the balance below the floor (e.g. a fraudulent top-up already spent)? Are voids allowed on frozen accounts? | Void must respect the floor (FAILED `insufficient_funds`); voids allowed on frozen accounts |
| Q4 | Are partial voids or refunds needed? | No; full voids only |
| Q5 | Which currencies, and is each currency's balance independent (no FX)? | `IDR` only; independent |
| Q6 | Who can unfreeze, and does unfreezing need dual control? | `ops` client only; single control |
| Q7 | What are the "abnormal deduction volume" thresholds per segment? | Static per-currency limits from config |
| Q8 | Is the retention clock 7 years from entry date, under which jurisdiction, and what is the legal-hold process? | **Decided (P6): the defaults.** 7 years from `created_at`; legal hold via `LEDGER_LEGAL_HOLD`. Both are configurable (§7.8) |
| Q9 | Is the account opened automatically at user registration (auth-api calls the ledger), or by the onboarding service? | Explicit `POST /internal/v1/accounts` by the caller |
| Q10 | Are authorisation holds (reserve now, capture later) needed for any product? | Not in v1 |
| Q11 | Adopt a migration runner now? | **Decided (P5):** golang-migrate as a one-shot compose service (§12.1) |
