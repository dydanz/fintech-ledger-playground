-- 002_ledger.up.sql
-- Immutable ledger & event-sourced balance system (spec/TRD §5).
--
-- Source of truth (append-only, partitioned monthly): ledger.events, ledger.journal_entries.
-- Projections (updated in the same transaction as the event they reflect):
--   ledger.accounts, ledger.ledgers, ledger.transactions.
-- Operational (mutable, purgeable): ledger.idempotency_keys, ledger.outbox, ledger.checkpoints.

BEGIN;

CREATE SCHEMA ledger;

-- ---------------------------------------------------------------- projections

CREATE TABLE ledger.accounts (
    id          UUID        PRIMARY KEY,
    user_id     UUID        NOT NULL REFERENCES public.users (id) ON DELETE RESTRICT,
    status      TEXT        NOT NULL DEFAULT 'ACTIVE',
    version     BIGINT      NOT NULL DEFAULT 0,          -- last account-stream version
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT accounts_user_key       UNIQUE (user_id), -- strict 1 user : 1 account
    CONSTRAINT accounts_status_valid   CHECK (status IN ('ACTIVE', 'FROZEN')),
    CONSTRAINT accounts_version_valid  CHECK (version >= 0)
);

CREATE TABLE ledger.ledgers (
    id              UUID        PRIMARY KEY,
    kind            TEXT        NOT NULL,
    account_id      UUID        REFERENCES ledger.accounts (id) ON DELETE RESTRICT,
    code            TEXT,                                -- SYSTEM only, e.g. 'topup_clearing'
    currency        CHAR(3)     NOT NULL,
    overdraft_limit BIGINT      NOT NULL DEFAULT 0,
    balance         BIGINT      NOT NULL DEFAULT 0,      -- USER only; a SYSTEM balance is SUM(journal)
    last_sequence   BIGINT      NOT NULL DEFAULT 0,      -- USER only; last journal ledger_sequence
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ledgers_kind_valid      CHECK (kind IN ('USER', 'SYSTEM')),
    CONSTRAINT ledgers_currency_format CHECK (currency ~ '^[A-Z]{3}$'),
    CONSTRAINT ledgers_code_format     CHECK (code ~ '^[a-z][a-z0-9_]{0,62}$'),
    CONSTRAINT ledgers_overdraft_valid CHECK (overdraft_limit >= 0),
    CONSTRAINT ledgers_sequence_valid  CHECK (last_sequence >= 0),
    CONSTRAINT ledgers_kind_shape CHECK (
        (kind = 'USER'   AND account_id IS NOT NULL AND code IS NULL) OR
        (kind = 'SYSTEM' AND account_id IS NULL     AND code IS NOT NULL
                         AND balance = 0 AND last_sequence = 0 AND overdraft_limit = 0)),
    -- FR-02 backstop: even a buggy service cannot commit a balance below the overdraft floor.
    CONSTRAINT ledgers_no_negative CHECK (kind = 'SYSTEM' OR balance >= -overdraft_limit),
    -- Target of the composite FKs that pin a journal line's or transaction's currency to its ledger's.
    CONSTRAINT ledgers_id_currency_key UNIQUE (id, currency)
);
CREATE UNIQUE INDEX ledgers_user_currency_key   ON ledger.ledgers (account_id, currency) WHERE kind = 'USER';
CREATE UNIQUE INDEX ledgers_system_currency_key ON ledger.ledgers (code, currency)       WHERE kind = 'SYSTEM';

CREATE TABLE ledger.transactions (
    id                      UUID        PRIMARY KEY,
    account_id              UUID        NOT NULL REFERENCES ledger.accounts (id),
    ledger_id               UUID,                        -- resolved in Tx2; NULL if FAILED ledger_not_found
    counterparty_ledger_id  UUID        NOT NULL,
    type                    TEXT        NOT NULL,
    status                  TEXT        NOT NULL,
    amount                  BIGINT      NOT NULL,
    currency                CHAR(3)     NOT NULL,
    original_transaction_id UUID        REFERENCES ledger.transactions (id),
    failure_reason          TEXT,
    reference               TEXT,
    metadata                JSONB,
    requested_by            TEXT        NOT NULL,        -- client_id of the internal caller
    version                 BIGINT      NOT NULL,        -- last transaction-stream version
    trace_id                CHAR(32)    NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    settled_at              TIMESTAMPTZ,
    CONSTRAINT transactions_ledger_fk FOREIGN KEY (ledger_id, currency)
        REFERENCES ledger.ledgers (id, currency),
    CONSTRAINT transactions_counterparty_fk FOREIGN KEY (counterparty_ledger_id, currency)
        REFERENCES ledger.ledgers (id, currency),
    CONSTRAINT transactions_type_valid      CHECK (type IN ('CREDIT', 'DEBIT', 'VOID')),
    CONSTRAINT transactions_status_valid    CHECK (status IN ('PENDING', 'PROCESSING', 'SETTLED', 'FAILED', 'VOIDED')),
    CONSTRAINT transactions_amount_valid    CHECK (amount > 0),
    CONSTRAINT transactions_version_valid   CHECK (version > 0),
    CONSTRAINT transactions_reference_len   CHECK (char_length(reference) <= 64),
    CONSTRAINT transactions_metadata_valid  CHECK (jsonb_typeof(metadata) = 'object' AND pg_column_size(metadata) <= 4096),
    CONSTRAINT transactions_requested_by_len CHECK (char_length(requested_by) BETWEEN 1 AND 64),
    CONSTRAINT transactions_trace_format    CHECK (trace_id ~ '^[0-9a-f]{32}$'),
    CONSTRAINT transactions_void_has_original CHECK ((type = 'VOID') = (original_transaction_id IS NOT NULL)),
    CONSTRAINT transactions_failure_reason  CHECK ((status = 'FAILED') = (failure_reason IS NOT NULL)),
    CONSTRAINT transactions_settled_at      CHECK ((status IN ('SETTLED', 'VOIDED')) = (settled_at IS NOT NULL))
);
CREATE INDEX transactions_account_created_idx ON ledger.transactions (account_id, created_at DESC, id DESC);
CREATE INDEX transactions_pending_idx         ON ledger.transactions (created_at) WHERE status = 'PENDING';
-- At most one void per original that is in flight or settled. A FAILED void can be retried.
CREATE UNIQUE INDEX transactions_single_void_key ON ledger.transactions (original_transaction_id)
    WHERE type = 'VOID' AND status IN ('PENDING', 'PROCESSING', 'SETTLED');

-- ------------------------------------- source of truth (append-only, monthly partitions)

CREATE TABLE ledger.journal_entries (
    id              UUID        NOT NULL,
    transaction_id  UUID        NOT NULL,
    ledger_id       UUID        NOT NULL,
    amount          BIGINT      NOT NULL,               -- signed; positive increases the ledger's balance
    currency        CHAR(3)     NOT NULL,
    ledger_sequence BIGINT,                             -- USER lines only, gapless per ledger from 1
    balance_after   BIGINT,                             -- USER lines only
    trace_id        CHAR(32)    NOT NULL,
    -- Both lines of a transaction share created_at (one INSERT, transaction-start now()).
    -- journal_balanced_check relies on this to prune partitions.
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, created_at),
    CONSTRAINT journal_ledger_fk FOREIGN KEY (ledger_id, currency) REFERENCES ledger.ledgers (id, currency),
    CONSTRAINT journal_amount_nonzero  CHECK (amount <> 0),
    CONSTRAINT journal_sequence_valid  CHECK (ledger_sequence > 0),
    CONSTRAINT journal_user_fields     CHECK ((ledger_sequence IS NULL) = (balance_after IS NULL)),
    CONSTRAINT journal_trace_format    CHECK (trace_id ~ '^[0-9a-f]{32}$')
) PARTITION BY RANGE (created_at);
CREATE INDEX journal_ledger_seq_idx ON ledger.journal_entries (ledger_id, ledger_sequence);
CREATE INDEX journal_tx_idx         ON ledger.journal_entries (transaction_id);

CREATE TABLE ledger.events (
    event_id       UUID        NOT NULL,
    stream_type    TEXT        NOT NULL,
    stream_id      UUID        NOT NULL,
    stream_version BIGINT      NOT NULL,
    event_type     TEXT        NOT NULL,
    account_id     UUID        NOT NULL,
    schema_version SMALLINT    NOT NULL DEFAULT 1,
    payload        JSONB       NOT NULL,
    trace_id       CHAR(32)    NOT NULL,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, occurred_at),
    CONSTRAINT events_version_valid  CHECK (stream_version > 0),
    CONSTRAINT events_schema_valid   CHECK (schema_version > 0),
    CONSTRAINT events_payload_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT events_trace_format   CHECK (trace_id ~ '^[0-9a-f]{32}$'),
    CONSTRAINT events_type_valid CHECK (
        (stream_type = 'transaction' AND event_type IN (
            'TransactionRequested', 'TransactionProcessing', 'TransactionSettled',
            'TransactionFailed', 'TransactionVoided')) OR
        (stream_type = 'account' AND event_type IN (
            'AccountOpened', 'LedgerOpened', 'AccountFrozen', 'AccountUnfrozen')))
) PARTITION BY RANGE (occurred_at);
CREATE INDEX events_stream_idx ON ledger.events (stream_id, stream_version);

-- Append-only guard. Unlike auth_token_history_guard, no UPDATE is ever allowed.
CREATE FUNCTION ledger.append_only_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '%.% is append-only (% not allowed)', TG_TABLE_SCHEMA, TG_TABLE_NAME, TG_OP;
END;
$$;

-- Row triggers on a partitioned table are cloned to every partition. Statement-level
-- TRUNCATE triggers are not: a TRUNCATE aimed at a partition bypasses the parent's
-- trigger, so ledger.ensure_partitions() adds one to each partition as well.
CREATE TRIGGER journal_entries_guard_row   BEFORE UPDATE OR DELETE ON ledger.journal_entries
    FOR EACH ROW EXECUTE FUNCTION ledger.append_only_guard();
CREATE TRIGGER journal_entries_guard_trunc BEFORE TRUNCATE ON ledger.journal_entries
    FOR EACH STATEMENT EXECUTE FUNCTION ledger.append_only_guard();
CREATE TRIGGER events_guard_row            BEFORE UPDATE OR DELETE ON ledger.events
    FOR EACH ROW EXECUTE FUNCTION ledger.append_only_guard();
CREATE TRIGGER events_guard_trunc          BEFORE TRUNCATE ON ledger.events
    FOR EACH STATEMENT EXECUTE FUNCTION ledger.append_only_guard();

-- Double-entry check at COMMIT: every transaction posts exactly two lines, in one
-- currency, summing to zero. Partition pruning comes from the shared created_at.
-- A stray line inserted later with a different created_at escapes this check; the
-- reconciler (TRD §7.6 check 3) covers that case.
CREATE FUNCTION ledger.journal_balanced_check() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    n_lines    bigint;
    total      numeric;
    currencies bigint;
BEGIN
    SELECT count(*), sum(amount), count(DISTINCT currency)
      INTO n_lines, total, currencies
      FROM ledger.journal_entries
     WHERE transaction_id = NEW.transaction_id
       AND created_at = NEW.created_at;

    IF n_lines <> 2 OR total <> 0 OR currencies <> 1 THEN
        RAISE EXCEPTION 'ledger.journal_entries: transaction % is unbalanced (% lines, sum %, % currencies)',
            NEW.transaction_id, n_lines, total, currencies;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER journal_entries_balanced
    AFTER INSERT ON ledger.journal_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger.journal_balanced_check();

-- Creates the monthly partitions of journal_entries and events for p_months months
-- starting at p_from's month (UTC boundaries), each with its local unique index and
-- TRUNCATE guard. Idempotent. Returns the number of partitions created.
-- Called below for the initial partitions, and by the ledger-worker partition job.
CREATE FUNCTION ledger.ensure_partitions(p_from date, p_months int) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
    month_start date;
    parent      text;
    part        text;
    created     int := 0;
BEGIN
    FOR i IN 0 .. p_months - 1 LOOP
        month_start := (date_trunc('month', p_from) + make_interval(months => i))::date;
        FOREACH parent IN ARRAY ARRAY['journal_entries', 'events'] LOOP
            part := format('%s_y%sm%s', parent, to_char(month_start, 'YYYY'), to_char(month_start, 'MM'));
            CONTINUE WHEN to_regclass(format('ledger.%I', part)) IS NOT NULL;

            EXECUTE format('CREATE TABLE ledger.%I PARTITION OF ledger.%I FOR VALUES FROM (%L) TO (%L)',
                part, parent,
                to_char(month_start, 'YYYY-MM-DD') || ' 00:00:00+00',
                to_char(month_start + interval '1 month', 'YYYY-MM-DD') || ' 00:00:00+00');

            -- Postgres can't enforce these across partitions (the partition key isn't in
            -- them). Global uniqueness comes from assignment under the projection row lock;
            -- these catch duplicates within a month, the reconciler catches the rest.
            IF parent = 'journal_entries' THEN
                EXECUTE format('CREATE UNIQUE INDEX %I ON ledger.%I (ledger_id, ledger_sequence) WHERE ledger_sequence IS NOT NULL',
                    part || '_seq_key', part);
            ELSE
                EXECUTE format('CREATE UNIQUE INDEX %I ON ledger.%I (stream_id, stream_version)',
                    part || '_stream_key', part);
            END IF;

            EXECUTE format('CREATE TRIGGER %I BEFORE TRUNCATE ON ledger.%I FOR EACH STATEMENT EXECUTE FUNCTION ledger.append_only_guard()',
                part || '_guard_trunc', part);
            created := created + 1;
        END LOOP;
    END LOOP;
    RETURN created;
END;
$$;

-- ---------------------------------------------------------------- operational

CREATE TABLE ledger.idempotency_keys (
    client_id           TEXT        NOT NULL,
    key_hash            CHAR(64)    NOT NULL,           -- sha256(Idempotency-Key); the raw key is never stored
    request_fingerprint CHAR(64)    NOT NULL,           -- sha256(method|route|canonical JSON body)
    operation           TEXT        NOT NULL,
    -- What the key produced: the transaction (credit/debit/void), the account
    -- (account_open/freeze/unfreeze) or the ledger (ledger_open).
    resource_id         UUID        NOT NULL,
    response_status     SMALLINT,                       -- NULL while in flight
    response_body       JSONB,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (client_id, key_hash),
    CONSTRAINT idempotency_key_hash_format    CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT idempotency_fingerprint_format CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT idempotency_operation_valid    CHECK (operation IN (
        'account_open', 'ledger_open', 'credit', 'debit', 'void', 'freeze', 'unfreeze')),
    CONSTRAINT idempotency_response_complete  CHECK ((response_status IS NULL) = (response_body IS NULL)),
    CONSTRAINT idempotency_expiry_valid       CHECK (expires_at > created_at)
);
CREATE INDEX idempotency_expires_idx ON ledger.idempotency_keys (expires_at);

CREATE TABLE ledger.outbox (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL,                  -- account_id
    payload      JSONB       NOT NULL,                  -- Kafka envelope (TRD §7.3)
    headers      JSONB       NOT NULL,                  -- traceparent, event_type, schema_version
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON ledger.outbox (id) WHERE published_at IS NULL;

CREATE TABLE ledger.checkpoints (                      -- opening balances for archived ranges (TRD §7.8)
    ledger_id      UUID        NOT NULL REFERENCES ledger.ledgers (id),
    as_of_sequence BIGINT      NOT NULL,
    balance        BIGINT      NOT NULL,
    archive_uri    TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (ledger_id, as_of_sequence)
);

-- ---------------------------------------------------------------- seed data

-- Current month + 3 ahead. Until the P5 partition job runs, extend by hand with
-- SELECT ledger.ensure_partitions((now() AT TIME ZONE 'UTC')::date, 4);
SELECT ledger.ensure_partitions((now() AT TIME ZONE 'UTC')::date, 4);

-- Double-entry counterparties for the default currency (LEDGER_CURRENCIES=IDR).
-- Seeds use v4 UUIDs; PG 17 has no uuidv7(). Application-created rows use v7.
INSERT INTO ledger.ledgers (id, kind, code, currency) VALUES
    (gen_random_uuid(), 'SYSTEM', 'topup_clearing', 'IDR'),
    (gen_random_uuid(), 'SYSTEM', 'settlement',     'IDR');

COMMIT;
