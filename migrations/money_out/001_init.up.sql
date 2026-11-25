-- money-out-middleware's own database (spec/money-out TRD §5). The database
-- holds current state; the full history is the money-out.journal.v1 topic,
-- written through the outbox in the same transaction as each change (P§3.5).
BEGIN;

CREATE TABLE transfers (
    transfer_id         UUID        PRIMARY KEY,               -- the core's id: the idempotency gate (P§3.2)
    payload_hash        TEXT        NOT NULL,                  -- SHA-256 of the canonical command payload
    amount              BIGINT      NOT NULL CHECK (amount > 0),  -- minor units; never a float
    currency            CHAR(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    bank_code           TEXT        NOT NULL CHECK (bank_code ~ '^[0-9]{3}$'),
    account_no          TEXT        NOT NULL CHECK (account_no ~ '^[0-9]{5,34}$'),
    metadata            JSONB,                                 -- passed through untouched
    core_transaction_id TEXT,                                  -- the core's debit (metadata.core_transaction_id), opaque here
    state               TEXT        NOT NULL CHECK (state IN ('RECEIVED','ROUTED','DISPATCHING','ACCEPTED','PROCESSING',
                                        'UNKNOWN','ATTEMPT_FAILED','SUCCEEDED','FAILED','NEEDS_MANUAL_REVIEW')),
    version             BIGINT      NOT NULL DEFAULT 1 CHECK (version >= 1),  -- = the journal stream_version of the last event
    routing             JSONB,                                 -- candidates and why each was kept or dropped (M4)
    current_attempt     INT         NOT NULL DEFAULT 0 CHECK (current_attempt >= 0),
    core_ack            TEXT        NOT NULL DEFAULT 'none' CHECK (core_ack IN ('none','pending','done')),
    trace_id            TEXT        NOT NULL DEFAULT '',
    requested_at        TIMESTAMPTZ NOT NULL,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX transfers_state_idx ON transfers (state, updated_at);

CREATE TABLE attempts (
    transfer_id      UUID        NOT NULL REFERENCES transfers (transfer_id),
    attempt_no       INT         NOT NULL CHECK (attempt_no >= 1),
    request_id       UUID        NOT NULL UNIQUE,
    bank_middleware  TEXT        NOT NULL,
    rail             TEXT        NOT NULL CHECK (rail IN ('BIFAST','RTOL','SKNBI','RTGS','INTRABANK')),
    status           TEXT        NOT NULL CHECK (status IN ('DISPATCHING','ACCEPTED','PROCESSING','UNKNOWN','SUCCEEDED','FAILED')),
    reason           TEXT,
    retry_elsewhere  BOOLEAN     NOT NULL DEFAULT false,
    reached_bank     BOOLEAN     NOT NULL DEFAULT false,
    bank_reference   TEXT,
    bank_raw_code    TEXT,
    next_inquiry_at  TIMESTAMPTZ,
    inquiry_deadline TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (transfer_id, attempt_no),
    CONSTRAINT attempts_failed_has_reason CHECK (status <> 'FAILED' OR reason IS NOT NULL)
);
-- At most one in-flight attempt per transfer (P§3.2), whatever the workers do.
CREATE UNIQUE INDEX attempts_one_open ON attempts (transfer_id)
    WHERE status IN ('DISPATCHING','ACCEPTED','PROCESSING','UNKNOWN');

-- The transactional outbox, as ledger.outbox (TRD L§7.3): the relay publishes
-- in id order, at least once; consumers deduplicate on event_id.
CREATE TABLE outbox (
    id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    topic        TEXT        NOT NULL,
    msg_key      TEXT        NOT NULL,                  -- transfer_id
    payload      JSONB       NOT NULL,                  -- the envelope
    headers      JSONB       NOT NULL,                  -- traceparent, event_type, schema_version
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Consumer deduplication: an event already handled is skipped (P§2.6).
CREATE TABLE processed_events (
    event_id     UUID        PRIMARY KEY,
    topic        TEXT        NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMIT;
