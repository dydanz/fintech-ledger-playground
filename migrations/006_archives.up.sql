-- 006_archives.up.sql
-- What the archiver (cmd/ledger-worker, spec/TRD §7.8) has exported and
-- verified, per partition month. 'verified' means the export was read back
-- and checked and the partition may be detached; 'dropped' means it is gone.
-- The reconciler and rebuild-projections read this to know which journal
-- lines and events are no longer in Postgres.
BEGIN;

CREATE TABLE ledger.archives (
    table_name   TEXT        NOT NULL,
    month        TIMESTAMPTZ NOT NULL,                  -- the partition's lower bound (UTC month start)
    status       TEXT        NOT NULL,
    manifest_uri TEXT        NOT NULL,
    rows         BIGINT      NOT NULL,
    checkpoints  INT         NOT NULL DEFAULT 0,        -- checkpoint rows written for this month (journal only)
    verified_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    dropped_at   TIMESTAMPTZ,
    PRIMARY KEY (table_name, month),
    CONSTRAINT archives_table_valid  CHECK (table_name IN ('journal_entries', 'events')),
    CONSTRAINT archives_status_valid CHECK (status IN ('verified', 'dropped')),
    CONSTRAINT archives_dropped      CHECK ((status = 'dropped') = (dropped_at IS NOT NULL)),
    CONSTRAINT archives_month_start  CHECK (month = date_trunc('month', month AT TIME ZONE 'UTC') AT TIME ZONE 'UTC')
);

COMMIT;
