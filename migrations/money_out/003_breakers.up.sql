-- Circuit breakers per (bank_middleware, rail) (P§3.3, spec/money-out TRD
-- §5, §6.3; plan M4.3). Shared by every MOM instance: changed under a row
-- lock with a version check, each transition journaled as BreakerChanged.
BEGIN;

CREATE TABLE breakers (
    bank_middleware  TEXT        NOT NULL,
    rail             TEXT        NOT NULL CHECK (rail IN ('BIFAST','RTOL','SKNBI','RTGS','INTRABANK')),
    state            TEXT        NOT NULL DEFAULT 'CLOSED' CHECK (state IN ('CLOSED','OPEN','HALF_OPEN')),
    consecutive      INT         NOT NULL DEFAULT 0 CHECK (consecutive >= 0),
    calls            INT         NOT NULL DEFAULT 0 CHECK (calls >= 0),       -- in the window
    failures         INT         NOT NULL DEFAULT 0 CHECK (failures >= 0),    -- in the window
    window_start     TIMESTAMPTZ NOT NULL DEFAULT now(),
    opened_at        TIMESTAMPTZ,
    probes_in_flight INT         NOT NULL DEFAULT 0 CHECK (probes_in_flight >= 0),
    probe_at         TIMESTAMPTZ,
    version          BIGINT      NOT NULL DEFAULT 0 CHECK (version >= 0),     -- = the BreakerChanged stream_version
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bank_middleware, rail)
);

-- Whether an attempt was a half-open probe: its result closes or reopens.
ALTER TABLE attempts ADD COLUMN breaker_probe BOOLEAN NOT NULL DEFAULT false;

COMMIT;
