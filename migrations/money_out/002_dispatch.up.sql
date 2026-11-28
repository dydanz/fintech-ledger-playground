-- Dispatch, retry and failover (spec/money-out TRD §6.2, §6.3; plan M4.2).
-- Additive: where a transfer is in its candidate list, how many same-node
-- retries its current candidate has had, and when it may be dispatched next.
BEGIN;

ALTER TABLE transfers
    ADD COLUMN candidate_index   INT         NOT NULL DEFAULT 0 CHECK (candidate_index >= 0),
    ADD COLUMN same_node_retries INT         NOT NULL DEFAULT 0 CHECK (same_node_retries >= 0),
    ADD COLUMN next_dispatch_at  TIMESTAMPTZ,
    -- Why a transfer is NEEDS_MANUAL_REVIEW: inquiry_deadline may still be
    -- finished by a late answer (Q4); late_event (a possible double payment)
    -- waits for ops (Q3).
    ADD COLUMN review_reason     TEXT        CHECK (review_reason IN ('inquiry_deadline', 'late_event'));

-- The dispatcher's queue, and the inquiry job's.
CREATE INDEX transfers_dispatch_idx ON transfers (next_dispatch_at) WHERE state IN ('ROUTED', 'ATTEMPT_FAILED');
CREATE INDEX attempts_inquiry_idx ON attempts (next_inquiry_at) WHERE status IN ('DISPATCHING', 'ACCEPTED', 'PROCESSING', 'UNKNOWN');

COMMIT;
