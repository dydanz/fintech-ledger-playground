-- 004_idempotency_resource_index.up.sql
-- The sweeper (cmd/ledger-worker, spec/TRD §7.5) finds the idempotency key
-- of each stuck transaction by resource_id, to complete its stored response.
CREATE INDEX idempotency_resource_idx ON ledger.idempotency_keys (resource_id);
