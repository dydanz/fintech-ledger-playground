-- 003_idempotency_response_bytes.up.sql
-- Stored idempotent responses must replay byte for byte (spec/TRD §6.4).
-- JSONB normalises key order and whitespace, so the column becomes bytea.

BEGIN;

ALTER TABLE ledger.idempotency_keys
    ALTER COLUMN response_body TYPE bytea USING convert_to(response_body::text, 'UTF8');

COMMIT;
