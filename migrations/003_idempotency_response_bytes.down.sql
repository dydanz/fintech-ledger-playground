-- 003_idempotency_response_bytes.down.sql
BEGIN;

ALTER TABLE ledger.idempotency_keys
    ALTER COLUMN response_body TYPE jsonb USING convert_from(response_body, 'UTF8')::jsonb;

COMMIT;
