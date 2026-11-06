-- 002_ledger.down.sql
-- Drops the whole ledger schema, including all journal and event data. The
-- append-only triggers guard DML only; this DDL is the deliberate escape hatch
-- for development. Never run it against an environment with real balances.
BEGIN;

DROP SCHEMA IF EXISTS ledger CASCADE;

COMMIT;
