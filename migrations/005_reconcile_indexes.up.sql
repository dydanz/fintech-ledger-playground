-- 005_reconcile_indexes.up.sql
-- The reconciler (cmd/ledger-worker, spec/TRD §7.6) checks, every
-- RECONCILE_INTERVAL, the transactions and accounts changed since its last
-- run. Ledgers are reached through their transactions' ledger_id.
-- Plain CREATE INDEX locks writes while it builds; on a large production
-- table, create these CONCURRENTLY by hand first (IF NOT EXISTS makes this a no-op).
CREATE INDEX IF NOT EXISTS transactions_updated_idx ON ledger.transactions (updated_at);
CREATE INDEX IF NOT EXISTS accounts_updated_idx ON ledger.accounts (updated_at);
