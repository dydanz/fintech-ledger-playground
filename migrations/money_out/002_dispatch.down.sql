BEGIN;
DROP INDEX IF EXISTS attempts_inquiry_idx;
DROP INDEX IF EXISTS transfers_dispatch_idx;
ALTER TABLE transfers DROP COLUMN IF EXISTS review_reason, DROP COLUMN IF EXISTS next_dispatch_at, DROP COLUMN IF EXISTS same_node_retries, DROP COLUMN IF EXISTS candidate_index;
COMMIT;
