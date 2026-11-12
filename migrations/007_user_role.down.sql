BEGIN;
DROP TABLE IF EXISTS user_role_changes;
DROP FUNCTION IF EXISTS user_role_changes_guard();
ALTER TABLE users DROP COLUMN IF EXISTS role;
COMMIT;
