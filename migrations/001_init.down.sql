-- 001_init.down.sql
BEGIN;

DROP TABLE IF EXISTS auth_token_history;
DROP FUNCTION IF EXISTS auth_token_history_guard();
DROP TABLE IF EXISTS users;

COMMIT;
