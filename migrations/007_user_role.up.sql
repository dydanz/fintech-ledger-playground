-- 007_user_role.up.sql
-- A role per user, for the web console's ops screens (spec/2026-11-10 Web
-- Console Design D§7.5): 'user' by default, 'ops' to read every account.
-- It's granted only with `auth-api grant-role` (make grant-role), never
-- through the API.
BEGIN;

ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'
    CONSTRAINT users_role_valid CHECK (role IN ('user', 'ops'));

COMMIT;
