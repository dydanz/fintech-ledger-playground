-- 007_user_role.up.sql
-- A role per user, for the web console's ops screens (spec/2026-11-10 Web
-- Console Design D§7.5): 'user' by default, 'ops' to read every account.
-- It's granted only with `auth-api grant-role` (make grant-role), never
-- through the API. Every change is kept in user_role_changes, append-only,
-- written in the same statement as the change: who ran it, the role before
-- and after.
BEGIN;

ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'
    CONSTRAINT users_role_valid CHECK (role IN ('user', 'ops'));

CREATE TABLE user_role_changes (
    id            BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id       UUID        NOT NULL REFERENCES users (id),
    role          TEXT        NOT NULL,
    previous_role TEXT        NOT NULL,
    changed_by    TEXT        NOT NULL,  -- the operator who ran grant-role (--by)
    changed_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT user_role_changes_role_valid     CHECK (role IN ('user', 'ops')),
    CONSTRAINT user_role_changes_previous_valid CHECK (previous_role IN ('user', 'ops')),
    CONSTRAINT user_role_changes_by_len         CHECK (char_length(changed_by) BETWEEN 1 AND 100)
);

CREATE INDEX user_role_changes_user_idx ON user_role_changes (user_id, changed_at DESC);

CREATE FUNCTION user_role_changes_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'user_role_changes is append-only (% not allowed)', TG_OP;
END;
$$;

CREATE TRIGGER user_role_changes_guard_row
    BEFORE UPDATE OR DELETE ON user_role_changes
    FOR EACH ROW EXECUTE FUNCTION user_role_changes_guard();

CREATE TRIGGER user_role_changes_guard_truncate
    BEFORE TRUNCATE ON user_role_changes
    FOR EACH STATEMENT EXECUTE FUNCTION user_role_changes_guard();

COMMIT;
