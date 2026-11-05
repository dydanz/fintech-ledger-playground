-- 001_init.up.sql
-- Schema for the auth-api service: users + append-only token audit trail.

BEGIN;

CREATE TABLE users (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT        NOT NULL,
    password_hash TEXT        NOT NULL,
    full_name     TEXT        NOT NULL DEFAULT '',
    bio           TEXT        NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT users_email_len     CHECK (char_length(email) BETWEEN 3 AND 254),
    CONSTRAINT users_full_name_len CHECK (char_length(full_name) <= 100),
    CONSTRAINT users_bio_len       CHECK (char_length(bio) <= 500)
);

-- Case-insensitive uniqueness; the application also stores emails lower-cased.
CREATE UNIQUE INDEX users_email_lower_key ON users (lower(email));

CREATE TABLE auth_token_history (
    id          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id     UUID        NOT NULL REFERENCES users (id) ON DELETE RESTRICT,
    -- SHA-256 hex digest of the bearer token. The plain token is never stored.
    token_hash  CHAR(64)    NOT NULL,
    login_ip    INET,
    user_agent  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ,
    CONSTRAINT auth_token_history_hash_format CHECK (token_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT auth_token_history_ua_len      CHECK (char_length(user_agent) <= 512)
);

CREATE UNIQUE INDEX auth_token_history_token_hash_key ON auth_token_history (token_hash);
CREATE INDEX auth_token_history_user_created_idx ON auth_token_history (user_id, created_at DESC);

-- Enforce the append-only contract at the database level:
--   * no DELETE / TRUNCATE
--   * the only permitted UPDATE is setting revoked_at once (NULL -> timestamp)
CREATE FUNCTION auth_token_history_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
        RAISE EXCEPTION 'auth_token_history is append-only (% not allowed)', TG_OP;
    END IF;

    IF OLD.revoked_at IS NOT NULL THEN
        RAISE EXCEPTION 'auth_token_history row % is already revoked', OLD.id;
    END IF;

    IF NEW.revoked_at IS NULL
       OR NEW.id         IS DISTINCT FROM OLD.id
       OR NEW.user_id    IS DISTINCT FROM OLD.user_id
       OR NEW.token_hash IS DISTINCT FROM OLD.token_hash
       OR NEW.login_ip   IS DISTINCT FROM OLD.login_ip
       OR NEW.user_agent IS DISTINCT FROM OLD.user_agent
       OR NEW.created_at IS DISTINCT FROM OLD.created_at
       OR NEW.expires_at IS DISTINCT FROM OLD.expires_at THEN
        RAISE EXCEPTION 'auth_token_history only allows setting revoked_at';
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER auth_token_history_guard_row
    BEFORE UPDATE OR DELETE ON auth_token_history
    FOR EACH ROW EXECUTE FUNCTION auth_token_history_guard();

CREATE TRIGGER auth_token_history_guard_truncate
    BEFORE TRUNCATE ON auth_token_history
    FOR EACH STATEMENT EXECUTE FUNCTION auth_token_history_guard();

COMMIT;
