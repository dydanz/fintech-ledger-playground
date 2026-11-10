# Auth API Reference

auth-api is a Go/Gin authentication service with Redis-backed sessions and a PostgreSQL audit trail. ledger-api's user routes accept the tokens it issues ([Ledger API Reference](<Ledger API Reference.md>)).

## Routes

| Method | Path | Auth | Body | Success |
| :--- | :--- | :--- | :--- | :--- |
| POST | `/api/v1/auth/register` | – | `{"email","password"}` | `201` user |
| POST | `/api/v1/auth/login` | – | `{"email","password"}` | `200 {"access_token","token_type":"Bearer","expires_in":3600,"expires_at"}` |
| POST | `/api/v1/auth/logout` | Bearer | – | `204` |
| GET | `/api/v1/users/profile` | Bearer | – | `200` user |
| PUT | `/api/v1/users/profile` | Bearer | `{"full_name"?, "bio"?}` | `200` user |
| GET | `/healthz`, `/readyz` | – | – | liveness / dependency readiness |

Errors are always `{"error":{"code","message","request_id"}}`.

## How it works

**Tokens.** Login issues a 256-bit random opaque token (base64url). Only its SHA-256 digest is stored anywhere. The token lifetime is fixed at 1 hour (`service.TokenTTL`) and is intentionally not configurable.

**Hot path (Redis).** The session is stored as `session:auth:<sha256(token)>` → `user_id`, with a 3600 s TTL.
- **Lookups:** the auth middleware does one pipelined `GET` + `PTTL` and never touches PostgreSQL.
- **TTL:** a key without a positive TTL is rejected, so a session can't outlive its window.
- **Redis down:** the middleware returns `503`, not `401`.

The key uses the token's hash rather than the raw token, so a Redis dump or `MONITOR` session doesn't expose usable credentials.

**Cold path (PostgreSQL).** Each login appends an `auth_token_history` row with these columns:
- `user_id`, `token_hash`;
- `login_ip`, `user_agent`;
- `created_at`, `expires_at`, `revoked_at`.

A trigger enforces the append-only contract: `DELETE` and `TRUNCATE` are refused, and the only update allowed is setting `revoked_at` once.

**Ordering and failure handling.**
- **Login:** the audit row is written **before** the Redis session, so no usable token exists without an audit record. If the Redis write fails, the audit row is immediately marked revoked and the client gets a `500`.
- **Logout:**
  - The Redis key is deleted **first**, which makes the token unusable. Then `revoked_at` is set.
  - If that audit update fails, the token is already dead, so the client still gets `204`.
  - The failure is logged at `error` level for reconciliation (`session deleted but audit revoke failed`, with `token_hash`).
- **Natural expiry:** tokens that expire naturally keep `revoked_at = NULL`. Use `expires_at` to tell them apart.

**Other hardening.**
- bcrypt, cost 12 by default. Passwords must be 8–72 bytes.
- Logins for unknown emails still run a bcrypt compare and return the same error as a wrong password, so accounts can't be enumerated.
- Request bodies are capped at 1 MiB.
- `X-Forwarded-For` is ignored unless `TRUSTED_PROXIES` is set.
- The runtime image is distroless and runs as non-root with a read-only root filesystem.

## Configuration

| Variable | Default | Notes |
| :--- | :--- | :--- |
| `DATABASE_URL` | – (required) | pgx connection string |
| `REDIS_ADDR` / `REDIS_PASSWORD` / `REDIS_DB` | `localhost:6379` / – / `0` | |
| `HTTP_ADDR` | `:8080` | |
| `BCRYPT_COST` | `12` | |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `TRUSTED_PROXIES` | – | comma-separated CIDRs/IPs |
| `SERVICE_NAME` | `auth-api` | value of the `service` log field |
| `SHUTDOWN_TIMEOUT` | `15s` | graceful drain on SIGTERM |
