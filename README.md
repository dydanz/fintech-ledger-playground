# auth-api

Go/Gin authentication service with Redis-backed sessions, a PostgreSQL audit trail, and structured JSON logs shipped to Loki/Grafana.

## Quick start

```bash
cp .env.example .env      # optional: override dev passwords
make up                   # docker compose up -d --build
make smoke                # register -> login -> profile -> logout -> 401
```

| Service | URL |
| :--- | :--- |
| API | http://localhost:8080 |
| Grafana | http://localhost:3000 (admin / admin), dashboard **auth-api › auth-api overview** |
| Loki | http://localhost:3100 |
| Alloy UI | http://localhost:12345 |

## API

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

**Tokens.** Login issues a 256-bit random opaque token (base64url). Only its SHA-256 digest is stored anywhere.

**Hot path (Redis).** The session is stored as `session:auth:<sha256(token)>` → `user_id` with a 3600 s TTL. The auth middleware does one pipelined `GET` + `PTTL` and never touches PostgreSQL. A key without a positive TTL is rejected, so a session can't outlive its window. If Redis is unreachable the middleware returns `503`, not `401`.

> The key uses the token's hash rather than the raw token, so a Redis dump or `MONITOR` session doesn't expose usable credentials.

**Cold path (PostgreSQL).** Each login appends an `auth_token_history` row (`user_id`, `token_hash`, `login_ip`, `user_agent`, `created_at`, `expires_at`, `revoked_at`). A trigger enforces the append-only contract: `DELETE` and `TRUNCATE` are refused, and the only update allowed is setting `revoked_at` once.

**Ordering and failure handling.**
- *Login*: the audit row is written **before** the Redis session, so no usable token exists without an audit record. If the Redis write fails, the audit row is immediately marked revoked and the client gets a `500`.
- *Logout*: the Redis key is deleted **first**, which makes the token unusable. Then `revoked_at` is set. If that audit update fails, the token is already dead, so the client still gets `204` and an `error`-level log (`session deleted but audit revoke failed`, with `token_hash`) is emitted for reconciliation.
- Tokens that expire naturally keep `revoked_at = NULL`. Use `expires_at` to tell them apart.

**Other hardening.**
- bcrypt (cost 12 by default). Passwords must be 8–72 bytes.
- Logins for unknown emails still run a bcrypt compare, and return the same error as a wrong password, so accounts can't be enumerated.
- Request bodies are capped at 1 MiB.
- `X-Forwarded-For` is ignored unless `TRUSTED_PROXIES` is set.
- The runtime image is distroless and runs as non-root with a read-only root filesystem.

## Logging and observability

Every log line is a single JSON object on stdout:

```json
{"timestamp":"2026-11-05T10:00:00.123Z","level":"info","message":"http_request","service":"auth-api",
 "request_id":"…","trace_id":"…","span_id":"…","method":"POST","path":"/api/v1/auth/login",
 "route":"/api/v1/auth/login","status":200,"latency_ms":63.2,"client_ip":"172.18.0.1",
 "response_bytes":142,"user_agent":"curl/8.7.1"}
```

- `request_id` is taken from a well-formed inbound `X-Request-ID`, or generated, and echoed in the response header.
- `trace_id` and `span_id` come from a valid inbound W3C `traceparent` header. If it is missing or invalid, new ones are generated. Either way the `traceparent` in use is echoed in the response header.
- Domain events (`login succeeded`, `login failed`, `logout succeeded`, …) share the same request-scoped fields.
- The query string is never logged.

**Pipeline.** Containers write to the json-file driver, with rotation. **Grafana Alloy** discovers containers labelled `logging=alloy` via the Docker socket, parses the JSON, and promotes `level` and `service` to Loki labels. Grafana auto-provisions Loki as the default data source, along with a starter dashboard.

> Alloy is used instead of Promtail because Promtail reached end-of-life in March 2026 and Alloy is its official replacement. The pipeline does the same thing. The Docker Loki log driver was avoided because it needs a plugin installed on every host and can block containers when Loki is down.

Useful LogQL:

```logql
{service="auth-api"} | json | status >= 500
{service="auth-api"} | json | message="login failed" | line_format "{{.reason}} {{.client_ip}}"
sum by (route) (rate({service="auth-api"} | json | message="http_request" [1m]))
```

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

The token lifetime is fixed at 1 hour (`service.TokenTTL`) and is intentionally not configurable.

## Database migrations

`migrations/001_init.up.sql` is mounted into `/docker-entrypoint-initdb.d`, so it runs automatically the **first** time Postgres starts with an empty volume. Run `make reset` to re-initialise. Up/down files follow the `golang-migrate` naming, so you can switch to a migration runner in production.

## Layout

```
cmd/api/              entrypoint, graceful shutdown, container healthcheck
internal/api/         router, middleware (request ctx, access log, recovery, auth), handlers
internal/service/     auth + profile business logic
internal/repository/  PostgreSQL (pgx) users + token history
internal/session/     Redis session store
internal/token/       token generation / hashing
internal/logger/      slog JSON logger
migrations/           SQL schema
deploy/               loki, alloy, grafana provisioning
```

## Tests

`make test` runs the HTTP-level tests. They use miniredis for Redis and in-memory fakes for the Postgres repositories. They cover:
- the full flow, plus 1 h expiry and rejection of keys without a TTL
- auth middleware rejections, and `503` when Redis is down
- revoking the orphaned audit row when the Redis write fails
- duplicate and invalid registration, and identical errors for unknown email vs wrong password
- profile validation
- the structured log fields

The SQL and repository layer are not covered by these tests. Exercise them with `make up && make smoke`.

## Not included (next steps)

- Rate limiting and lockout on `/login`
- Metrics and tracing export (Prometheus, OTel); trace IDs are propagated and logged but not exported
- Logout-all-sessions and password change, which would need a per-user session index in Redis
