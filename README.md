Go/Gin authentication service with Redis-backed sessions, a PostgreSQL audit trail, and structured JSON logs shipped to Loki/Grafana, plus **ledger-api**, an immutable, event-sourced balance ledger (design: [`spec/`](spec/)) that is being built in phases.

## Quick start

```bash
cp .env.example .env      # optional: override dev passwords
make up                   # docker compose up -d --build
make smoke                # register -> login -> profile -> logout -> 401
make smoke-ledger         # open account -> credit -> debit -> NSF -> replay -> auth checks
```

| Service | URL |
| :--- | :--- |
| API | http://localhost:8080 |
| ledger-api | http://localhost:8081, bound to 127.0.0.1 only |
| ledger-worker | no published port; background jobs (outbox relay to Kafka, sweeper, reconciler, idempotency and outbox purges). The reconciler checks that balances, sequences, double entries and projections match the journal and events every `RECONCILE_INTERVAL` (default 1m), with a full sweep daily. `make rebuild-projections` (dry run; `ARGS=--apply` to write) recomputes the projections from the events and journal when the reconciler reports drift |
| fraud-engine | no published port; consumes `ledger.events.v1` and freezes accounts on `topup_velocity` (more than 3 top-ups in 5 m) or `debit_volume` (more than 30 debits, or over the currency limit, in 1 h). `make smoke-fraud` checks it end to end |
| Grafana | http://localhost:3000 (admin / admin), dashboards **auth-api overview** and **ledger overview**; alert rules in folder **ledger** (success ratio, outbox lag, stuck PENDING, fraud freeze failing, reconcile failure). `make check-alerts` verifies them |
| Loki | http://localhost:3100 |
| Alloy UI | http://localhost:12345 |
| Kafka | `127.0.0.1:9094` from the host (containers use `kafka:9092`); topic `ledger.events.v1`, 12 partitions. `make kafka-topics` describes it |

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

## ledger-api

### Internal routes

Trusted services call it with `Authorization: Bearer <service token>`. `LEDGER_CLIENTS` maps each `client_id` to the SHA-256 of its token and the operations it may perform (`account_open`, `credit`, `debit`, `void`, `freeze`, `unfreeze`, `read`). Every mutating call needs an `Idempotency-Key` header; a retry with the same key gets the original response back byte for byte (`Idempotent-Replayed: true`), a different request with the same key gets `422 idempotency_key_reused`, and one still in flight gets `409 request_in_progress`. Completed responses are also cached in Redis (`idem:ledger:<client_id>:<sha256(key)>`, expiring with the Postgres row), so most retries are answered without touching Postgres. Postgres stays the authority: with Redis down, writes and replays still work, and each failed Redis call logs a `dependency.call` warning.

| Method | Path | Operation | Success |
| :--- | :--- | :--- | :--- |
| POST | `/internal/v1/accounts` `{"user_id"}` | `account_open` | `201` new account with an IDR ledger, or `200` the existing one (one account per user) |
| POST | `/internal/v1/accounts/:account_id/ledgers` `{"currency","overdraft_limit"?}` | `account_open` | `201` ledger |
| POST | `/internal/v1/accounts/:account_id/credits` `{"amount","currency","counterparty"?,"reference"?,"metadata"?}` | `credit` | `201` SETTLED, or `202` PENDING |
| POST | `/internal/v1/accounts/:account_id/debits` (same body) | `debit` | `201` SETTLED, `422 insufficient_funds` / `account_frozen`, or `202` |
| POST | `/internal/v1/transactions/:transaction_id/void` `{"reason"}` | `void` | `201` the SETTLED VOID (the original becomes `VOIDED`), `202`, or `422 not_voidable` / `insufficient_funds` |
| POST | `/internal/v1/accounts/:account_id/freeze` `{"reason","rule"?}` | `freeze` | `200` account, `FROZEN`. Debits fail `account_frozen` from the moment it commits; credits and voids still settle. Idempotent if already frozen |
| POST | `/internal/v1/accounts/:account_id/unfreeze` `{"reason"}` | `unfreeze` | `200` account, `ACTIVE` |
| GET | `/internal/v1/accounts/:account_id`, `/internal/v1/accounts?user_id=` | `read` | `200` account with balances |
| GET | `/internal/v1/transactions/:transaction_id` | `read` | `200` transaction |

### User routes

End users call these with the `access_token` from auth-api's `/api/v1/auth/login`; ledger-api checks it against the same Redis sessions. The account always comes from the session's `user_id`, so no route takes an `account_id`, and another user's transaction is a `404`, never a `403`.

| Method | Path | Success |
| :--- | :--- | :--- |
| GET | `/api/v1/ledger/account` | `200` the caller's account with a balance per currency, or `404 account_not_found` |
| GET | `/api/v1/ledger/transactions?currency=&limit=&cursor=` | `200 {"transactions":[…],"next_cursor"?}`, newest first. `limit` is 1–100 (default 20); pass `next_cursor` back as `cursor` for the next page, and its absence means the last page |
| GET | `/api/v1/ledger/transactions/:transaction_id` | `200` transaction, or `404 transaction_not_found` if it isn't the caller's |

Pages are keyset-paginated on `(created_at, id)`, so a transaction written while you page doesn't shift or repeat later pages. Redis isn't part of ledger-api's `/readyz`: if Redis goes down, the user routes return `503` and the internal routes keep serving on Postgres-only idempotency.

Amounts are integers in the currency's minor unit (IDR has none). The dev tokens in `.env.example` are `dev-onboarding-token`, `dev-payments-token` and `dev-ops-token`; replace them anywhere but your laptop. The Kafka relay and the fraud engine arrive in later phases (see the development plan in `spec/`).

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

ledger-api reads `DATABASE_URL`, `REDIS_*` (auth-api's session store, for the user routes), `LOG_LEVEL`, `SERVICE_NAME` (default `ledger-api`) and `SHUTDOWN_TIMEOUT` as above, plus (TRD §12.2):

| Variable | Default | Notes |
| :--- | :--- | :--- |
| `LEDGER_HTTP_ADDR` | `:8081` | |
| `LEDGER_CLIENTS` | – (required) | `client_id:sha256(token):op,op;…` |
| `LEDGER_OVERDRAFT_CLIENTS` | – | clients allowed to set `overdraft_limit` |
| `LEDGER_UNFREEZE_CLIENTS` | `ops` | the only clients `LEDGER_CLIENTS` may grant `unfreeze`; startup fails otherwise |
| `LEDGER_CURRENCIES` | `IDR` | the first one is opened with every account |
| `LEDGER_MAX_AMOUNT` | `1000000000000` | per transaction, minor units |
| `LEDGER_PROCESS_TIMEOUT` | `2s` | bound on each of Tx1 and Tx2; past it the client gets `202` |
| `IDEMPOTENCY_TTL` | `24h` | |
| `KAFKA_TOPIC_EVENTS` | `ledger.events.v1` | topic written to the outbox |

The token lifetime is fixed at 1 hour (`service.TokenTTL`) and is intentionally not configurable.

## Database migrations

[golang-migrate](https://github.com/golang-migrate/migrate) applies `migrations/*.up.sql` in order. `make up` runs it as the one-shot `migrate` service, and every service that uses Postgres waits for it to finish, so pulling a new migration needs only `make up` (or `make migrate`). Your data is kept. `make migrate-version` shows the recorded version (`public.schema_migrations`).

- **Volumes created before the runner** (migrations 001–004 ran as Postgres init scripts) are baselined automatically on the first `make up`: `deploy/migrate/baseline.sh` detects which of 001–004 are in place and records that version, then `migrate` applies the rest. It does nothing on an empty database or one that is already baselined.
- **A new migration** is a `NNN_name.up.sql` / `NNN_name.down.sql` pair with the next number. `make test-integration` applies every up file to a throwaway database (`internal/testutil/pgtest`).
- `make reset` still drops all volumes; the next `up` migrates the empty database.

`002_ledger.up.sql` creates the `ledger` schema with monthly partitions for the current month and 3 ahead. Until the ledger-worker partition job exists (P5), extend them by hand when needed: `SELECT ledger.ensure_partitions((now() AT TIME ZONE 'UTC')::date, 4);` In production, run the same `migrate` image (or CLI) against the database before deploying the services.

## Layout

```
cmd/api/              auth-api entrypoint
cmd/ledger-api/       ledger-api entrypoint
cmd/ledger-worker/    ledger-worker entrypoint (outbox relay, sweeper, reconciler, purges)
cmd/fraud-engine/     fraud-engine entrypoint (velocity rules → freeze)
internal/startup/     dependency retry, graceful serve, container healthcheck (shared by cmd/*)
internal/ledger/      ledger domain: money, events, state machines, Decide (no I/O)
internal/idempotency/ Idempotency-Key hashing, request fingerprints, Redis replay cache
internal/kafka/      franz-go producer (acks=all, idempotent) and consumer-group wrappers
internal/fraud/      velocity rules (topup_velocity, debit_volume) over event-time windows; Redis ZSET store
internal/outbox/      Kafka envelope written to ledger.outbox
internal/api/         router, middleware (request ctx, access log, recovery, auth), handlers
internal/service/     auth, profile and ledger business logic
internal/repository/  PostgreSQL (pgx): users, token history, ledger
internal/session/     Redis session store
internal/token/       token generation / hashing
internal/logger/      slog JSON logger
internal/testutil/    test helpers (pgtest: throwaway Postgres for integration tests)
migrations/           SQL schema
deploy/               loki, alloy, grafana provisioning
```

The `Dockerfile` builds any binary under `cmd/`, chosen with the `CMD` build arg (default `api`): `docker build --build-arg CMD=api .`. Every binary must implement a `healthcheck` subcommand, which the image uses as its container healthcheck.

## Tests

`make test` runs the HTTP-level tests. They use miniredis for Redis and in-memory fakes for the Postgres repositories. They cover:
- the full flow, plus 1 h expiry and rejection of keys without a TTL
- auth middleware rejections, and `503` when Redis is down
- revoking the orphaned audit row when the Redis write fails
- duplicate and invalid registration, and identical errors for unknown email vs wrong password
- profile validation
- the structured log fields

The SQL and repository layer are not covered by these tests. Exercise them with `make up && make smoke`, or with integration tests.

**Integration tests** are Go tests behind the `integration` build tag, run against a real Postgres:

```bash
docker compose up -d postgres kafka kafka-init
make test-integration   # TEST_DATABASE_URL / TEST_KAFKA_BROKERS default to the compose Postgres and Kafka
```

`pgtest.New(t)` (in `internal/testutil/pgtest`) creates a throwaway database for each test, applies every `migrations/*.up.sql` in order, and drops the database afterwards. Tests are skipped when `TEST_DATABASE_URL` is unset. The role needs `CREATEDB`, which the compose `auth` role has.

## Not included (next steps)

- Rate limiting and lockout on `/login`
- Metrics and tracing export (Prometheus, OTel); trace IDs are propagated and logged but not exported
- Logout-all-sessions and password change, which would need a per-user session index in Redis
