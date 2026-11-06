# Critical signals and logging convention

**Applies to:** every new feature, new function, or change to existing behaviour in this repo.

A change is not done until every **critical operation** it adds or touches emits its **critical event**, using the schema below.

The stack is Go `slog` writing JSON to stdout. Alloy ships it to Loki, and Grafana reads from Loki. Logs are the only signal source, so a failure that isn't logged per this convention can't be seen, counted, or alerted on.

---

## 1. Which operations are critical

An operation is critical if it falls into any of these classes. Each one MUST emit exactly one event per occurrence, covering both success and failure.

| Class | What it covers | Examples in this repo |
| :--- | :--- | :--- |
| **C1 Security** | Authentication and authorization decisions; credential or session issuance and revocation | login, logout, a token rejected by the auth middleware, a password change |
| **C2 State change** | Any create, update or delete of persisted data | register, profile update, audit insert or revoke |
| **C3 Integrity** | A partial failure that leaves stores inconsistent, and any compensating action | Redis session deleted but PG revoke failed; orphaned audit row revoked |
| **C4 Dependency** | A Postgres, Redis or external call that fails, times out, or is retried | Redis down in the auth middleware; the startup retry loop |
| **C5 Lifecycle** | Process start and stop, config load, readiness, panics | `http server listening`, config error, shutdown, `panic recovered` |

Reads that don't change state and don't make a security decision (e.g. `GET /users/profile`) are covered by the HTTP access log alone. Don't add events for them.

## 2. Event schema

Every critical event is one `slog` record with these fields:

| Field | Required | Rule |
| :--- | :--- | :--- |
| `message` | yes | Short human-readable sentence, lower case: `"login failed"`. |
| `event` | yes | Stable machine name `<domain>.<action>`, lower snake case, never renamed once shipped. Dashboards and alerts key on this, never on `message`. Domains: `auth`, `user`, `audit`, `session`, `dependency`, `app`. |
| `outcome` | yes | One of `success`, `failure` (our fault or a dependency's), `denied` (caller's fault: bad credentials, validation, forbidden), `partial` (C3: some steps succeeded). |
| `reason` | on `failure`, `denied`, `partial` | snake_case value from a **fixed set** per event, listed in the catalog. Never free text and never `err.Error()`. |
| `error` | on unexpected errors | `err.Error()` of the wrapped error. Only on `failure` and `partial`. |
| `dependency` | C4 | `postgres`, `redis`, … |
| `duration_ms` | C4, and slow operations | float milliseconds |
| `user_id` | whenever known | Already added automatically after the auth middleware runs. Pass it explicitly before that point (e.g. at login). |
| `token_hash` | C3 only | Allowed only so a broken session can be reconciled. Never on other events. |

These fields are injected automatically. **Never set them by hand:** `timestamp`, `level`, `service`, `request_id`, `trace_id`, `span_id`. Always log through `logger.FromContext(ctx)` and the `*Context` methods so they're present.

```go
log := logger.FromContext(ctx)
log.WarnContext(ctx, "login failed",
    "event", "auth.login", "outcome", "denied", "reason", "bad_password", "user_id", u.ID)

log.ErrorContext(ctx, "session deleted but audit revoke failed",
    "event", "audit.revoke", "outcome", "partial", "reason", "pg_update_failed",
    "error", err, "user_id", userID, "token_hash", tokenHash)
```

## 3. Levels

| Level | Use for | Expected volume |
| :--- | :--- | :--- |
| `debug` | Developer diagnostics. Off in production. Never carries a critical event. | any |
| `info` | `outcome=success` critical events; lifecycle | normal traffic |
| `warn` | `outcome=denied` (the client caused it); a recovered retry or degradation | Spikes are worth watching, but a single one isn't actionable. |
| `error` | `outcome=failure` or `partial`; panics; anything an operator must act on | Should be ~0. Every error-level event needs an alert or dashboard signal (§5). |

`fatal` is not used. `main` logs at `error` and exits non-zero.

## 4. Where to log (exactly once)

- **Service layer** (`internal/service`): owns the C1, C2 and C3 events. This is where the business outcome is known.
- **HTTP layer** (`internal/api`): the access log covers every request. The middleware owns C1 events it decides alone (`auth.token` rejected). Handlers **don't** re-log errors the service already logged; for unexpected errors they call `c.Error(err)` so the access log records them.
- **Repositories / session store**: **never log.** Return wrapped errors (`fmt.Errorf("insert user: %w", err)`). The caller decides the event.
- **`cmd/api`**: owns the C5 events and the C4 startup retry events.
- One occurrence produces one event. Don't log an error and then return it to a caller that logs it again.

## 5. Signals: making events observable

- A new **error-level** event MUST get a panel or alert query in `deploy/grafana/dashboards/auth-api.json`, keyed on `event` (and `reason` where useful):
  ```logql
  sum by (reason) (count_over_time({service="auth-api", level="error"} | json | event="audit.revoke" [5m]))
  ```
- A new **C1** event MUST be countable by `outcome`, so denial and failure rates can be graphed:
  ```logql
  sum by (outcome) (rate({service="auth-api"} | json | event="auth.login" [1m]))
  ```
- Only `level` and `service` are Loki stream labels. Don't promote `event`, `user_id` or any other high-cardinality field to a label in `deploy/alloy/config.alloy`. Query them with `| json`.

## 6. Never log

Passwords or password hashes, raw bearer tokens, `Authorization` headers, request or response bodies, query strings, emails or other PII (use `user_id`), DB connection strings, secrets or env values. `token_hash` is allowed only on C3 events.

## 7. Definition of done for any change

Before committing a feature or behaviour change, confirm all of these:

1. Every critical operation added or changed (§1) emits one event with `event`, `outcome`, and `reason`/`error` as required (§2), at the right level (§3) and in the right layer (§4).
2. Every **failure, denied and partial** path is covered, not just the success path.
3. New `event` names and `reason` values are added to the catalog below.
4. Error-level and C1 events have a signal (§5).
5. A test asserts the event for each new failure, denied or partial path. Use the log buffer in `internal/api/api_test.go` (`h.logs`) and check `event`, `outcome` and `reason`.
6. Nothing from §6 appears in any new log line.

Tell the user when one of these can't be met, and don't skip it silently.

---

## Event catalog

This is the source of truth for event names and reasons. Add entries here in the same change that introduces them.

| `event` | Class | Level by outcome | Allowed `reason` values | Emitted in |
| :--- | :--- | :--- | :--- | :--- |
| `http.request` | – | info (<400), warn (4xx), error (5xx) | – | `api.AccessLog` (message `http_request`) |
| `auth.register` | C2 | success=info, denied=warn, failure=error | `email_taken`, `invalid_input`, `pg_error` | `service.AuthService.Register` |
| `auth.login` | C1 | success=info, denied=warn, failure=error | `unknown_email`, `bad_password`, `pg_error`, `session_store_error` | `service.AuthService.Login` |
| `auth.logout` | C1 | success=info, failure=error | `session_store_error` | `service.AuthService.Logout` |
| `auth.token` | C1 | denied=warn, failure=error | `missing_token`, `malformed_token`, `invalid_or_expired`, `session_store_error` | `api.Auth` middleware |
| `audit.revoke` | C3 | success=info, partial=error, failure=warn | `pg_update_failed`, `no_open_record`, `orphan_compensation_failed` | `service.AuthService.Login` / `Logout` |
| `user.profile_update` | C2 | success=info, denied=warn, failure=error | `invalid_input`, `not_found`, `pg_error` | `service.UserService.UpdateProfile` |
| `dependency.connect` | C4 | success=info, failure=warn (retrying) / error (gave up) | `timeout`, `refused`, `auth_failed`, `unknown` | `cmd/api` `retry` |
| `app.lifecycle` | C5 | info, or error for config/startup failure | `config_invalid`, `startup_failed`, `shutdown_timeout` | `cmd/api` (`started`, `stopping`, `stopped`) |
| `app.panic` | C5 | error | – | `api.Recovery` |

### Current gaps (existing code predates this convention)

The code works but doesn't yet meet this convention. Bring each item into line whenever you next touch the code around it, or in a dedicated `chore/logging-convention` PR:
- No log call sets `event` or `outcome` yet. `login failed` already uses `reason`.
- `Register` and `UpdateProfile` log nothing on `denied` or `failure` paths.
- The `auth.token` rejections in the `Auth` middleware are only visible through the access log's 401/503.
- No dashboard panel exists for `audit.revoke` partial failures.
- The `cmd/api` startup and retry logs have no `event` or `dependency` fields.
