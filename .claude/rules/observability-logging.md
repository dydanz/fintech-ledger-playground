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
| `event` | yes | Stable machine name `<domain>.<action>`, lower snake case, never renamed once shipped. Dashboards and alerts key on this, never on `message`. Domains: `auth`, `user`, `audit`, `session`, `dependency`, `app`, `ledger`, `fraud`, `outbox`, `console`. |
| `outcome` | yes | One of `success`, `failure` (our fault or a dependency's), `denied` (caller's fault: bad credentials, validation, forbidden), `partial` (C3: some steps succeeded). |
| `reason` | on `failure`, `denied`, `partial` | snake_case value from a **fixed set** per event, listed in the catalog. Never free text and never `err.Error()`. |
| `error` | on unexpected errors | `err.Error()` of the wrapped error. Only on `failure` and `partial`. |
| `dependency` | C4 | `postgres`, `redis`, … |
| `duration_ms` | C4, and slow operations | float milliseconds |
| `user_id` | whenever known | Already added automatically after the auth middleware runs. Pass it explicitly before that point (e.g. at login). |
| `token_hash` | C3 only | Allowed only so a broken session can be reconciled. Never on other events. |
| `account_id`, `ledger_id`, `transaction_id` | ledger events, whenever known | UUIDs. Use them to correlate with `ledger.*` tables; never log a user-supplied identifier instead. |
| `amount`, `currency` | ledger money events | `amount` is an integer in minor units, `currency` is ISO 4217. Never a float or a formatted string. |
| `client_id` | internal routes, after `ClientAuth` | The authenticated internal caller (e.g. `payments`, `fraud-engine`, `ops`). |
| `actor` | freeze, unfreeze, void | Who initiated the action: a `client_id`. |
| `rule` | `fraud.*`, `ledger.freeze` | Fraud rule name, from a fixed set (e.g. `topup_velocity`, `debit_volume`). |
| `pending`, `lag_ms` | `outbox.lag` | Unpublished outbox row count, and age in milliseconds of the oldest one. |

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
- **`cmd/*`** (via `startup`): own the C5 events and the C4 startup retry events. `worker.Runner` owns `app.leader`.
- One occurrence produces one event. Don't log an error and then return it to a caller that logs it again.

## 5. Signals: making events observable

- A new **error-level** event MUST get a panel or alert query in the dashboard of the service that emits it, keyed on `event` (and `reason` where useful): `deploy/grafana/dashboards/auth-api.json` for auth-api, `deploy/grafana/dashboards/ledger.json` for `ledger.*`, `outbox.*` and `fraud.*` events (ledger-api, ledger-worker, fraud-engine).
  ```logql
  sum by (reason) (count_over_time({service="auth-api", level="error"} | json | event="audit.revoke" [5m]))
  ```
- Alert rules live in `deploy/grafana/provisioning/alerting/ledger.yaml` (TRD §11.3). A new page-worthy condition gets a rule there; check it with `make check-alerts`.
- A new **C1** event MUST be countable by `outcome`, so denial and failure rates can be graphed:
  ```logql
  sum by (outcome) (rate({service="auth-api"} | json | event="auth.login" [1m]))
  ```
- Only `level` and `service` are Loki stream labels. Don't promote `event`, `user_id` or any other high-cardinality field to a label in `deploy/alloy/config.alloy`. Query them with `| json`.

## 6. Never log

Passwords or password hashes, raw bearer tokens, `Authorization` headers, request or response bodies, query strings, emails or other PII (use `user_id`), DB connection strings, secrets or env values. `token_hash` is allowed only on C3 events.

Ledger additions: transaction `reference` and `metadata` (caller-supplied free text), raw `Idempotency-Key` values (keys may embed caller data; the database stores only their SHA-256), and internal service tokens or their hashes.

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
| `auth.password_change` | C1 | success=info (with `sessions_ended`), denied=warn, failure=error, partial=error | denied: `bad_password` (wrong current password: `403`), `invalid_input` (new password not 8–72 bytes); failure: `session_store_error` (sessions not ended, password unchanged: `503`), `pg_error` (the user lookup or the hash write failed; after the sessions were ended, the password is unchanged), `hash_failed` (bcrypt); partial: `audit_revoke_failed` (password changed and sessions ended, but the `auth_token_history` rows weren't stamped) | `service.AuthService.ChangePassword`. Ends every session of the user via the per-user index `session:user:<user_id>` (`session.RedisStore.RevokeAll`); panel in `auth-api.json` |
| `dependency.connect` | C4 | success=info, failure=warn (retrying) / error (gave up) | `timeout`, `refused`, `auth_failed`, `unknown` | `startup.Retry` (every `cmd/*`); `dependency` is `postgres`, `redis` or `kafka` (`kafka.Producer.Connect`) |
| `dependency.call` | C4 | failure=warn (degraded, fell back, or retrying), success=info (only the idempotency cache's recovery) | `timeout`, `refused`, `unknown` | `service.LedgerService` idempotency cache (`dependency=redis`, `operation` = `idempotency_get` / `idempotency_set`; writes continue on Postgres). Logged **once per outage** (#60): the failure that opens the circuit breaker (with `retry_in`), then the cache is skipped for `LedgerConfig.CacheRetry` (5 s) between probes, and failed probes aren't logged again; the probe that finds it back logs `outcome=success` with `down_ms` and `skipped` (calls skipped); `fraud.Engine` (`redis`: `fraud_seen`, `fraud_window`, `fraud_mark_seen`; `kafka`: `fraud_poll`, `fraud_commit`; retried until it succeeds); `outbox.Notifier` (`postgres`, `operation=outbox_notify`; once per outage, the relay keeps polling); `console.proxy` (ledger-console: `dependency=auth-api`, `operation` = `auth_register` / `auth_login` / `auth_logout` / `profile_get` / `profile_update` / `password_change` / `demo_profile`; `dependency=ledger-api`, `operation` = `me_account` / `me_transactions` / `demo_account` / `demo_open` / `demo_credit` / `demo_debit` / `ops_read`; `dependency=auth-api`, `operation` = `ops_access` (the ops role check on every `/console/api/ops/*` request); `dependency=loki`, `operation` = `loki_matrix` / `loki_lines`, logged **once per outage** (the ops screens poll; the screens then show `signals.available=false`); with `duration_ms`; only when auth-api can't be reached, answered `503 upstream_unavailable`; `refused` includes a host that doesn't resolve (a stopped compose container); auth-api's own error responses pass through and aren't logged again) |
| `app.lifecycle` | C5 | info, or error for config/startup failure | `config_invalid`, `startup_failed`, `shutdown_timeout` | `cmd/*` `main` and `startup.Serve` |
| `app.panic` | C5 | error | – | `api.Recovery` |
| `app.leader` | C5 | success=info (`state` = `acquired` / `released`), failure=warn (retried) | `pg_error`, `connection_lost` | `worker.Runner` (ledger-worker; carries `job`) |
| `ledger.account_open` | C2 | success=info, denied=warn, failure=error | `invalid_input`, `user_not_found`, `account_not_found`, `currency_unsupported`, `ledger_exists`, `pg_error` | `service.LedgerService.OpenAccount` / `OpenLedger` / `EnsureSystemLedgers` |
| `ledger.credit`, `ledger.debit` | C2 | success=info, denied=warn, failure=error, partial=error | denied: `invalid_input`, `currency_unsupported`, `account_not_found`, `insufficient_funds`, `account_frozen`, `ledger_not_found`; failure: `pg_error` (Tx1 did not commit), `commit_unknown` (COMMIT got no answer: the connection dropped or a FATAL ended the session. Tx1 may have committed; carries `transaction_id`. The client gets `503` and retries with the same key: a committed one is PENDING, answered `409 request_in_progress` until the sweeper completes it); partial: `tx2_timeout`, `tx2_error` (Tx1 committed, the transaction stays PENDING for the sweeper) | `service.LedgerService.Credit` / `Debit` (validation, Tx1) and `Process` (terminal state, once) |
| `ledger.void` | C2 | success=info, denied=warn, failure=error, partial=error | denied: `invalid_input`, `transaction_not_found`, `not_voidable` (another void in flight or settled: no transaction; original not SETTLED or itself a VOID: FAILED transaction), `insufficient_funds` (§16 Q3); failure: `pg_error`, `commit_unknown` (as for credits); partial: `tx2_timeout`, `tx2_error` | `service.LedgerService.Void` (validation, Tx1) and `Process` (terminal state, once). Carries `original_transaction_id` |
| `ledger.freeze`, `ledger.unfreeze` | C1 | success=info (with `changed`; `false` when already in the target state), denied=warn, failure=error | denied: `invalid_input`, `account_not_found`; failure: `pg_error`, `timeout` (lock wait exceeded `LEDGER_PROCESS_TIMEOUT`) | `service.LedgerService.Freeze` / `Unfreeze`. Carries `actor`, and `rule` on a freeze; never the free-text `reason` |
| `ledger.recover` | C3 | success=warn (a stuck transaction was completed by the sweeper), failure=error | success: `stuck_pending` (Tx2 re-run), `processing_timeout` (older than `LEDGER_PENDING_MAX_AGE`, failed through PROCESSING); failure: `pg_error`, `timeout` | `service.Recovery.Sweep` (ledger-worker). Carries `transaction_id`, `account_id`, `age_ms`, `status`, `key_found`; the transaction's own terminal event is logged once by `Process` |
| `ledger.reconcile` | C3 | success=info (clean run summary), failure=error | `balance_mismatch` (check 1: balance ≠ checkpoint + journal; check 2: a `balance_after` isn't the running sum), `sequence_gap` (check 2: missing, duplicate or out-of-range `ledger_sequence`), `unbalanced_transaction` (check 3: lines not 2 / not summing to 0 / wrong currency or ledger; lines without a SETTLED or VOIDED transaction; a currency's journal not summing to 0), `projection_drift` (check 4: a transaction or account row isn't the fold of its events); run level: `mismatches_found` (summary of a run that found any, with `mismatches` and `suppressed`), `pg_error` (the run failed; the watermark doesn't move) | `service.Reconciler` (ledger-worker job `reconciler`). Mismatch events carry `mode` (`incremental` / `full`), `check` (1–4), `field`, `expected`, `actual`, and `ledger_id` / `transaction_id` / `account_id` / `currency`; summaries carry `transactions`, `ledgers`, `accounts`, `mismatches`, `duration_ms`. At most 100 mismatch events per run |
| `ledger.rebuild` | C2 | success=warn (a projection column differs from its source; with `applied`), info (run summary), failure=error | `pg_error`, `timeout` (row lock wait), `invalid_stream` (the events don't fold; the row is skipped), `rows_failed` (summary: some rows failed), `interrupted` | `service.Rebuilder` (`ledger-worker rebuild-projections`). Diff events carry `mode` (`dry_run` / `apply`), `kind` (`account` / `transaction` / `ledger`), the row's `<kind>_id`, `field`, `current`, `rebuilt`; never `reference` or `metadata` (not rebuilt). The summary carries `checked`, `changed`, `failed`, `duration_ms` |
| `ledger.partition` | C5 | success=info (every run, with `created`, `future_months`, `current_month`), failure=error | `insufficient_future_partitions` (fewer than 2 future months in `table`), `pg_error` (creating or counting failed) | `service.Partitions` (ledger-worker job `partitions`, hourly) |
| `ledger.archive` | C2 | success=info (each step, and a run summary with `archived`, `held_months`), denied=warn (`legal_hold`: the month is skipped), failure=error | denied: `legal_hold`; failure: `bucket_not_worm` (no Object Lock in compliance mode: nothing archived), `bucket_unavailable`, `export_failed`, `verify_failed`, `detach_failed` (detach or drop), `pg_error` | `service.Archiver` (ledger-worker job `archiver`, daily). Carries `step` (`bucket`, `list`, `hold`, `checkpoint`, `export`, `verify`, `detach`, `drop`, `run`), `table`, `month`, `partition`, `rows`, `files`, `manifest` (the s3:// URI), `checkpoints`, `held_accounts` (account ids) |
| `ledger.purge` | C2 | success=info (only when rows were deleted, with `deleted`), failure=error | `pg_error` | `service.Recovery.PurgeIdempotencyKeys` (ledger-worker) |
| `outbox.publish` | C4 | failure=warn (retrying), error (still failing after 1 minute; it keeps retrying) | `kafka_unavailable`, `kafka_rejected`, `pg_error` | `outbox.Relay` (ledger-worker). Carries `retry_in`, `failing_ms`; success is not logged (the lag event shows progress) |
| `outbox.lag` | C4 | info; warn when `lag_ms` > 5000; failure=warn | failure: `pg_error` | `outbox.Relay`, every 30 s (`pending`, `lag_ms`) |
| `outbox.purge` | C2 | success=info (only when rows were deleted, with `deleted`), failure=error | `pg_error` | `outbox.Relay.Purge` (ledger-worker; published rows older than `OUTBOX_RETENTION`) |
| `fraud.decision` | C1 | success=warn (freeze applied), failure=error | `freeze_call_failed` (ledger-api unreachable or 5xx past `FRAUD_FREEZE_RETRY`; still retrying, offset not committed), `freeze_rejected` (4xx: token, grant or account; skipped) | `fraud.Engine` (fraud-engine). Carries `rule`, `account_id`, `transaction_id`, `count`, `window_start`, `attempts`, `detect_ms` (event commit → freeze); `currency`/`amount` for `debit_volume`; never the free-text reason |
| `console.demo_action` | C2 | success=info, denied=warn, failure=error | denied: ledger-api's refusal code (`insufficient_funds`, `account_frozen`, `account_not_found`, `invalid_input`, …), or `rejected` without one; failure: `upstream_error` (ledger-api answered 5xx; it logged its own event) | `console.demo.logAction` (ledger-console): one per demo open, credit or debit on My account, the record that the change came from the console on behalf of `user_id` (ledger-api's events carry `client_id` = `onboarding` / `payments`). Carries `action` (`open` / `credit` / `debit`), `user_id`, `account_id`, `transaction_id`, `amount`, `currency`, `status`; panel in `ledger.json` |
| `console.access` | C1 | denied=warn, failure=error | denied: `missing_token` (no bearer token on an ops route), `invalid_token` (auth-api says the session ended), `not_ops` (the user has no ops role; carries `user_id`), `host_not_allowed` (the `Host` header isn't in `CONSOLE_ALLOWED_HOSTS`; the host isn't logged); failure: `auth_error` (auth-api answered the profile check with something other than 200 or 401; with `status`) | `console.requireOps` / `console.allowHosts` (ledger-console). A granted ops request isn't logged again: it's in the access log with `user_id`. Panel in `ledger.json` |
| `user.role_change` | C1 | success=info (with `user_id`, `role`, `previous_role`), denied=warn, failure=error | denied: `invalid_input` (not `user` or `ops`), `not_found` (no user with that email); failure: `pg_error` | `service.RoleService.SetRole`, run only by `auth-api grant-role` (`make grant-role`); never the email or the operator's name. The durable record is the append-only `user_role_changes` table (the one-off container usually exits before Alloy collects its logs). Panel in `auth-api.json` |
| `fraud.event` | C4 | failure=warn | `malformed_event` (skipped) | `fraud.Engine` |
| `ledger.idempotency` | C1 | success=info (replay, with `source` = `cache` or `postgres`), denied=warn | `key_reused`, `in_progress` | `service.LedgerService` (Redis fast path, or key claim in Tx1) |
| `ledger.client_auth` | C1 | denied=warn | `missing_token`, `invalid_token`, `forbidden_operation` | `api.Clients.Require` (ledger-api internal routes) |

### Current gaps (existing code predates this convention)

The code works but doesn't yet meet this convention. Bring each item into line whenever you next touch the code around it, or in a dedicated `chore/logging-convention` PR:
- No log call sets `event` or `outcome` yet. `login failed` already uses `reason`.
- `Register` and `UpdateProfile` log nothing on `denied` or `failure` paths.
- The `auth.token` rejections in the `Auth` middleware are only visible through the access log's 401/503.
- No dashboard panel exists for `audit.revoke` partial failures.
