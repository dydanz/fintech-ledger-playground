# Ledger API Reference

ledger-api serves the immutable, event-sourced balance ledger. The design is in the [TRD](<2026-11-06 TRD - Immutable Ledger & Event-Sourced Balance System.md>).

Amounts are integers in the currency's minor unit (IDR has none).

## Internal routes

Trusted services call these with `Authorization: Bearer <service token>`. They are bound to 127.0.0.1 locally; in production they're internal only, with mTLS (TRD §6.1).

**Clients.** `LEDGER_CLIENTS` maps each `client_id` to the SHA-256 of its token and to the operations it may perform: `account_open`, `credit`, `debit`, `void`, `freeze`, `unfreeze`, `read`. The dev tokens in `.env.example` are `dev-onboarding-token`, `dev-payments-token` and `dev-ops-token`; replace them anywhere but your laptop.

**Idempotency.** Every mutating call needs an `Idempotency-Key` header:
- **The same request again** gets the original response back byte for byte, with `Idempotent-Replayed: true`.
- **A different request with the same key** gets `422 idempotency_key_reused`.
- **A request with that key still in flight** gets `409 request_in_progress`.

Completed responses are also cached in Redis (`idem:ledger:<client_id>:<sha256(key)>`, expiring with the Postgres row), so most retries are answered without touching Postgres. Postgres stays the authority:
- **Redis down:** writes and replays still work, and each failed Redis call logs a `dependency.call` warning.
- **Cost:** each write waits out two 100 ms cache timeouts, so writes take about 190 ms instead of 5 ms (Chaos Runbook §6.2).

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

### Ops read routes

These are for the web console and other ops tooling. They read across accounts, need only `read`, and write nothing; the access log covers them.

**Never in a response:**
- `reference` and `metadata`, which are caller-supplied free text;
- the `reason` of a freeze or unfreeze, which is operator-typed free text.

A failure's `reason` is kept, because it's a fixed code.

| Method | Path | Success |
| :--- | :--- | :--- |
| GET | `/internal/v1/accounts?limit=&cursor=` | `200 {"accounts":[…],"next_cursor"?}`: accounts with their ledgers, in creation order. With `?user_id=` instead, the route returns that one account, as before |
| GET | `/internal/v1/transactions?limit=&cursor=&status=&type=&since=` | `200 {"transactions":[…],"next_cursor"?}` across accounts, **newest change first** (`updated_at`). Filters: `status` (`PENDING`, `SETTLED`, `FAILED`, `VOIDED`), `type` (`CREDIT`, `DEBIT`, `VOID`), `since` (RFC 3339) |
| GET | `/internal/v1/transactions/:transaction_id/detail` | `200 {"transaction","events","journal","outbox"}`: the event stream, the journal lines with their ledger kind, and each event's outbox row (`published_at` is null until relayed; rows are purged after `OUTBOX_RETENTION`) |
| GET | `/internal/v1/accounts/:account_id/events` | `200 {"events":[…]}`: opened, ledger opened, frozen and unfrozen events, with `actor` and `rule` |
| GET | `/internal/v1/stats?since=` | `200 {"since","transactions":[{type,status,failure_reason,count}],"pending":{count,oldest_age_ms},"outbox":{pending,oldest_age_ms}}`. `since` defaults to 10 minutes ago, and counts transactions changed since then. "Pending" counts `PENDING` only: `PROCESSING` is never committed, because Tx2 passes through it inside one database transaction |

**Paging:** `limit` is 1–100 (default 20), and the cursor is opaque.

**Changes between polls:** the transaction feed is ordered by last change, so a transaction that changes moves to the top:
- a page read after the change doesn't repeat it;
- the next poll of the first page finds it.

The compose `console` client (`dev-console-token`) has `read` only.

A `202` means the request is durable but not finished: Tx1 committed and Tx2 didn't complete within `LEDGER_PROCESS_TIMEOUT`. ledger-worker's sweeper completes it, and a retry with the same key returns the final answer.

## User routes

End users call these with the `access_token` from auth-api's `/api/v1/auth/login`, and ledger-api checks it against the same Redis sessions.
- **Whose account:** it always comes from the session's `user_id`, so no route takes an `account_id`.
- **Someone else's transaction:** it's a `404`, never a `403`.

| Method | Path | Success |
| :--- | :--- | :--- |
| GET | `/api/v1/ledger/account` | `200` the caller's account with a balance per currency, or `404 account_not_found` |
| GET | `/api/v1/ledger/transactions?currency=&limit=&cursor=` | `200 {"transactions":[…],"next_cursor"?}`, newest first. `limit` is 1–100 (default 20). Pass `next_cursor` back as `cursor` for the next page; its absence means the last page |
| GET | `/api/v1/ledger/transactions/:transaction_id` | `200` transaction, or `404 transaction_not_found` if it isn't the caller's |

**Paging:** pages are keyset-paginated on `(created_at, id)`, so a transaction written while you page doesn't shift or repeat later pages.

**Without Redis:** Redis isn't part of ledger-api's `/readyz`. If Redis goes down, the user routes return `503`, while the internal routes keep serving on Postgres-only idempotency.

## Configuration (TRD §12.2)

ledger-api also reads, as auth-api does ([Auth API Reference](<2026-11-10 Auth API Reference.md>)):
- `DATABASE_URL`;
- `REDIS_*`: auth-api's session store, for the user routes;
- `LOG_LEVEL`;
- `SERVICE_NAME`, default `ledger-api`;
- `SHUTDOWN_TIMEOUT`.

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

ledger-worker and fraud-engine settings are listed in `docker-compose.yml` and in TRD §12.2.
