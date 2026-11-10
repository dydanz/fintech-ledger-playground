# Web Console Design: Ledger Trust Monitor

**Status:** draft, MVP · **Date:** 2026-11-10 · **Framework:** React

## 1. Why

The ledger already copes with hard failures: Redis, Kafka or Postgres going down, a worker dying mid-batch, duplicate requests, frozen accounts. But today the proof sits in Grafana panels, SQL and `report.txt` files. A non-technical person watching a load test or a chaos drill can't see it.

The console is a browser UI that makes that proof visible. **Someone non-technical should leave a test run trusting three things:**

1. **Reliability:** money is handled correctly. Nothing is half-done or applied twice, and the books balance.
2. **Self-healing:** when something breaks, the system recovers by itself.
3. **Traceability and accountability:** every transaction has a permanent, step-by-step record of who did what, and when.

The console also gives end users the basic account pages: register, log in and out, edit their profile and change their password, and see their own balance and history.

## 2. Goals, non-goals, anti-goals

**Goals (MVP):**
- **Monitor screen:** live totals, incident banners in plain language, a live transaction feed with each row labelled by how it was handled, and a self-healing activity feed.
- **Transaction screen:** a timeline of every step, the double entry, and the ids to follow it into the logs.
- **Accounts screen:** a list of accounts with balances and status, linking to their transactions.
- **Auth pages:** register, login, logout, profile (`full_name`, `bio`) and password change.
- **My account:** the logged-in user's balance and paged history, plus demo actions to open an account and add or spend demo money.
- One `make up` starts it, with no Node.js needed on the host.

**Non-goals (MVP):**
- Writing to the ledger from the ops screens. They only read; the demo actions are on My account.
- Real-time push. Polling every 3 s is enough.
- Auth for the ops screens, beyond binding to 127.0.0.1.
- Mobile layout, theming, i18n, and accessibility beyond semantic HTML.
- Replacing Grafana for engineers. The console links to it.

**Anti-goals:**
- **Claiming what the system can't prove.** Every reassuring number or label must come from data: the database, the reconciler, or a log event. If the console can't show something, it leaves it out. For example, it can't show per-transaction retry counts (§6.3).
- **Putting a service token in the browser.** Tokens stay in the console's backend.
- **Becoming a second source of truth.** The console stores nothing. Postgres stays the authority, and the logs are the record of what happened.

## 3. Constraints

- **Repo conventions:**
  - Go backends follow `.claude/rules/observability-logging.md`.
  - Every PR carries a Test evidence section (`.claude/rules/pr-test-evidence.md`).
  - Work happens on branches with a PR per step (`.claude/rules/git-workflow.md`).
- **Ownership:** ledger-api owns the ledger schema. Nothing else reads `ledger.*` tables directly.
- **Logs:** they're the only signal source (the observability rule). Loki keeps 7 days of them.
- **No Node.js on the host:** the React build runs in a Docker multi-stage build. Local `npm run dev` is optional.
- **Bind addresses:** every published port binds to 127.0.0.1.

## 4. Architecture

```
browser (React SPA)
   │  same origin, http://localhost:8090
   ▼
ledger-console (new Go service: serves the built SPA + /console/api/*)
   ├──► api (auth-api)       register, login, logout, profile, password
   ├──► ledger-api /api/v1/ledger/*      user routes, with the user's own token
   ├──► ledger-api /internal/v1/*        read-only ops routes, with the console's read token
   │                                     (and onboarding/payments tokens for demo actions only)
   └──► Loki /loki/api/v1/query*         recovery, incident and reconciler signals
```

**Why a small Go backend in front of the SPA,** rather than the SPA calling the services directly:
- **Tokens:** it keeps the service tokens out of the browser.
- **Merging sources:** it combines Postgres-backed ledger data with Loki signals into one response per screen. Writing LogQL in the browser would be fragile.
- **Same origin:** with one origin there's no CORS setup on auth-api or ledger-api.
- **One place for display rules:** the labels and the plain-language text live in tested Go code.

**`ledger-console`:**
- `cmd/ledger-console` and `internal/console`, built by the existing `Dockerfile` (`CMD=ledger-console`) with a `healthcheck` subcommand.
- It serves the SPA's static files, built in a Node stage and copied into the image. The `Dockerfile` gains an optional web build stage, or the console gets its own `Dockerfile.console`.
- Compose service `ledger-console`, published at `127.0.0.1:8090`, with label `logging: alloy` and `SERVICE_NAME=ledger-console`.

**SPA** (`web/`):
- **Stack:** React 19 with TypeScript, built with Vite.
- **Libraries:** React Router for pages and TanStack Query for fetching and polling. No component library; plain CSS modules.
- **Tests:** Vitest and React Testing Library.

## 5. Screens

The reviewed mockup, [`mockups/2026-11-10 console-monitor-v1.html`](mockups/2026-11-10%20console-monitor-v1.html) (open it in a browser), is the baseline: Monitor and Transaction detail, with numbers from the chaos drills. Changes from it are noted.

### 5.1 Monitor (`/`)

**Tiles, for the selected window (default the last 10 minutes; 1 h, 6 h and 24 h selectable):**
- **Transactions:** a count, and the rate.
- **Handled correctly:** the TRD success ratio, `1 − (5xx + processing_timeout) / valid requests`. *Change from the mockup:* the mockup's sub-line "0 lost · 0 duplicated" is dropped, because the server can't prove it live. Instead it shows "0 stuck · 0 timed out".
- **Needed recovery:** transactions the sweeper completed, plus requests refused during an incident. The sub-line says whether everything has since completed: "0 still pending".
- **Books balanced:** the last reconciler result (clean, or N mismatches) and how long ago it ran.
- **Events to Kafka:** pending outbox rows and the age of the oldest.

**Incident banners:** one per detected incident (§6.4).
- **While active:** yellow, with the plain-language effect and what the system is doing about it.
- **When resolved:** green, with the duration and the outcome.

**Live transactions:** the newest 50 by last change, filterable by label (§6.2). Each row shows:
- time, account (short id), type, amount, label;
- a one-line "what happened";
- a click-through to the transaction screen.

*Change from the mockup:* there's no per-transaction "Retried" label, because retries aren't linked to transactions (§6.3).

**How it was handled:** bars per label for the window.

**Self-healing activity:** the newest 20 recovery events (§6.4), in plain language.

### 5.2 Transaction (`/transactions/:id`)

- **Header:** type, amount, account, label.
- **Timeline:** every event of the transaction stream, each with its time and its actor (`requested_by`, or the sweeper). The time of each step comes from the event's `occurred_at`. The steps come from three places:
  - **The ledger:** the transaction's events (Requested, Processing, Settled, Failed, Voided).
  - **The logs:**
    - the sweeper's `ledger.recover`, when it completed the transaction;
    - the fraud engine's `fraud.decision`, if a freeze followed.
  - **Derived:**
    - the outbox publish time;
    - "verified by the reconciler" when a clean `ledger.reconcile` run finished after the transaction settled.
- **Double entry:** the journal lines (ledger, debit or credit, amount), with the totals shown equal.
- **Traceability:**
  - the transaction id;
  - the trace id, with a link to Grafana Explore filtered by it;
  - `requested_by`;
  - for a VOID or a voided transaction, the original or the void, linked.
- **Never shown:** `reference` and `metadata`, which are caller-supplied free text (TRD §11).

### 5.3 Accounts (`/accounts`, `/accounts/:id`)

A paged list of accounts showing short id, status (ACTIVE or FROZEN), balance per currency, and last activity. An account's page shows its balances, its status history (frozen and unfrozen, by whom, under which rule) and its transactions.

### 5.4 Auth and My account (`/login`, `/register`, `/profile`, `/me`)

- **Register, login, logout:** these call auth-api. The token is held in memory and in `sessionStorage`, so it's cleared when the tab closes. It's sent only to the console's own `/console/api/auth/*` and `/console/api/me/*`.
- **Profile:** edit `full_name` and `bio`, and change the password (§7.1). After a password change, every session ends and the user logs in again.
- **My account:**
  - **Before an account exists:** an "Open my account" button.
  - **After:**
    - the balance;
    - paged history from the user routes;
    - **Demo actions** (§7.3): "Add demo money" (a credit) and "Spend" (a debit).

  Four top-ups within 5 minutes trigger the real fraud freeze, so the demo shows it end to end.

## 6. How transactions are labelled

### 6.1 Sources

| Signal | Source |
| :--- | :--- |
| Status, failure reason, amounts, times | ledger-api read routes (Postgres) |
| Event stream, journal lines | new ledger-api detail route (§7.2) |
| Completed by the sweeper | Loki `ledger.recover` (carries `transaction_id`) |
| Retries answered from the record | Loki `ledger.idempotency` `outcome=success` (no `transaction_id`: totals only) |
| Refusals during an outage | Loki ledger-api access log `status=503` and `ledger.credit`/`ledger.debit` `reason=pg_error` (totals only) |
| Reconciler result | Loki `ledger.reconcile` summaries |
| Outbox backlog | new ledger-api stats route (Postgres) |

### 6.2 Labels

| Label | Rule | "What happened" text (examples) |
| :--- | :--- | :--- |
| **Normal** | SETTLED, no `ledger.recover` for it | "Settled in 3 ms" |
| **Recovered** | Has a `ledger.recover` event, whether it ended SETTLED or FAILED | "Database stopped mid-way; the recovery job completed it after restart" |
| **Rejected** | FAILED, with a business reason: `insufficient_funds`, `account_frozen`, `not_voidable`, `ledger_not_found` | "Insufficient funds; balance unchanged", "Account frozen by the fraud engine" |
| **Reversed** | VOIDED, or a SETTLED VOID | "Original kept; a reversing entry was added" |
| **Timed out** | FAILED `processing_timeout` | "Didn't complete in 15 min; failed safely, nothing moved" (shown red: needs attention) |
| **In progress** | PENDING or PROCESSING | "Processing" (red if older than 60 s, NFR-7) |

### 6.3 What the console can't show (and says so nowhere)

- **Retries per transaction:** a refused attempt never creates a transaction, and replays don't log the transaction id. So retries appear only as totals: "1,097 requests refused with 'try again'" and "N retries answered from the record, not applied again". *Possible later:* log `transaction_id` on replays.
- **"0 lost / 0 duplicated" end to end:** that needs the client's own record, which only the chaos harness has. The console shows the server-side guarantees instead: 0 stuck, 0 timed out, and a clean reconciler.

### 6.4 Incidents and self-healing events (from Loki)

| Incident | Starts when | Ends when | Banner while active → when resolved |
| :--- | :--- | :--- | :--- |
| Kafka unavailable | first `outbox.publish` `kafka_unavailable` | `outbox.lag` back under 5 s | "Payments keep working; events are queued and will be delivered" → "N queued events delivered after Kafka recovered (T s)" |
| Database unavailable | first ledger-api 503 `ledger_unavailable` | first 2xx write after it | "Payments are refused with 'try again'; nothing half-done is kept" → "N requests refused; M stuck payments completed by the recovery job" |
| Redis unavailable | first ledger-api `dependency.call` `dependency=redis` | none for 30 s | "Payments keep working without the cache, a little slower" → duration |
| Worker leader lost | `app.leader` `connection_lost`, or a replica stops | `app.leader` `acquired` for that job | → "Backup worker took over all jobs in T s" |

**Self-healing feed:**
- `ledger.recover`;
- `app.leader` `acquired` after a loss;
- the outbox draining after a Kafka incident;
- the first successful write after a database incident.

The thresholds above (30 s, 5 s) are starting values for the builder to tune against the recorded drills.

## 7. Backend work

### 7.1 auth-api: password change (new)

- **Route:** `PUT /api/v1/users/password`, Bearer, with body `{"current_password","new_password"}`. It answers `204`, or `401` for a wrong current password, `400 invalid_input` (8–72 bytes, as at registration) or `503`.
- **Steps:**
  1. Verify the current password with bcrypt.
  2. Update the hash.
  3. End **every** session of the user.
- **Session index:** ending every session needs a per-user index. A Redis set `session:user:<user_id>` of token hashes is added to on login and removed from on logout, with a TTL kept at least as long as the newest session's. Ending them all then means:
  - deleting each `session:auth:<hash>` and the set;
  - setting `revoked_at` on the user's open `auth_token_history` rows.
- **Logging:** a new catalog event `auth.password_change` (C1):
  - success=info, denied=warn, failure=error;
  - reasons `bad_password`, `invalid_input`, `pg_error`, `session_store_error`;
  - an error-level panel in the auth-api dashboard;
  - tests for each path.

### 7.2 ledger-api: read-only ops routes (new; operation `read`)

| Route | Returns |
| :--- | :--- |
| `GET /internal/v1/accounts?limit=&cursor=` | accounts with status and balances, keyset-paged |
| `GET /internal/v1/transactions?limit=&cursor=&status=&type=&since=` | transactions across accounts, newest change first (on the existing `transactions_updated_idx`) |
| `GET /internal/v1/transactions/:id/detail` | the transaction, its event stream (type, version, `occurred_at`, payload without `reference`/`metadata`), its journal lines, and its outbox `published_at` |
| `GET /internal/v1/accounts/:id/events` | the account's event stream (opened, frozen, unfrozen, with actor and rule). *Changed in C1.2:* the freeze and unfreeze `reason` is left out, because it's free text an operator typed |
| `GET /internal/v1/stats?since=` | counts by type, status and failure reason; outbox pending and oldest age; the PENDING count and the oldest age (*changed in C1.2:* PROCESSING is never committed, since Tx2 passes through it inside one database transaction, so it isn't counted) |

These are reads, so they emit no new events; the access log covers them. A new client `console` gets only `read` in `LEDGER_CLIENTS`.

### 7.3 Demo actions

The console holds:
- the `onboarding` token, to open the logged-in user's account. The user's id comes from auth-api's `GET /api/v1/users/profile`, called with the user's own token;
- the `payments` token, for credits and debits on that user's own account, with the account resolved from the user's session. Amounts are capped at IDR 1,000,000 per action.

Each action sends a fresh `Idempotency-Key` per click. Demo actions are on when `CONSOLE_DEMO_ACTIONS=true`, the compose default, and they never act on another user's account.

### 7.4 `ledger-console` API (`/console/api/*`)

| Area | Routes |
| :--- | :--- |
| Auth | `/auth/*`, `/profile`, `/password`: pass-throughs to auth-api |
| My account | `/me/*`: the user routes plus the demo actions |
| Ops | `/ops/monitor?window=`: tiles, incidents, feed, bars and self-healing, in one response |
| Ops | `/ops/transactions?…` and `/ops/transactions/:id`: the detail, with the timeline already merged |
| Ops | `/ops/accounts…` |

`/ops/*` responses are cached for 2 s, so many open tabs don't multiply the load on ledger-api and Loki.

**Logging:** access log; `dependency.call` for failed ledger-api and Loki calls (reasons `timeout`, `refused`, `unknown`); `app.lifecycle`. When Loki is down, the screens still render from ledger-api data, and an "activity signals unavailable" note replaces the log-backed parts.

## 8. Delivery (one PR each)

The [Development Plan](<2026-11-10 Development Plan - Web Console.md>) breaks this into four phases (C1–C4) and nine PRs, with tests and acceptance per PR. Where the two differ, the plan is current.

1. `feat/auth-password-change`: §7.1.
2. `feat/ledger-ops-read-routes`: §7.2.
3. `feat/console-skeleton`: `ledger-console` service, SPA scaffold and Docker build, the auth pages and My account with demo actions (§5.4), and `make smoke-console`.
4. `feat/console-monitor`: the labelling and incidents (§6), and the Monitor, Transaction and Accounts screens.
5. Docs: the README and Operations Guide entries, and screenshots of the console during a chaos drill attached to the PR.

## 9. Testing

- **Go:**
  - unit tests for the label and incident rules, using recorded log lines and rows from the chaos drills;
  - integration tests for the new ledger-api routes and the password change;
  - log-event assertions per the observability rule.
- **SPA:** Vitest and React Testing Library for the label chips, the banners and the timeline.
- **End to end:**
  - `make smoke-console`: register, login, open an account, top up, see it on My account and on Monitor;
  - a chaos drill (`FAULT=postgres`) with the console open, with screenshots in the PR.

## 10. Left to the builder

- Component structure, styling details, and the exact copy of the "what happened" texts. Keep them plain and short.
- The page sizes, the polling interval (about 3 s) and the cache TTL.
- How the SPA build is wired into Docker (a stage in the existing `Dockerfile`, or `Dockerfile.console`).
- The exact LogQL, and the incident thresholds in §6.4.

## 11. Decisions made in drafting (for review)

- A Go backend for the console rather than an SPA-only frontend or an nginx proxy (§4).
- Loki as the source of recovery and incident signals, rather than new database columns (§6.1). The ledger stays unchanged.
- Labels drop "Retried" per transaction, and the tiles drop "0 lost / 0 duplicated" (§6.3).
- The ops screens are open to anyone who reaches 127.0.0.1:8090, with no login, for the MVP.
- A password change ends all of the user's sessions, including the current one.
- Demo actions live on My account, on by default in compose, capped per action.
- The browser token is kept in `sessionStorage`. An httpOnly cookie session held by the console backend is a later hardening step.
