# Development Plan: Web Console (Ledger Trust Monitor)

| | |
| :--- | :--- |
| **Status** | Draft for review |
| **Date** | 2026-11-10 |
| **Source** | [Web Console Design](<2026-11-10 Web Console Design - Ledger Trust Monitor.md>), [TRD](<2026-11-06 TRD - Immutable Ledger & Event-Sourced Balance System.md>) |
| **Mockup** | [`mockups/2026-11-10 console-monitor-v1.html`](mockups/2026-11-10%20console-monitor-v1.html) |
| **Repo** | `github.com/dydanz/fintech-ledger-playground` |

This plan breaks the design's §8 delivery into four phases, C1–C4.
- **One phase = one GitHub issue.**
- **One issue = one or more PRs.**
- Every PR is small enough to review in one sitting, and leaves `main` green and deployable.

Design sections are cited as `D§n` and TRD sections as `§n`.

---

## 1. Phase overview

| Phase | Issue title | PRs | Depends on | Size | Exit criteria |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **C1** | `[Console C1] Backend prerequisites: password change, ops read routes` | 2 (parallel) | — | M | D§7.1 and D§7.2 shipped, with tests |
| **C2** | `[Console C2] Console foundation: service, SPA, auth pages, My account` | 3 (stack) | C1.1 (for 2.2) | M | A user can register, log in, change their password, open an account and move demo money, in the browser |
| **C3** | `[Console C3] Trust monitor: labels, incidents, Monitor, Transaction, Accounts` | 3 (stack) | C1.2, C2.1 | L | A viewer can follow a chaos drill on the Monitor and open any transaction's audit trail |
| **C4** | `[Console C4] Evidence and docs` | 1 | C3 | S | Screenshots from a live drill, plus the README, the Operations Guide and the design status updated |

### 1.1 Dependency graph

```
C1.1 (password) ─────────────┐
                             ▼
C2.1 (service + SPA) ──▶ C2.2 (auth pages) ──▶ C2.3 (My account)
   │
   └──────────┐
              ▼
C1.2 (ops routes) ──▶ C3.1 (labels, incidents, ops API) ──▶ C3.2 (Monitor) ──▶ C3.3 (Transaction, Accounts) ──▶ C4
```

- C1.1, C1.2 and C2.1 can start at once.
- C2.2's password page needs C1.1. The other auth pages don't, so C2.2 can open first with the password form disabled.
- C3.1 needs both C1.2 (data) and C2.1 (the service it lives in).

---

## 2. Working conventions

The same as the [ledger Development Plan](<2026-11-06 Development Plan - Immutable Ledger & Event-Sourced Balance System.md>) §2, with these changes.

### 2.1 Issues

- **Title:** `[Console Cn] <name>`.
- **Labels:** `console`, `phase:Cn`, plus `blocked` while an open question (§3) gates it.
- **Body:** §4 holds the body for each issue: the PR table, the acceptance checklist and the blocking questions, copied as is.

### 2.2 PRs

- **Branches:** `<type>/<short-kebab-desc>`, from a freshly pulled `main`, or stacked as in the ledger plan §2.2.
- **Body:**
  - `Part of #<issue>` on every PR;
  - `Closes #<issue>` on the last PR of the phase;
  - `Stacked on #<PR>` when stacked.
- **Evidence:** a `## Test evidence` section (`.claude/rules/pr-test-evidence.md`). Any PR that changes a screen attaches screenshots.

### 2.3 Definition of done (every PR)

1. `make vet test` passes. `make test-integration` passes for PRs that touch SQL, Redis or Kafka.
2. **Web PRs:** `make test-web` passes. It runs `tsc --noEmit` and Vitest in a Node container, because the host has no Node.
3. **Logging rule** (`.claude/rules/observability-logging.md` §7):
   - new events are emitted;
   - failure and denied paths are asserted in tests;
   - catalog rows are added in the same PR;
   - error-level events have a dashboard panel.
4. **Never-log list:**
   - nothing from the list appears in logs;
   - the console never logs request or response bodies;
   - the SPA never sends a service token anywhere.
5. **New env vars** are added to `internal/config`, `.env.example` and the Operations Guide.
6. **Departures from the design** are written back into the design in the same PR.

---

## 3. Open questions that gate work

These are the design's D§11 drafting decisions. Each is assumed unless the reviewer objects.

| Decision | Blocks | Proceed on the default? |
| :--- | :--- | :--- |
| A Go backend serves the SPA and holds the tokens (D§4) | C2.1 | Yes |
| Recovery and incident signals come from Loki, with no schema change (D§6.1) | C3.1 | Yes |
| No per-transaction "Retried" label, and no "0 lost" tile (D§6.3) | C3.1 | Yes |
| Ops screens have no login and are reachable only on 127.0.0.1 (D§2) | C3.2 | Yes, for the MVP. **Revisited after C3** (`feat/console-ops-role`): the ops screens need the ops role, granted with `make grant-role`, and the console checks the `Host` header (D§7.5) |
| A password change ends every session, including the current one (D§7.1) | C1.1 | Yes |
| The user's token is kept in `sessionStorage` (D§5.4) | C2.2 | Yes. An httpOnly cookie session is a later hardening step |
| Demo actions are on by default in compose, capped at IDR 1,000,000 (D§7.3) | C2.3 | Yes |

---

## 4. Phases

### C1: Backend prerequisites

**Goal.** Land the two backend changes the console needs, each useful on its own:
- password change in auth-api;
- read-only ops routes in ledger-api.

**Out of scope.** Any console code.

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 1.1 | `feat/auth-password-change` (`main`) | **Route:** `PUT /api/v1/users/password` `{"current_password","new_password"}` → `204` (D§7.1). **Session index:** `internal/session/redis.go` keeps a Redis set `session:user:<user_id>`, added to on `Create` (`SADD`, then `PEXPIRE` to the newest TTL) and removed from on `Delete`; `RevokeAll(ctx, userID)` deletes every session and the set. **Service:** `AuthService.ChangePassword(ctx, userID, current, next)`: verify `current` with bcrypt (wrong → `403 invalid_credentials`, nothing changes); validate `next` (8–72 bytes); compute the hash; `RevokeAll`; write the hash (`repository/users.go`); revoke the user's open `auth_token_history` rows (`repository/token_history.go`). **Logging:** catalog row `auth.password_change` (C1: success=info, denied=warn, failure=error; reasons `bad_password`, `invalid_input`, `pg_error`, `session_store_error`, `hash_failed`; partial=error `audit_revoke_failed`), and an error panel in `auth-api.json` | Service: a wrong current password changes nothing and ends no session; a success ends every session (log in twice, then both tokens get `401`); a short new password gets `400 invalid_request`. API: the event is asserted per outcome, with no password or token in the line. Integration: the index is cleaned on logout, and `RevokeAll` against real Redis |
| 1.2 | `feat/ledger-ops-read-routes` (`main`, parallel to 1.1) | **Routes** (D§7.2), all with `read` scope: `GET /internal/v1/accounts` without `user_id` lists accounts (keyset on `id`; with `user_id` it behaves as today); `GET /internal/v1/transactions?limit=&cursor=&status=&type=&since=` (keyset on `(updated_at, id)`, on `transactions_updated_idx`); `GET /internal/v1/transactions/:id/detail` (the transaction, its events with `occurred_at` and payload minus `reference`/`metadata`, its journal lines, and its outbox `published_at`); `GET /internal/v1/accounts/:id/events`; `GET /internal/v1/stats?since=` (counts by type, status and reason; outbox pending and oldest age; PENDING and PROCESSING count and oldest age). `limit` 1–100, default 20. **Client:** `docker-compose.yml` `LEDGER_CLIENTS` gains `console` with `read` only. Reads emit no events | API: 25 rows paged by 10 give 25 unique ids; a PENDING row that settles between pages appears once; the detail omits `reference` and `metadata`; a `payments` token gets `403`. Integration: each query against real Postgres. **The PR records `EXPLAIN ANALYZE`** for the list and stats queries on a `make load PROFILE=quick` database |

**Acceptance.**
- [ ] A password change ends every session of the user; a wrong current password changes nothing.
- [ ] `auth.password_change` is in the catalog, countable by `outcome`, with an error panel.
- [ ] The ops routes page stably, never return `reference` or `metadata`, and refuse clients without `read`.
- [ ] The list and stats queries use an index (plans in the PR).

---

### C2: Console foundation

**Goal.**
- A running `ledger-console` that serves the React app.
- The auth pages.
- My account with demo actions, so a person can try the ledger from a browser.

**Out of scope.** The ops screens (C3).

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 2.1 | `feat/console-service` (`main`) | `cmd/ledger-console` (config, graceful shutdown, `healthcheck`), and `internal/console` with `config.go` and the static-file handler. `web/`: Vite + React 19 + TypeScript, React Router, TanStack Query, plus a layout shell with the nav from the mockup. **Docker:** a Node build stage copies `web/dist` into the image (`CMD=ledger-console`). **Compose:** `ledger-console` on `127.0.0.1:8090`, `logging: alloy`, `SERVICE_NAME=ledger-console`. **Make:** `make test-web` (typecheck and Vitest in `node:22-alpine`) | `healthz` and `readyz`; the SPA's `index.html` is served for unknown paths (client routing); `make up` builds the image with no Node on the host; `make test-web` runs |
| 2.2 | `feat/console-auth-pages` (stacked on 2.1) | `internal/console/proxy.go`: `/console/api/auth/*`, `/console/api/profile`, `/console/api/password`, passed through to auth-api with the error envelope unchanged. **SPA:** `Login`, `Register`, `Profile` (name, bio, password change), logout, `src/auth/session.ts` (token in `sessionStorage`), and a route guard. **Logging:** `dependency.call` for failed upstream calls (`dependency=auth-api`; `timeout`, `refused`, `unknown`) | Go: an upstream `401` is passed through, and nothing is logged beyond the access log. Web: a wrong password shows the message; a `401` from any call clears the token and goes to `/login`; after a password change the user is sent to `/login` |
| 2.3 | `feat/console-my-account` (stacked on 2.2) | `internal/console/me.go`: `/console/api/me/account`, `/me/transactions`, `/me/open`, `/me/credit`, `/me/debit`. The user's id comes from auth-api's `GET /api/v1/users/profile` with the user's own token, so the browser never names an account. The console holds the `onboarding` and `payments` tokens (compose env). Every action sends a fresh `Idempotency-Key`. `internal/console/validate.go`: integer amount, `1 ≤ amount ≤ 1000000`. `CONSOLE_DEMO_ACTIONS` (default `true` in compose). **SPA:** `MyAccount` (the "Open my account" button; balance; paged history; "Add demo money" and "Spend"). **Smoke:** `scripts/smoke-console.sh` and `make smoke-console` | Go: amount table (0, -1, 1.5, 1000001 rejected; 1 and 1000000 accepted); `/me/*` acts only on the caller's own account; actions are refused when `CONSOLE_DEMO_ACTIONS=false`. Web: the open button appears before an account exists. Smoke: register → login → open → top up → the balance shows. Four top-ups in 5 min freeze the account (the real fraud rule) |

**Acceptance.**
- [ ] `make up` serves the console at http://localhost:8090, with no Node on the host.
- [ ] Register, login, logout, profile edit and password change work in the browser.
- [ ] My account opens an account and moves demo money, and the fourth top-up within 5 minutes freezes it.
- [ ] No service token appears in the browser (checked in the network panel, with screenshots in the PR).

---

### C3: Trust monitor

**Goal.** The Monitor, Transaction and Accounts screens from the mockup, labelled from real data, so a viewer can follow a chaos drill (D§1).

**Out of scope.** Real-time push (polling every 3 s), ops login, and write actions on ops screens.

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 3.1 | `feat/console-signals` (`main`, after C1.2 and C2.1) | `internal/console/labels.go`: `Label(tx, events, recovered) Label`, which returns `Normal`, `Recovered`, `Rejected`, `Reversed`, `TimedOut` or `InProgress`, plus the "what happened" text (D§6.2). `internal/console/incidents.go`: `Incidents(lines, now) []Incident` for kafka, database, redis and worker, with start, end and plain-language text (D§6.4). `internal/console/loki.go`: a LogQL client with a timeout, logging `dependency.call` (`dependency=loki`). `internal/console/ops.go`: `/console/api/ops/monitor?window=`, `/ops/transactions`, `/ops/transactions/:id` (with the merged timeline), `/ops/accounts`, cached 2 s. When Loki fails, the response has `signals.available=false` and the ledger data still renders | Table tests, one case per label. A transaction that was recovered and then FAILED is `Recovered`, not `Rejected`. Incident tests on log lines copied verbatim from the chaos drills (runbook §5 and §6.1): Kafka starts at the first `kafka_unavailable` and ends when `outbox.lag` is under 5 s; a database incident from the 503 burst. Loki down: `signals.available=false`, tiles and feed still present |
| 3.2 | `feat/console-monitor` (stacked on 3.1) | **SPA `Monitor`:** tiles (transactions, the success ratio from §10 "Success ratio", needed recovery, the last reconciler result, outbox backlog), incident banners (active in yellow, resolved in green), the live feed (50 rows, label filters), the "how it was handled" bars, and the self-healing feed; no window selector (dropped in C3.1: a fixed 10 minutes, §5); 3 s polling; an "activity signals unavailable" note | Vitest: label chips, banner states, the Loki-down note. Live: `make chaos FAULT=kafka` with the console open. The banner turns active, then resolved, and the counts match the drill's `report.txt` |
| 3.3 | `feat/console-transaction-accounts` (stacked on 3.2) | **ledger-api:** `GET /internal/v1/accounts/:id/transactions` (`read`), since the cross-account list has no account filter. **SPA `Transaction`** (D§5.2): the timeline of events, recovery, outbox publish, fraud decision and "verified by the reconciler"; the double entry; the trace id, linked to Grafana Explore. **SPA `Accounts`** (D§5.3): the list, an account's balances, its frozen and unfrozen history, and its transactions | Vitest: timeline ordering, and journal totals shown equal. Live: the Recovered transaction from a `make chaos FAULT=postgres` run shows the sweeper step and settles once |

**Acceptance.**
- [ ] During a Kafka drill, the Monitor shows the incident while it's active and resolved afterwards. Its counts match the drill's report.
- [ ] During a Postgres drill, the refusals and the sweeper's recovery show; the recovered transaction's timeline has the sweeper step.
- [ ] No label or tile claims something the data can't show (D§6.3).
- [ ] With Loki stopped, the screens still render balances and transactions, with the note.
- [ ] `reference` and `metadata` never appear on any screen.

---

### C4: Evidence and docs

**Goal.** Make the console part of how the project is shown and run.

| PR | Branch (base) | Contents | Tests |
| :--- | :--- | :--- | :--- |
| 4.1 | `docs/console-evidence` (`main`) | `examples/console/`: screenshots from a Kafka drill and a Postgres drill (Monitor during and after, a Recovered transaction, an account). **README:** the console in the quick start and the services table. **Operations Guide:** a console section (URL, env vars, demo actions, what each label means). **Design:** status set to implemented, with any decisions changed while building | The link check over the changed docs; the screenshots match the drill output they came from |

**Acceptance.**
- [ ] A newcomer can start the stack, open the console and follow a drill from the README alone.
- [ ] `examples/console/` holds the screenshots, with a one-line caption each.

---

## 5. Risks

| Risk | Mitigation |
| :--- | :--- |
| LogQL queries are slow at load-test volume | Narrow selectors (`{service=…}` + `| json` on named fields only, as in `scripts/load/run.sh`), a 2 s cache, and the timeouts in 3.1 |
| `GET /internal/v1/stats` cost grows with the rows changed in the window: measured in C1.2 at 28,279 rows in 29 ms on `transactions_updated_idx`, so about 150 ms for 10 minutes at 300 TPS, but far too slow for 24 h | The Monitor's tiles use the short window. Longer windows (1 h–24 h) need a cheaper source before C3.2 offers them, e.g. hourly counts from the access log in Loki, or a cap on the selectable window. **Decided in C3.1:** the tiles cover a fixed 10 minutes and the signals look back 30 minutes; no window selector in the MVP |
| Loki's 7-day retention hides older incidents | Not shown while the Monitor looks back only 30 minutes; to be stated on a window selector if one is added. The ledger data isn't affected |
| A frontend toolchain the host lacks (no Node) | Everything web runs in containers: `make test-web` and the Docker build |
| Console load skews a load test | The console polls only while a tab is open; the cache absorbs extra tabs. Note it in the load results when the console is open during a run |
