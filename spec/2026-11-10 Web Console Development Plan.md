# Web Console Implementation Plan

> **For agentic workers:** implement one task at a time, in the order in §3. Steps use checkbox (`- [ ]`) syntax for tracking. Each task is one PR.

**Goal:** build the Ledger Trust Monitor console (ops view, transaction timeline, accounts) and the basic user pages (register, login, logout, profile, password change, my account with demo actions), as specified in [Web Console Design](<2026-11-10 Web Console Design - Ledger Trust Monitor.md>).

**Architecture:** a new `ledger-console` Go service serves a React SPA and proxies to auth-api and ledger-api. It holds the service tokens; the browser never does. The monitor's recovery and incident signals come from Loki; balances and transactions come from ledger-api read routes.

**Tech Stack:** Go 1.26 (Gin, pgx, go-redis, franz-go as today); React 19 with TypeScript, Vite, React Router, TanStack Query; Vitest and React Testing Library; Docker multi-stage build (no Node on the host).

**Spec:** [Web Console Design](<2026-11-10 Web Console Design - Ledger Trust Monitor.md>) (the decisions are in its §11). Read it alongside this plan.

## 1. Global constraints

Every task inherits these. They're taken from the design and the repo rules.

- **Ports:** every published port binds to `127.0.0.1`. The console is `127.0.0.1:8090`.
- **Tokens:** service tokens (`onboarding`, `payments`, `read`) exist only in the console's environment. The browser holds only the user's own access token, in `sessionStorage`.
- **Read-only ops:** `/console/api/ops/*` never writes. The only writes are the demo actions on the logged-in user's own account.
- **Demo caps:** amounts are integer minor units, `1 ≤ amount ≤ 1000000`, per action. `CONSOLE_DEMO_ACTIONS=true` in compose.
- **Polling:** the SPA polls every 3 s. The console caches `/ops/*` responses for 2 s.
- **Events:** reads emit no new events. The new password change is a C1 event, and any new `event`/`reason` goes into the catalog in the same PR (observability rule §7).
- **Never logged:** `reference`, `metadata`, raw `Idempotency-Key`, tokens or their hashes, passwords (rule §6).
- **PR hygiene:** a fresh branch from `main` named `<type>/<short-desc>`, a `## Test evidence` section with the real output, screenshots for UI changes, and the `Co-Authored-By` trailer.
- **Loki retention:** 7 days (`deploy/loki`). Incident history older than that isn't shown.

## 2. Review Focus

The five inputs most likely to break what a person sees. Each has a test in the task that owns it.

1. **Wrong current password on change:** `401 invalid_credentials`, and no session is ended. (Task 1)
2. **Expired token mid-session:** every `/console/api/*` call returns `401`, and the SPA sends the user to login, not a blank page. (Task 3)
3. **Loki down or slow:** the monitor still renders balances and transactions, with a visible "activity signals unavailable" note. (Task 4)
4. **Transactions that change between two polls:** a PENDING row that settles, or a new row, doesn't duplicate or skip rows in the list. Pagination uses a stable `(updated_at, id)` cursor. (Tasks 2 and 4)
5. **Demo amounts out of range:** `0`, negative, non-integer, or above the cap are rejected in the console before reaching ledger-api. (Task 3)

## 3. Order and dependencies

| # | Task | Depends on | Repo PR branch |
| :--- | :--- | :--- | :--- |
| 1 | Password change in auth-api | – | `feat/auth-password-change` |
| 2 | Read-only ops routes in ledger-api | – | `feat/ledger-ops-read-routes` |
| 3 | Console skeleton, auth pages, My account | 1 (for the password page) | `feat/console-skeleton` |
| 4 | Monitor, Transaction and Accounts screens | 2, 3 | `feat/console-monitor` |
| 5 | Docs, screenshots, chaos evidence | 4 | `docs/console-runbook` |

Tasks 1 and 2 can run in parallel. Task 3 can start with the password page stubbed until Task 1 merges.

---

## Task 1: Password change in auth-api

**Files:**
- Modify: `internal/session/redis.go`: maintain the per-user index (`session:user:<user_id>`).
- Modify: `internal/repository/users.go`: `UpdatePasswordHash(ctx, userID, hash)`.
- Modify: `internal/repository/token_history.go`: `RevokeAllForUser(ctx, userID)`.
- Modify: `internal/service/auth.go`: `ChangePassword`.
- Modify: `internal/api/handlers.go`, `internal/api/router.go`: `PUT /api/v1/users/password`.
- Modify: `internal/api/api_test.go`, `internal/service/auth_test.go`, `internal/session/redis_test.go`.
- Modify: `.claude/rules/observability-logging.md`: catalog row for `auth.password_change`.
- Modify: `deploy/grafana/dashboards/auth-api.json`: error-level panel.

**Interfaces:**
- Produces: `func (s *AuthService) ChangePassword(ctx context.Context, userID, currentTokenHash, current, next string) error`. Sentinels: `ErrBadPassword`, `ErrInvalidInput`.
- Produces: `func (s *RedisStore) RevokeAll(ctx context.Context, userID string) (int, error)`.
- Consumes: the existing `Delete` and `Revoke`.

**Behaviour**
- The session index is written on `Create` (`SADD`, then `PEXPIRE` to the newest TTL) and cleaned on `Delete`.
- `ChangePassword` runs in this order: verify `current` with bcrypt (wrong → `ErrBadPassword`, nothing else changes); validate `next` (8–72 bytes, as at registration); write the new hash; `RevokeAll` sessions; `RevokeAllForUser` audit rows.
- The response is `204`. The current token is revoked too, so the caller must log in again.

- [ ] **Step 1: Write the failing tests.** In `internal/service/auth_test.go`: `TestChangePasswordWrongCurrentChangesNothing`, `TestChangePasswordEndsEverySession` (two logins, then assert both tokens are rejected), `TestChangePasswordRejectsShortNew`. In `internal/api/api_test.go`: `TestPasswordChangeEmitsEvent` asserts `event=auth.password_change`, `outcome=denied`, `reason=bad_password` on a wrong current password, with no password or token in the line.
- [ ] **Step 2: Run them and watch them fail.** `make vet test` → FAIL: `ChangePassword` undefined.
- [ ] **Step 3: Implement** the session index, `RevokeAll`, the repository methods, `ChangePassword`, and the handler and route.
- [ ] **Step 4: Run them and watch them pass.** `make vet test` → PASS. `make test-integration` → PASS, including a new `TestSessionIndexCleanedOnDelete` against Redis.
- [ ] **Step 5: Update the catalog and dashboard**, then re-run `make check-alerts`.
- [ ] **Step 6: Smoke test** with a running stack: `make up`, then change a password with curl and check the old token returns `401`.
- [ ] **Step 7: Open the PR** with evidence for each step.

## Task 2: Read-only ops routes in ledger-api

**Files:**
- Modify: `internal/api/ledger_router.go`: new routes, and `GET /internal/v1/accounts` lists when `user_id` is absent (today `FindAccount` requires it).
- Modify: `internal/api/ledger_handlers.go`: `ListAccounts`, `ListTransactions`, `TransactionDetail`, `AccountEvents`, `Stats`.
- Modify: `internal/service/ledger_views.go`: views that drop `reference` and `metadata`.
- Modify: `internal/repository/ledger.go`: keyset queries; reads use `transactions_updated_idx` and `transactions_account_created_idx`.
- Modify: `docker-compose.yml`: `LEDGER_CLIENTS` gains `console:<sha256>:read`.
- Create: `internal/api/ledger_ops_test.go`, `internal/repository/ledger_ops_integration_test.go`.

**Interfaces:**
- Produces, all with `read` scope:
  - `GET /internal/v1/accounts?limit=&cursor=`
  - `GET /internal/v1/transactions?limit=&cursor=&status=&type=&since=`
  - `GET /internal/v1/transactions/:id/detail`: the transaction, its events (type, version, `occurred_at`, payload minus free text), its journal lines, and its outbox `published_at`
  - `GET /internal/v1/accounts/:id/events`
  - `GET /internal/v1/stats?since=`
- Cursor: an opaque base64 of `(updated_at, id)` for transactions, `(id)` for accounts.

**Behaviour**
- Reads write no events. The access log covers them.
- A non-`read` client gets `403`, as other internal routes do.
- Bounds: `limit` 1–100, default 20.

- [ ] **Step 1: Write the failing tests.** `TestOpsTransactionsPagesWithoutRepeats` (create 25 rows, page by 10, expect 25 unique ids); `TestOpsTransactionsStableWhenRowSettles` (a PENDING row settles between pages; it appears once); `TestOpsDetailOmitsFreeText` (`reference` and `metadata` absent); `TestOpsRequiresReadScope` (a `payments` token gets `403`).
- [ ] **Step 2: Run them and watch them fail.** `make vet test` and `make test-integration` → FAIL (routes return 404).
- [ ] **Step 3: Implement** the routes, queries and views.
- [ ] **Step 4: Run them and watch them pass.** Both suites → PASS.
- [ ] **Step 5: Measure** the list and stats queries on the load database: `make load-up`, `make load PROFILE=quick`, then `EXPLAIN ANALYZE` each. Record the plans in the PR.
- [ ] **Step 6: Open the PR** with evidence and the query plans.

## Task 3: Console skeleton, auth pages, My account

**Files:**
- Create: `cmd/ledger-console/main.go` (with a `healthcheck` subcommand).
- Create: `internal/console/config.go`, `internal/console/proxy.go` (auth-api and ledger-api pass-through), `internal/console/me.go` (`/console/api/me/*`, demo actions), `internal/console/validate.go` (demo amount rules).
- Create: `web/` (Vite React TS): `src/api/client.ts`, `src/auth/session.ts` (`sessionStorage` token), `src/pages/Login.tsx`, `Register.tsx`, `Profile.tsx`, `MyAccount.tsx`, `src/App.tsx`, `src/routes.tsx`.
- Modify: `Dockerfile`: a Node build stage that copies `web/dist` into the image; `CMD=ledger-console`.
- Modify: `docker-compose.yml`: service `ledger-console`, `127.0.0.1:8090:8090`, `logging: alloy`, `SERVICE_NAME=ledger-console`, `CONSOLE_DEMO_ACTIONS`, and the `onboarding` and `payments` tokens.
- Modify: `Makefile`: `smoke-console`; `scripts/smoke-console.sh`.

**Interfaces:**
- Produces: `/console/api/auth/*`, `/console/api/profile`, `/console/api/password` (pass-through to auth-api); `/console/api/me/account`, `/console/api/me/transactions`, `/console/api/me/open`, `/console/api/me/credit`, `/console/api/me/debit`.
- The console resolves the user's account id from the session's `user_id` (via auth-api `GET /api/v1/users/profile`, with the user's token), so the browser never names an account.
- Demo action bodies: `{"amount": int}`. Every click sends a fresh `Idempotency-Key`.

**Behaviour**
- Error envelope `{"error":{"code","message","request_id"}}`, passed through unchanged from the upstream service.
- An upstream `401` is passed through, and the SPA treats it as "session ended".
- The console logs only the access log, `dependency.call` for failed upstream calls (`dependency=auth-api|ledger-api`, reasons `timeout`, `refused`, `unknown`), and `app.lifecycle`.

- [ ] **Step 1: Write the failing tests.** Go: `TestDemoAmountRules` (table: 0, -1, 1000001, 1.5 as JSON, 1 and 1000000 valid); `TestMeCreditsOnlyOwnAccount`; `TestUpstream401PassedThrough`. Web: `Login.test.tsx` (wrong password shows the message); `Session.test.ts` (token cleared on `401`); `MyAccount.test.tsx` (the open button shows before an account exists).
- [ ] **Step 2: Run them and watch them fail.** `go test ./internal/console/...` and `npm test` (in a container) → FAIL.
- [ ] **Step 3: Implement** the Go package, the SPA pages, the Dockerfile stage and the compose service.
- [ ] **Step 4: Run them and watch them pass.** Both test runs → PASS, then `make up`, then `make smoke-console` (register → login → open → top up → see the balance).
- [ ] **Step 5: Screenshots** of login and My account, attached to the PR.
- [ ] **Step 6: Open the PR.**

## Task 4: Monitor, Transaction and Accounts screens

**Files:**
- Create: `internal/console/labels.go`: the label rules from design §6.2, as a pure function over one transaction and its events.
- Create: `internal/console/incidents.go`: the incident detection from design §6.4, as a pure function over Loki-shaped lines.
- Create: `internal/console/loki.go`: a small LogQL client with a timeout and `dependency.call` on failure.
- Create: `internal/console/ops.go`: `/console/api/ops/monitor`, `/ops/transactions`, `/ops/transactions/:id`, `/ops/accounts`, with the 2 s cache.
- Create: `internal/console/testdata/`: log lines copied verbatim from the chaos drill logs quoted in the runbook §6.1 and the recovery events from the Postgres drill.
- Create: `web/src/pages/Monitor.tsx`, `Transaction.tsx`, `Accounts.tsx`, and `web/src/components/` (tiles, banners, label chips, timeline, journal table).

**Interfaces:**
- `func Label(t Transaction, ev []Event, recovered bool) Label`: returns one of `Normal`, `Recovered`, `Rejected`, `Reversed`, `TimedOut`, `InProgress`. `TimedOut` is `FAILED processing_timeout`; `InProgress` past 60 s is flagged.
- `func Incidents(lines []LogLine, now time.Time) []Incident` with `Active bool`, `Started`, `Ended *time.Time`, `Kind` (`kafka`, `database`, `redis`, `worker`).
- Monitor response: `{tiles, incidents, feed, bars, selfHealing, signals: {available bool}}`.

**Behaviour**
- Thresholds from §6.4 are starting values. The builder records the ones that the chaos drills cross or don't.
- When Loki fails, `signals.available` is false, and the incident and self-healing lists are empty with the note shown.
- The "Handled correctly" tile is the TRD success ratio over the window, from the stats route.

- [ ] **Step 1: Write the failing tests.** Go, table-driven: `TestLabelRules` (one case per label, plus the overlap: a recovered transaction that ended `FAILED` is `Recovered`, not `Rejected`); `TestIncidentKafkaFromRelayLines` (fixture lines from the drill → one active incident at the first `kafka_unavailable`, ended when `outbox.lag` returns under 5 s); `TestMonitorWithLokiDown` (`signals.available=false`, ledger data still present).
- [ ] **Step 2: Run them and watch them fail.**
- [ ] **Step 3: Implement** the pure functions first, then the handlers, then the screens.
- [ ] **Step 4: Run them and watch them pass**, plus Vitest for the chips and banners.
- [ ] **Step 5: Live check.** Run `make chaos FAULT=kafka` with the console open, and confirm the Kafka incident appears as active and then resolved, and that the screens match the drill's `report.txt`. Record that against the drill's output.
- [ ] **Step 6: Screenshots** during the drill, attached to the PR: Monitor during the incident, and a Recovered transaction.
- [ ] **Step 7: Open the PR.**

## Task 5: Docs, screenshots, chaos evidence

**Files:**
- Modify: `README.md`: the console in the quick start, the URL in the services table.
- Modify: `spec/2026-11-10 Operations Guide.md`: a console section.
- Create: `examples/console/` with the screenshots from Task 4.
- Modify: `examples/README.md`: the console screenshots and a one-line summary of each.
- Modify: `spec/2026-11-10 Web Console Design - Ledger Trust Monitor.md`: status to "implemented", with any decision that changed while building.

- [ ] **Step 1: Check every relative link** in the changed docs (the script from PR #40).
- [ ] **Step 2: Confirm the screenshots** match the drill output they came from.
- [ ] **Step 3: Open the PR** with the link-check output.

---

## 4. Review Focus tests, by task

| Review Focus line | Owning task | Test |
| :--- | :--- | :--- |
| Wrong current password | 1 | `TestChangePasswordWrongCurrentChangesNothing` |
| Expired token mid-session | 3 | `Session.test.ts`: token cleared on `401` |
| Loki down or slow | 4 | `TestMonitorWithLokiDown` |
| Transactions changing between polls | 2, 4 | `TestOpsTransactionsStableWhenRowSettles` |
| Demo amounts out of range | 3 | `TestDemoAmountRules` |

## 5. Left to the builder

- Component structure, styling and the exact "what happened" copy (plain and short).
- Page sizes, and the Loki query shape, within the limits in §1.
- Whether the SPA is built in the existing `Dockerfile` or a `Dockerfile.console`.
