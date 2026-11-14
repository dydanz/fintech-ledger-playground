# Operations Guide

How to run, observe, migrate and test the stack locally. API details are in the [Auth API Reference](<2026-11-10 Auth API Reference.md>) and the [Ledger API Reference](<2026-11-10 Ledger API Reference.md>).

## 1. Services

`make up` builds and starts everything with `docker compose`. It then reloads Grafana's alert rules (`scripts/grafana-reload.sh`), because Grafana only reads them at startup.

**Compose project name:** the project is still called `auth-api` (`name:` in `docker-compose.yml`), so the data volumes are `auth-api_pgdata`, `auth-api_kafkadata` and so on. The name was kept on purpose when the project became `fintech-ledger-playground`, because changing it would make Docker start on new, empty volumes. Throwaway runs override it with `COMPOSE_PROJECT_NAME=…`.

| Service | Where | What it does |
| :--- | :--- | :--- |
| api (auth-api) | http://localhost:8080 | Registration, login, sessions, profile |
| ledger-api | http://localhost:8081, bound to 127.0.0.1 | Ledger accounts and transactions: internal routes for trusted services, user routes for end users |
| ledger-console | http://localhost:8090, bound to 127.0.0.1 | The web console: a React app (`web/`) served by a Go server (`cmd/ledger-console`), built by `Dockerfile.console`. Client routes get the app shell; `/console/api/*` is JSON only. `/console/api/auth/*`, `/profile` and `/password` pass through to auth-api (`CONSOLE_AUTH_API_URL`) with the user's own token; `/console/api/me/account` and `/me/transactions` pass through to ledger-api's user routes (`CONSOLE_LEDGER_API_URL`). Demo actions (`/me/open`, `/me/credit`, `/me/debit`; `CONSOLE_DEMO_ACTIONS`, on in compose) use `CONSOLE_ONBOARDING_TOKEN` and `CONSOLE_PAYMENTS_TOKEN` on the caller's own account, IDR, 1 to 1,000,000 per action, and log `console.demo_action`. The ops routes need a signed-in user with the **ops role**: grant it with `make grant-role EMAIL=<email>` (`ROLE=user` takes it back). Without it they answer `401` (not signed in) or `403` (no role), and the app shows "Ops access needed". Calls to auth-api carry the browser's address, vouched for by `CONSOLE_PROXY_TOKEN` (the same value on both services), so console logins record it in `auth_token_history`; a mismatch is logged as `auth.proxy` `denied`. The console answers only the `Host` headers in `CONSOLE_ALLOWED_HOSTS` (default `localhost:8090,127.0.0.1:8090`; change it if you publish another port, or `*` behind a proxy that checks the host). The ops routes (`/console/api/ops/monitor`, `/ops/transactions[/:id]`, `/ops/accounts[/:id]`, the last with its transactions, `?cursor=` for older ones) read ledger-api with `CONSOLE_READ_TOKEN` (the read-only `console` client) and the activity signals from Loki (`CONSOLE_LOKI_URL`); a transaction's trace id links to Grafana Explore at `CONSOLE_GRAFANA_URL` (Grafana as the browser reaches it; no link when unset); responses are cached 2 s; without Loki they answer from the ledger data with `signals.available=false`. The Monitor screen (`/`) polls `/ops/monitor` every 3 s while its tab is visible |
| ledger-worker | no published port | Background jobs, each with one leader across replicas (a Postgres advisory lock), listed below |
| fraud-engine | no published port | Consumes `ledger.events.v1` and freezes accounts on two rules: `topup_velocity` (more than 3 top-ups in 5 min) and `debit_volume` (more than 30 debits, or over the currency limit, in 1 h) |
| Postgres 17 | `127.0.0.1:5432` | The authority: users, audit trail, ledger schema |
| Redis | `127.0.0.1:6379` | Sessions, idempotency cache, fraud windows |
| Kafka | `127.0.0.1:9094` from the host (`kafka:9092` in containers) | Topic `ledger.events.v1`, 12 partitions. `make kafka-topics` describes it |
| archive-store | `127.0.0.1:7070` | S3 API with Object Lock (Versity S3 gateway): the archiver's write-once bucket `ledger-archive` |
| Grafana | http://localhost:3000 (admin / admin) | Dashboards **auth-api overview** and **ledger overview**; alert rules in folder **ledger** |
| Loki · Alloy | http://localhost:3100 · http://localhost:12345 | Log store · log shipper |

**ledger-worker jobs:**

| Job | Interval | What it does |
| :--- | :--- | :--- |
| `outbox_relay` | on NOTIFY, or every 200 ms | Publishes outbox rows to Kafka and logs `outbox.lag` every 30 s |
| `outbox_purge` | 10 min | Deletes published outbox rows older than `OUTBOX_RETENTION` (72 h) |
| `sweeper` | 5 s | Completes transactions left PENDING (Tx1 committed, Tx2 didn't) |
| `idempotency_purge` | 1 min | Deletes expired idempotency keys |
| `reconciler` | `RECONCILE_INTERVAL` (1 min), with a full sweep daily and on gaining leadership | Checks balances, sequences, double entries and projections against the journal and events |
| `partitions` | 1 h | Keeps monthly partitions 3 months ahead |
| `archiver` | 24 h | Partitions older than `LEDGER_ARCHIVE_AFTER` (7 years) go to Parquet in `ledger-archive`; each is verified, then detached and dropped. `LEDGER_LEGAL_HOLD` (account ids, or `*`) holds them back |

When the reconciler reports drift, `make rebuild-projections` recomputes the projections from the events and journal. It's a dry run by default; `ARGS=--apply` writes the changes.

**Grafana alert rules:**
- success ratio;
- outbox lag;
- stuck PENDING;
- fraud freeze failing;
- reconcile failure;
- partitions running out;
- archiver failing.

`make check-alerts` checks that the rules load, the dashboard queries run, and the alerts trip on synthetic log lines.

## 2. Logging and observability

Every log line is one JSON object on stdout:

```json
{"timestamp":"2026-11-05T10:00:00.123Z","level":"info","message":"http_request","service":"auth-api",
 "request_id":"…","trace_id":"…","span_id":"…","method":"POST","path":"/api/v1/auth/login",
 "route":"/api/v1/auth/login","status":200,"latency_ms":63.2,"client_ip":"172.18.0.1",
 "response_bytes":142,"user_agent":"curl/8.7.1"}
```

- **`request_id`** is taken from a well-formed inbound `X-Request-ID`, or generated, and echoed in the response header.
- **`trace_id` and `span_id`** come from a valid inbound W3C `traceparent` header, or new ones are generated. Either way, the `traceparent` in use is echoed in the response header.
- **Critical events** (login, ledger writes, freezes, outbox, reconciler and so on) carry `event`, `outcome` and `reason`. The event catalog and the rules are in `.claude/rules/observability-logging.md`.
- **Never logged:** the query string, tokens, raw `Idempotency-Key` values, and transaction `reference` or `metadata`.

**Pipeline:**
1. Containers write to Docker's json-file driver, with rotation.
2. **Grafana Alloy** discovers containers labelled `logging=alloy` via the Docker socket, parses the JSON, and promotes `level` and `service` to Loki labels.
3. Grafana provisions Loki as its data source, the dashboards and the alert rules.

Alloy is used instead of Promtail because Promtail reached end of life in March 2026, and Alloy is its official replacement. The Docker Loki log driver was avoided because it needs a plugin on every host and can block containers when Loki is down.

**Useful LogQL:**

```logql
{service="auth-api"} | json | status >= 500
{service="auth-api"} | json | message="login failed" | line_format "{{.reason}} {{.client_ip}}"
sum by (route) (rate({service="auth-api"} | json | message="http_request" [1m]))
{service="ledger-worker"} | json | event="outbox.lag"
```

## 3. Database migrations

[golang-migrate](https://github.com/golang-migrate/migrate) applies `migrations/*.up.sql` in order.
- **Applying:** `make up` runs it as the one-shot `migrate` service, and every service that uses Postgres waits for it. So pulling a new migration needs only `make up` (or `make migrate`), and your data is kept.
- **Version:** `make migrate-version` shows the recorded version (`public.schema_migrations`).
- **Volumes from before the runner:** migrations 001–004 used to run as Postgres init scripts. Such volumes are baselined automatically on the first `make up`: `deploy/migrate/baseline.sh` records the version already in place, then `migrate` applies the rest.
- **A new migration** is an `NNN_name.up.sql` / `NNN_name.down.sql` pair with the next number. `make test-integration` applies every up file to a throwaway database.
- **Starting over:** `make reset` drops all volumes; the next `up` migrates an empty database.
- **Production:** run the same `migrate` image, or the CLI, against the database before deploying the services.

**Partitions:** `002_ledger.up.sql` creates the `ledger` schema with monthly partitions for the current month and 3 ahead. The `partitions` job keeps them 3 months ahead and raises a ticket alert when fewer than 2 future months exist. By hand: `SELECT ledger.ensure_partitions((now() AT TIME ZONE 'UTC')::date, 4);`

## 4. Tests

| Command | What it runs | Needs |
| :--- | :--- | :--- |
| `make vet test` | Unit and HTTP-level tests (miniredis and in-memory fakes; franz-go's `kfake` for the Kafka producer) | nothing |
| `make test-web` | The web console's `npm ci`, `tsc --noEmit` and Vitest, in `node:22-alpine` (no Node on the host). `cd web && npm run dev` gives live reload against a running stack, if you have Node | Docker |
| `make test-integration` | Tests behind the `integration` build tag, with `-race`, against real Postgres, Kafka and S3 | `docker compose up -d postgres kafka kafka-init archive-store` |
| `make smoke` | register → login → profile → logout → token rejected | `make up` |
| `make smoke-ledger` | open account → credit → debit → insufficient funds → replay → freeze → auth checks | `make up` |
| `make smoke-fraud` | 4 top-ups freeze an account within 2 s | `make up`, with the default fraud thresholds |
| `make smoke-console` | Through the console: register → login → open → top up → balance → refusals → four top-ups freeze the account → a frozen spend is refused | `make up`, with the default fraud thresholds |
| `make check-alerts` | alert rules load, dashboard queries run, alerts trip | `make up` |
| `make load` | the TRD §13.4 load test (below) | `make load-up` |
| `make chaos FAULT=…` | a TRD §13.5 fault drill (below) | `make load-up`, in a throwaway project |
| `make load-credits` | the older credits-only outbox lag check (k6 at `RATE`/s for `DURATION`) | `make up` |

**Integration tests.**
- **Database:** `pgtest.New(t)` (`internal/testutil/pgtest`) creates a throwaway database per test, applies every migration and drops it afterwards. The role needs `CREATEDB`, which the compose `auth` role has.
- **Configuration:** `TEST_DATABASE_URL`, `TEST_KAFKA_BROKERS` and `TEST_S3_*` default to the compose services. Tests are skipped when `TEST_DATABASE_URL` is unset.

**Load tests** (TRD §13.4). The results are in [Load Test Results](<2026-11-08 Load Test Results - Immutable Ledger.md>).
- **Isolation:** use a throwaway compose project (`COMPOSE_PROJECT_NAME=…`) to keep the load data out of your stack.
- **`make load-up`:** `make up` with the fraud thresholds raised, so the run measures writes rather than freezes, and with production-like Postgres memory and WAL settings.
- **Profiles:**
  - `PROFILE=full`: 30 min at 300 TPS (60% debit, 35% credit, 5% void) across 10,000 accounts, with 10 hot accounts at 20 TPS each, then 600 TPS for 5 min.
  - `PROFILE=quick`: the same shape in about 2 minutes, across 1,000 accounts.
- **Seeding:** `scripts/load/run.sh` inserts the load users straight into Postgres. Registering 10,000 through auth-api at bcrypt cost 12 would take most of an hour.
- **What it runs:** `scripts/load/ledger-mixed.js` in k6. It samples the outbox lag, then reports transaction outcomes, `processing_timeout`s, server-side write latency from Loki, and a full reconciler sweep.
- **Results:** k6 exits 99 when an NFR threshold fails, and results go to `load-results/<run>/`. A passing example is in [`examples/load/`](../examples/load/).

**Chaos drills** (TRD §13.5). The procedure and the recorded results are in the [Chaos Runbook](<2026-11-09 Chaos Runbook - Immutable Ledger.md>).

```bash
COMPOSE_PROJECT_NAME=chaos make load-up
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=redis      # stop Redis for 60 s
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=kafka      # stop Kafka for 5 min
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=postgres   # kill -9 Postgres, restart after 30 s
COMPOSE_PROJECT_NAME=chaos make chaos FAULT=worker     # 2 ledger-worker replicas, kill -9 the leader
COMPOSE_PROJECT_NAME=chaos docker compose down -v
```

- **The client:** `scripts/load/ledger-chaos.js` writes at 50/s and retries each request with its `Idempotency-Key` until it gets an answer.
- **The checks:** afterwards, Postgres, every balance and the Kafka topic must match what the client was told, and a full reconciler sweep must be clean. Each check prints PASS or FAIL.
- **Results:** they go to `chaos-results/<run>-<fault>/`; passing examples are in [`examples/chaos/`](../examples/chaos/).

**Timing-sensitive tests on a laptop.** The load test and the chaos drills measure latency, so run them on AC power with macOS Low Power Mode off:
- **On battery with Low Power Mode on:** the quick load test's p99 rose from 47–95 ms to 395–1,140 ms, and one drill saw a 12 s freeze.
- **Sleep:** `caffeinate` can't keep a Mac awake on battery. A drill during which the host slept or froze is reported INVALID (exit 3).

## 5. Container image

The `Dockerfile` builds any binary under `cmd/`, chosen with the `CMD` build arg (default `api`): `docker build --build-arg CMD=api .`. Every binary must implement a `healthcheck` subcommand, which the image uses as its container healthcheck. The runtime image is distroless and runs as non-root with a read-only root filesystem.

## 6. Web console

The console is `ledger-console`, at http://localhost:8090. It's bound to 127.0.0.1 and starts with `make up`. It shows whether money is being handled correctly right now, what went wrong, and how the system recovered. The [design](<2026-11-10 Web Console Design - Ledger Trust Monitor.md>) explains why it exists; this section covers running it.

### 6.1 First use

1. Open http://localhost:8090/register and create a user. Any email works: nothing is sent.
2. **My account** opens a demo account and tops it up or spends from it (see §6.3).
3. For the ops screens, give that user the ops role: `make grant-role EMAIL=you@example.com`. Sign in again, or reload, and **Monitor**, **Transactions** and **Accounts** appear. `make grant-role EMAIL=… ROLE=user` takes the role back. Each change is kept in the append-only `user_role_changes` table.

### 6.2 Screens

| Screen | Path | Who | What it shows |
| :--- | :--- | :--- | :--- |
| Monitor | `/` | ops | **Tiles** for the last 10 minutes: transactions, handled correctly, needed recovery, books balanced, events to Kafka. **Incident banners** and the **self-healing** feed look back 30 minutes. Also the live feed of the newest transactions. It polls every 3 s while the tab is visible |
| Transactions | `/transactions` | ops | Every account's transactions, newest change first, filtered by status and type |
| Transaction | `/transactions/:id` | ops | The timeline (each step's time and source), the double entry, the trace id with a link to its logs in Grafana, and links to a void and its original |
| Accounts | `/accounts`, `/accounts/:id` | ops | Balances and status. An account's page adds its freeze history and its transactions, 20 at a time |
| My account | `/me` | any user | The signed-in user's own account, with the demo actions |
| Profile | `/profile` | any user | Name and bio, and the password change (which ends every session) |

### 6.3 Demo actions

On **My account**, open, top up and spend call ledger-api with the console's `onboarding` and `payments` tokens. They act on the signed-in user's own account only, in IDR, from 1 to 1,000,000 per action. Each one is logged as `console.demo_action` with `user_id`.

The fraud rules are real. More than 3 top-ups in 5 minutes (`FRAUD_TOPUP_MAX`, `FRAUD_TOPUP_WINDOW`) freezes the account, and spending is then refused with `account_frozen`. An ops client unfreezes it through ledger-api's `/unfreeze`.

### 6.4 Labels

Every transaction carries one label, chosen from the ledger's record and the recovery job's log lines:

| Label | When | Example "what happened" |
| :--- | :--- | :--- |
| **Normal** | Settled on the request path | "Settled in 3 ms" |
| **Recovered** | The recovery job completed it (a `ledger.recover` line names it), whether it settled or was refused | "Completed by the recovery job after the request couldn't finish on its own" |
| **Rejected** | Refused for a business reason: `insufficient_funds`, `account_frozen`, `not_voidable`, `ledger_not_found` | "Insufficient funds; balance unchanged" |
| **Reversed** | Voided, or a settled void | "Reversed by a later void; the original stays on record" |
| **Timed out** | Failed with `processing_timeout`. Shown red: look at it | "Couldn't complete in time; failed safely, nothing moved" |
| **In progress** | Pending. Red after 60 s (NFR-7) | "Processing" |

Without Loki, the console can't see the recovery job's lines. A recovered transaction then shows as **Normal**, and the screens say "Activity signals unavailable" rather than claim a recovery they can't see.

### 6.5 Incidents

Each banner shows "Incident in progress" while the incident lasts, then "Resolved" with what happened:

| Incident | Starts | Ends |
| :--- | :--- | :--- |
| Database unavailable | ledger-api's first `503 ledger_unavailable` | the first successful write after it |
| Kafka unavailable | the first `outbox.publish` `kafka_unavailable` | `outbox.lag` back under 5 s |
| Redis unavailable | ledger-api's first `dependency.call` for Redis | none for 30 s |

Worker leader changes go in the self-healing feed, not in a banner: "Ledger-worker replica <container> took leadership of 7 background jobs". A killed replica logs nothing itself, so there's no start time to show.

### 6.6 Watching a chaos drill

1. On a throwaway stack (stop the default one first: they publish the same ports): `COMPOSE_PROJECT_NAME=chaos make load-up`, then register a user and grant it the ops role (§6.1). `load-up` raises the fraud thresholds, so the drill's accounts aren't frozen.
2. Open the Monitor, then in a terminal run `COMPOSE_PROJECT_NAME=chaos make chaos FAULT=kafka` (or `redis`, `postgres`, `worker`).
3. Watch the banner for the fault. Kafka: events queue up, then drain. Postgres: payments are refused, then the recovery job completes anything caught mid-way. During a Postgres outage the ops screens themselves answer 503 (the role check and the ledger data read Postgres), so the banner shows once the database is back ([#67](https://github.com/dydanz/fintech-ledger-playground/issues/67)).
4. Compare with the drill's `report.txt` in `chaos-results/<run>-<fault>/`. The runbook (`spec/2026-11-09 Chaos Runbook - Immutable Ledger.md`) says what each fault should do.

`examples/console/` has screenshots from these drills, each with a caption.

### 6.7 Configuration

| Variable | Compose default | What it does |
| :--- | :--- | :--- |
| `CONSOLE_HTTP_ADDR` | `:8090` | Listen address |
| `CONSOLE_AUTH_API_URL` / `CONSOLE_LEDGER_API_URL` | `http://api:8080` / `http://ledger-api:8081` | The two upstreams |
| `CONSOLE_DEMO_ACTIONS` | `true` | Turns the My account actions on |
| `CONSOLE_ONBOARDING_TOKEN` / `CONSOLE_PAYMENTS_TOKEN` | the dev tokens in `.env.example` | The demo actions' ledger-api clients. Never logged |
| `CONSOLE_READ_TOKEN` | `dev-console-token` | The ops screens' ledger-api client, the read-only `console` client. Never logged |
| `CONSOLE_LOKI_URL` | `http://loki:3100` | The activity signals. Empty leaves them out |
| `CONSOLE_GRAFANA_URL` | `http://localhost:3000` | Grafana as your browser reaches it, for the trace id's Explore link. Empty: no link |
| `CONSOLE_ALLOWED_HOSTS` | `localhost:8090,127.0.0.1:8090` | The `Host` headers it answers (this blocks DNS rebinding). Change it when you publish another port; use `*` only behind a proxy that checks the host |
| `TRUSTED_PROXIES` | empty | CIDRs whose `X-Forwarded-For` it trusts |

**What gets logged:**
- `console.access` (refusals on the ops routes) and `console.demo_action`. Both have panels in `ledger.json`.
- `dependency.call` when an upstream or Loki can't be reached. Loki's failures are logged once per outage.
