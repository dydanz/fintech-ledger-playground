# fintech-ledger-playground

A Go/Gin playground for reliable money movement, with structured JSON logs shipped to Loki and Grafana. It has four services:

- **auth-api:** registration and login with Redis-backed opaque tokens and an append-only PostgreSQL audit trail.
- **ledger-api:** an immutable, event-sourced, double-entry balance ledger, with idempotent writes, voids and freezes.
- **ledger-worker:** an outbox relay to Kafka, plus the sweeper, the reconciler, partition maintenance and a write-once archiver.
- **fraud-engine:** consumes ledger events from Kafka and freezes accounts on velocity rules.

A web console, **ledger-console**, shows whether money is being handled correctly right now, what went wrong, and how the system recovered.

The design is in [`spec/`](spec/): the BRD, the TRD and the development plan.

## Quick start

```bash
cp .env.example .env      # optional: override the dev passwords
make up                   # build and start everything (docker compose)
make smoke                # auth: register -> login -> profile -> logout -> 401
make smoke-ledger         # ledger: open -> credit -> debit -> NSF -> replay -> freeze
make smoke-console        # the console's API end to end
```

Then open the console:

1. Register at http://localhost:8090/register (any email; nothing is sent).
2. `make grant-role EMAIL=you@example.com` gives that user the ops role, for the **Monitor**, **Transactions** and **Accounts** screens. Reload the page.
3. **My account** opens a demo account and moves money. More than 3 top-ups in 5 minutes and the fraud engine freezes it.

| Service | URL |
| :--- | :--- |
| auth-api | http://localhost:8080 |
| ledger-api | http://localhost:8081 (127.0.0.1 only) |
| Web console | http://localhost:8090 (127.0.0.1 only): Monitor, Transactions, Accounts (ops role), My account. See the [Operations Guide §6](<spec/2026-11-10 Operations Guide.md#6-web-console>) |
| Grafana | http://localhost:3000 (admin / admin): dashboards **auth-api overview** and **ledger overview** |

## Watch the system recover

On a throwaway stack (stop the default one first with `make down`: they use the same ports), with the Monitor open in your browser:

```bash
export COMPOSE_PROJECT_NAME=chaos
make load-up                           # fraud thresholds out of the way
make grant-role EMAIL=you@example.com  # after registering at http://localhost:8090/register
make chaos FAULT=postgres              # or kafka, redis, worker
```

The Monitor shows the incident and, once it's over, what was refused and what the recovery job completed. (In a Postgres drill, the ops screens can't load until the database is back; see [#67](https://github.com/dydanz/fintech-ledger-playground/issues/67).) The drill's `chaos-results/<run>-<fault>/report.txt` checks the same thing from the client's side. The screenshots in [`examples/console/`](examples/console/) come from these drills, and the [Operations Guide §6.6](<spec/2026-11-10 Operations Guide.md#66-watching-a-chaos-drill>) walks through them. `make down` (with the same `COMPOSE_PROJECT_NAME`) stops it.

## How it fits together

```
client ──► auth-api ──► Redis (sessions) + Postgres (audit)
service ─► ledger-api ─► Postgres: events + journal + outbox, in one transaction
                          │
           ledger-worker ─┴─► Kafka ledger.events.v1 ──► fraud-engine ──► freeze via ledger-api

browser ─► ledger-console ─► auth-api, ledger-api (reads), Loki (what happened)
```

- **Postgres is the authority.** Redis caches sessions and idempotent responses, and Kafka is a feed filled from the transactional outbox (at-least-once; consumers deduplicate on `event_id`).
- **Correctness is checked continuously.** The reconciler compares balances, sequences, double entries and projections with the journal and events.

## Common commands

| Command | What it does |
| :--- | :--- |
| `make up` / `make down` / `make reset` | Start; stop; drop all volumes |
| `make logs-ledger` / `logs-worker` / `logs-fraud` | Follow one service's logs |
| `make psql` · `make kafka-topics` | Postgres shell · describe the Kafka topic |
| `make vet test` | Unit tests |
| `make test-web` | Web console typecheck and tests (in a Node container) |
| `make grant-role EMAIL=you@example.com` | Give a registered user the ops role, needed for the console's Monitor, Transactions and Accounts (`ROLE=user` takes it back) |
| `make test-integration` | Integration tests (start `postgres kafka kafka-init archive-store` first) |
| `make smoke-fraud` · `make check-alerts` | Fraud end to end · Grafana alert rules |
| `make load PROFILE=quick` | Load test, about 2 min (after `make load-up`) |
| `make chaos FAULT=redis\|kafka\|postgres\|worker` | Fault drill; use a throwaway `COMPOSE_PROJECT_NAME` |

## Documentation

| Topic | Document |
| :--- | :--- |
| Auth routes, token design, auth config | [Auth API Reference](<spec/2026-11-10 Auth API Reference.md>) |
| Ledger routes, idempotency, ledger config | [Ledger API Reference](<spec/2026-11-10 Ledger API Reference.md>) |
| Services, worker jobs, logging, migrations, tests | [Operations Guide](<spec/2026-11-10 Operations Guide.md>) |
| Design and requirements | [TRD](<spec/2026-11-06 TRD - Immutable Ledger & Event-Sourced Balance System.md>), [BRD](<spec/2026-11-06 BRD - Immutable Ledger & Event-Sourced Balance System.md>), [Development Plan](<spec/2026-11-06 Development Plan - Immutable Ledger & Event-Sourced Balance System.md>) |
| Load test results (§13.4) | [Load Test Results](<spec/2026-11-08 Load Test Results - Immutable Ledger.md>) |
| Chaos drills and findings (§13.5) | [Chaos Runbook](<spec/2026-11-09 Chaos Runbook - Immutable Ledger.md>) |
| Example passing runs | [`examples/`](examples/) |

## Layout

```
cmd/                 api, ledger-api, ledger-worker, fraud-engine entrypoints
internal/ledger/     domain: money, events, state machines (no I/O)
internal/service/    auth, profile and ledger business logic
internal/repository/ PostgreSQL (pgx)
internal/api/        router, middleware, handlers
internal/outbox/     outbox envelope, relay, NOTIFY
internal/kafka/      franz-go producer and consumer
internal/fraud/      velocity rules
internal/worker/     leader election for ledger-worker jobs
internal/archive/    Parquet export to write-once S3
migrations/          SQL schema (golang-migrate)
deploy/              Loki, Alloy, Grafana provisioning
scripts/             smoke, load (k6) and chaos scripts
examples/            example passing load and chaos runs
```

## Known gaps

- A Redis outage adds about 190 ms to every ledger write (Chaos Runbook §6.2).
- A hung (not dead) ledger-worker leader keeps its jobs (§6.3).
- NFR-1 and NFR-2 (throughput and latency) aren't confirmed on server-class storage yet (Load Test Results §4.2).
- Not built: login rate limiting, metrics and trace export (trace ids are logged but not exported), and logout of all sessions.
