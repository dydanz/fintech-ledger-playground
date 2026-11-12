# fintech-ledger-playground

A Go/Gin playground for reliable money movement, with structured JSON logs shipped to Loki and Grafana. It has four services:

- **auth-api:** registration and login with Redis-backed opaque tokens and an append-only PostgreSQL audit trail.
- **ledger-api:** an immutable, event-sourced, double-entry balance ledger, with idempotent writes, voids and freezes.
- **ledger-worker:** an outbox relay to Kafka, plus the sweeper, the reconciler, partition maintenance and a write-once archiver.
- **fraud-engine:** consumes ledger events from Kafka and freezes accounts on velocity rules.

The design is in [`spec/`](spec/): the BRD, the TRD and the development plan.

## Quick start

```bash
cp .env.example .env      # optional: override the dev passwords
make up                   # build and start everything (docker compose)
make smoke                # auth: register -> login -> profile -> logout -> 401
make smoke-ledger         # ledger: open -> credit -> debit -> NSF -> replay -> freeze
```

| Service | URL |
| :--- | :--- |
| auth-api | http://localhost:8080 |
| ledger-api | http://localhost:8081 (127.0.0.1 only) |
| Web console | http://localhost:8090 (127.0.0.1 only; being built, see the [design](<spec/2026-11-10 Web Console Design - Ledger Trust Monitor.md>)) |
| Grafana | http://localhost:3000 (admin / admin): dashboards **auth-api overview** and **ledger overview** |

## How it fits together

```
client ──► auth-api ──► Redis (sessions) + Postgres (audit)
service ─► ledger-api ─► Postgres: events + journal + outbox, in one transaction
                          │
           ledger-worker ─┴─► Kafka ledger.events.v1 ──► fraud-engine ──► freeze via ledger-api
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
| `make demo-story EMAIL=you@example.com` | Walk one registered user through a top-up, purchases, a refund and a chargeback, explaining each step and checking the books (writes real transactions; local stack only) |
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
