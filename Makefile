.PHONY: up down reset migrate migrate-version rebuild-projections logs logs-ledger logs-worker kafka-topics load check-alerts test test-integration vet smoke smoke-ledger smoke-fraud logs-fraud psql

up:
	docker compose up -d --build

down:
	docker compose down

reset: ## drop all volumes (the next `up` migrates an empty database)
	docker compose down -v

migrate: ## apply pending migrations (`up` does this too)
	docker compose run --rm migrate-baseline && docker compose run --rm migrate

# Dry run by default; review the ledger.rebuild lines, then ARGS=--apply.
# Scope to one account with ARGS="--account <id>". Exit 3: a dry run found differences.
rebuild-projections: ## recompute projections from events + journal (TRD §7.6)
	docker compose run --rm --no-deps ledger-worker rebuild-projections $(ARGS)

migrate-version: ## the schema version recorded by golang-migrate
	docker compose run --rm migrate version

logs:
	docker compose logs -f api

logs-ledger:
	docker compose logs -f ledger-api

logs-worker:
	docker compose logs -f ledger-worker

logs-fraud:
	docker compose logs -f fraud-engine

load: ## k6 credits at RATE/s for DURATION; prints the max outbox lag (needs make up)
	./scripts/load/outbox-lag.sh 60 & docker compose --profile load run --rm k6 run /scripts/ledger-credits.js; wait

check-alerts: ## alert rules loaded, dashboard queries run, alerts trip on synthetic lines (needs make up)
	./scripts/check-alerts.sh

kafka-topics: ## topics, partitions and configs on the local broker
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

test:
	go test -race -count=1 ./...

# Needs the compose Postgres (`docker compose up -d postgres`). Each test gets
# its own throwaway database with all migrations applied (internal/testutil/pgtest).
TEST_KAFKA_BROKERS ?= 127.0.0.1:9094
TEST_S3_ENDPOINT ?= 127.0.0.1:7070
TEST_S3_ACCESS_KEY ?= archive-dev
TEST_S3_SECRET_KEY ?= archive_dev_secret
TEST_DATABASE_URL ?= postgres://auth:auth_dev_password@localhost:5432/auth?sslmode=disable
test-integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TEST_KAFKA_BROKERS='$(TEST_KAFKA_BROKERS)' TEST_S3_ENDPOINT='$(TEST_S3_ENDPOINT)' TEST_S3_ACCESS_KEY='$(TEST_S3_ACCESS_KEY)' TEST_S3_SECRET_KEY='$(TEST_S3_SECRET_KEY)' go test -race -count=1 -tags integration ./...

vet:
	go vet ./...

smoke:
	./scripts/smoke.sh

smoke-ledger: ## needs api and ledger-api running (make up)
	./scripts/smoke-ledger.sh

smoke-fraud: ## 4 top-ups freeze an account within 2 s (needs make up)
	./scripts/smoke-fraud.sh

psql:
	docker compose exec postgres psql -U auth -d auth
