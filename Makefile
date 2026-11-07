.PHONY: up down reset logs logs-ledger logs-worker kafka-topics test test-integration vet smoke smoke-ledger psql

up:
	docker compose up -d --build

down:
	docker compose down

reset: ## drop all volumes (re-runs DB init scripts on next `up`)
	docker compose down -v

logs:
	docker compose logs -f api

logs-ledger:
	docker compose logs -f ledger-api

logs-worker:
	docker compose logs -f ledger-worker

kafka-topics: ## topics, partitions and configs on the local broker
	docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe

test:
	go test -race -count=1 ./...

# Needs the compose Postgres (`docker compose up -d postgres`). Each test gets
# its own throwaway database with all migrations applied (internal/testutil/pgtest).
TEST_KAFKA_BROKERS ?= 127.0.0.1:9094
TEST_DATABASE_URL ?= postgres://auth:auth_dev_password@localhost:5432/auth?sslmode=disable
test-integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TEST_KAFKA_BROKERS='$(TEST_KAFKA_BROKERS)' go test -race -count=1 -tags integration ./...

vet:
	go vet ./...

smoke:
	./scripts/smoke.sh

smoke-ledger: ## needs api and ledger-api running (make up)
	./scripts/smoke-ledger.sh

psql:
	docker compose exec postgres psql -U auth -d auth
