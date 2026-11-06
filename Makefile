.PHONY: up down reset logs test test-integration vet smoke psql

up:
	docker compose up -d --build

down:
	docker compose down

reset: ## drop all volumes (re-runs DB init scripts on next `up`)
	docker compose down -v

logs:
	docker compose logs -f api

test:
	go test -race -count=1 ./...

# Needs the compose Postgres (`docker compose up -d postgres`). Each test gets
# its own throwaway database with all migrations applied (internal/testutil/pgtest).
TEST_DATABASE_URL ?= postgres://auth:auth_dev_password@localhost:5432/auth?sslmode=disable
test-integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' go test -race -count=1 -tags integration ./...

vet:
	go vet ./...

smoke:
	./scripts/smoke.sh

psql:
	docker compose exec postgres psql -U auth -d auth
