.PHONY: up down reset migrate migrate-version grant-role rebuild-projections logs logs-ledger logs-worker kafka-topics load load-up load-credits chaos check-alerts test test-web test-integration vet smoke smoke-ledger smoke-fraud smoke-console logs-fraud psql

up: ## build and start everything, then load any new alert rules into Grafana
	docker compose up -d --build
	./scripts/grafana-reload.sh

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

grant-role: ## give a user a role: make grant-role EMAIL=you@example.com [ROLE=ops|user] [BY=name]. ops opens the console's ops screens; BY (default $$USER) is kept in user_role_changes
	docker compose run --rm --no-deps api grant-role --email '$(EMAIL)' --role '$(or $(ROLE),ops)' --by '$(or $(BY),$(USER))'

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

PROFILE ?= full
load: ## TRD §13.4 ledger load (PROFILE=full: 30 min at 300 TPS + 10 hot accounts, then 600 TPS for 5 min; PROFILE=quick: ~2 min). Start with make load-up
	PROFILE=$(PROFILE) ./scripts/load/run.sh

load-up: ## make up for a load run: fraud thresholds out of the way (it measures writes, not freezes) and production-like Postgres memory and WAL settings
	FRAUD_TOPUP_MAX=1000000000 FRAUD_DEBIT_MAX_COUNT=1000000000 FRAUD_DEBIT_LIMITS=IDR:1000000000000000 \
	PG_SHARED_BUFFERS=1GB PG_MAX_WAL_SIZE=8GB PG_CHECKPOINT_TIMEOUT=15min PG_WAL_COMPRESSION=lz4 $(MAKE) up

chaos: ## TRD §13.5 fault drill, FAULT=redis|kafka|postgres|worker. It stops and kills containers: use a throwaway COMPOSE_PROJECT_NAME started with make load-up
	FAULT=$(FAULT) ./scripts/chaos/run.sh

load-credits: ## k6 credits at RATE/s for DURATION; prints the max outbox lag (needs make up)
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
TEST_REDIS_ADDR ?= 127.0.0.1:6379
TEST_REDIS_PASSWORD ?= redis_dev_password
# Web console: typecheck and unit tests in a Node container (no Node on the host needed).
WEB_NODE ?= node:22-alpine
test-web: ## web console: npm ci, tsc --noEmit and Vitest in $(WEB_NODE)
	docker run --rm --user "$$(id -u):$$(id -g)" -e HOME=/tmp -v "$(CURDIR)/web":/web -w /web $(WEB_NODE) \
		sh -c 'npm ci --ignore-scripts --no-audit --no-fund --loglevel=error && npm run typecheck && npm test'

test-integration:
	TEST_DATABASE_URL='$(TEST_DATABASE_URL)' TEST_KAFKA_BROKERS='$(TEST_KAFKA_BROKERS)' TEST_S3_ENDPOINT='$(TEST_S3_ENDPOINT)' TEST_S3_ACCESS_KEY='$(TEST_S3_ACCESS_KEY)' TEST_S3_SECRET_KEY='$(TEST_S3_SECRET_KEY)' TEST_REDIS_ADDR='$(TEST_REDIS_ADDR)' TEST_REDIS_PASSWORD='$(TEST_REDIS_PASSWORD)' go test -race -count=1 -tags integration ./...

vet:
	go vet ./...

smoke:
	./scripts/smoke.sh

smoke-ledger: ## needs api and ledger-api running (make up)
	./scripts/smoke-ledger.sh

smoke-console: ## the web console's API end to end, incl. the fraud freeze and the ops screens (needs make up; gives its throwaway user the ops role)
	./scripts/smoke-console.sh

smoke-fraud: ## 4 top-ups freeze an account within 2 s (needs make up)
	./scripts/smoke-fraud.sh

psql:
	docker compose exec postgres psql -U auth -d auth
