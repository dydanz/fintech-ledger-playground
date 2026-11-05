.PHONY: up down reset logs test vet smoke psql

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

vet:
	go vet ./...

smoke:
	./scripts/smoke.sh

psql:
	docker compose exec postgres psql -U auth -d auth
