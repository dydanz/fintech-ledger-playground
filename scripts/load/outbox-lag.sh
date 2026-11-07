#!/usr/bin/env bash
# Samples the outbox lag (age of the oldest unpublished row, ms) every 0.5s
# while a load runs, and prints the maximum. Usage: outbox-lag.sh SECONDS
set -euo pipefail
SECONDS_TO_RUN="${1:-40}"
PROJECT=(docker compose ${COMPOSE_PROJECT:+-p "$COMPOSE_PROJECT"})
max=0
end=$(( $(date +%s) + SECONDS_TO_RUN ))
while (( $(date +%s) < end )); do
  lag=$("${PROJECT[@]}" exec -T postgres psql -U "${POSTGRES_USER:-auth}" -d "${POSTGRES_DB:-auth}" -X -q -At -c \
    "SELECT coalesce(round(extract(epoch FROM now() - min(created_at)) * 1000), 0) FROM ledger.outbox WHERE published_at IS NULL")
  (( lag > max )) && max=$lag
  sleep 0.5
done
echo "max outbox lag: ${max} ms"
