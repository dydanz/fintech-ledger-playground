#!/bin/sh
# One-time baseline for databases created before the migration runner (TRD §16 Q11).
#
# Until P5, migrations 001–004 ran as Postgres init scripts, which leave no
# record of what was applied. golang-migrate keeps its version in
# public.schema_migrations; when that table is missing but the schema exists,
# this probes which of 001–004 are in place and records that version, so
# `migrate up` applies only the rest. It's a no-op on an empty database
# (migrate applies everything) and once schema_migrations exists.
#
# The probes cover the legacy migrations only. Migrations from 005 on are
# applied by the runner from the start, so they need none.
set -eu

q() { psql -X -q -t -A -v ON_ERROR_STOP=1 -c "$1"; }

if [ "$(q "SELECT to_regclass('public.schema_migrations') IS NOT NULL")" = t ]; then
	echo "baseline: schema_migrations exists; nothing to do"
	exit 0
fi
if [ "$(q "SELECT to_regclass('public.users') IS NOT NULL")" = f ]; then
	echo "baseline: empty database; migrate applies every migration"
	exit 0
fi

version=1
if [ "$(q "SELECT to_regnamespace('ledger') IS NOT NULL")" = t ]; then
	version=2
	if [ "$(q "SELECT data_type FROM information_schema.columns
	           WHERE table_schema = 'ledger' AND table_name = 'idempotency_keys' AND column_name = 'response_body'")" = bytea ]; then
		version=3
		if [ "$(q "SELECT to_regclass('ledger.idempotency_resource_idx') IS NOT NULL")" = t ]; then
			version=4
		fi
	fi
fi

# The table golang-migrate's postgres driver creates, and what `migrate force` writes.
q "BEGIN;
   CREATE TABLE schema_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL);
   INSERT INTO schema_migrations (version, dirty) VALUES ($version, false);
   COMMIT;"
echo "baseline: existing schema recorded at version $version"
