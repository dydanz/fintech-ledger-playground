//go:build integration

package service_test

import (
	"testing"
	"time"

	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/testutil/pgtest"
)

func partitionJob(t *testing.T, store service.PartitionStore, at time.Time) *service.Partitions {
	t.Helper()
	p, err := service.NewPartitions(store, service.PartitionConfig{MonthsAhead: 3, MinAhead: 2, Now: func() time.Time { return at }})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// An injected clock creates that month and 3 ahead, each with its local
// unique index and TRUNCATE guard; a second run creates nothing.
func TestPartitionsCreatedAheadOfAClock(t *testing.T) {
	pool := pgtest.New(t)
	repo := repository.NewPartitionRepo(pool)
	at := time.Date(2031, 5, 15, 12, 0, 0, 0, time.UTC)
	ctx, logs := logCtx()

	if err := partitionJob(t, repo, at).Run(ctx); err != nil {
		t.Fatal(err)
	}
	wantEvent(t, logs, "ledger.partition", map[string]any{"outcome": "success", "created": 8.0, "future_months": 3.0,
		"current_month": "2031-05"})

	for _, m := range []string{"2031m05", "2031m06", "2031m07", "2031m08"} {
		for table, index := range map[string]string{"journal_entries": "_seq_key", "events": "_stream_key"} {
			part := table + "_y" + m
			var n int
			if err := pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
				  WHERE c.relname = $1 AND i.inhparent = ('ledger.' || $2)::regclass)
				+ (SELECT count(*) FROM pg_indexes WHERE schemaname = 'ledger' AND indexname = $1 || $3 AND indexdef LIKE 'CREATE UNIQUE%')
				+ (SELECT count(*) FROM pg_trigger WHERE tgname = $1 || '_guard_trunc')`, part, table, index).Scan(&n); err != nil {
				t.Fatal(err)
			}
			if n != 3 {
				t.Errorf("%s: partition, unique index and truncate guard: found %d of 3", part, n)
			}
		}
	}
	if _, err := pool.Exec(ctx, `TRUNCATE ledger.journal_entries_y2031m07`); err == nil {
		t.Error("TRUNCATE of a new partition was allowed")
	}

	logs.Reset()
	if err := partitionJob(t, repo, at).Run(ctx); err != nil {
		t.Fatal(err)
	}
	wantEvent(t, logs, "ledger.partition", map[string]any{"outcome": "success", "created": 0.0})
}

// Fewer than 2 future months is a failure. Plain tables squatting on the
// names of two journal partitions make ensure_partitions skip them.
func TestPartitionsRunningOutIsAFailure(t *testing.T) {
	pool := pgtest.New(t)
	ctx, logs := logCtx()
	for _, m := range []string{"2030m02", "2030m03"} {
		if _, err := pool.Exec(ctx, `CREATE TABLE ledger.journal_entries_y`+m+` (x int)`); err != nil {
			t.Fatal(err)
		}
	}
	err := partitionJob(t, repository.NewPartitionRepo(pool), time.Date(2030, 1, 10, 0, 0, 0, 0, time.UTC)).Run(ctx)
	if err == nil {
		t.Fatal("running out of partitions was not an error")
	}
	wantEvent(t, logs, "ledger.partition", map[string]any{"outcome": "failure", "level": "error",
		"reason": "insufficient_future_partitions", "table": "journal_entries", "future_months": 1.0, "min_ahead": 2.0})
}
