package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PartitionRepo maintains the monthly partitions of ledger.journal_entries
// and ledger.events (spec/TRD §7.7).
type PartitionRepo struct{ db *pgxpool.Pool }

func NewPartitionRepo(db *pgxpool.Pool) *PartitionRepo { return &PartitionRepo{db: db} }

// PartitionedTables are the tables the job maintains.
var PartitionedTables = []string{"journal_entries", "events"}

// Ensure creates any missing monthly partitions of both tables for months
// months starting at from's month (UTC), each with its local unique index
// and TRUNCATE guard (ledger.ensure_partitions, migration 002). It returns
// how many it created.
func (r *PartitionRepo) Ensure(ctx context.Context, from time.Time, months int) (int, error) {
	var created int
	if err := r.db.QueryRow(ctx, `SELECT ledger.ensure_partitions($1::date, $2)`,
		from.UTC().Format(time.DateOnly), months).Scan(&created); err != nil {
		return 0, fmt.Errorf("ensure partitions: %w", err)
	}
	return created, nil
}

// FuturePartitions counts, per table, the attached partitions whose range
// starts at or after from (a month boundary, UTC).
func (r *PartitionRepo) FuturePartitions(ctx context.Context, from time.Time) (map[string]int, error) {
	rows, err := r.db.Query(ctx, `
		SELECT parent.relname::text,
		       count(*) FILTER (WHERE substring(pg_get_expr(child.relpartbound, child.oid) FROM 'FROM \(''([^'']+)''\)')::timestamptz >= $1)
		  FROM pg_inherits i
		  JOIN pg_class parent ON parent.oid = i.inhparent
		  JOIN pg_namespace n ON n.oid = parent.relnamespace AND n.nspname = 'ledger'
		  JOIN pg_class child ON child.oid = i.inhrelid
		 WHERE parent.relname = ANY($2) AND child.relispartition
		 GROUP BY parent.relname`, from.UTC(), PartitionedTables)
	if err != nil {
		return nil, fmt.Errorf("count partitions: %w", err)
	}
	out := make(map[string]int, len(PartitionedTables))
	for _, t := range PartitionedTables {
		out[t] = 0
	}
	type row struct {
		Table string
		N     int
	}
	got, err := pgx.CollectRows(rows, pgx.RowToStructByPos[row])
	if err != nil {
		return nil, fmt.Errorf("count partitions: %w", err)
	}
	for _, r := range got {
		out[r.Table] = r.N
	}
	return out, nil
}
