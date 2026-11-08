package service

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"auth-api/internal/logger"
)

// PartitionStore is what the partition job uses (repository.PartitionRepo).
type PartitionStore interface {
	Ensure(ctx context.Context, from time.Time, months int) (int, error)
	FuturePartitions(ctx context.Context, from time.Time) (map[string]int, error)
}

type PartitionConfig struct {
	MonthsAhead int              // keep this many months after the current one (TRD §7.7: 3)
	MinAhead    int              // fewer future months than this is a failure (TRD §7.7: 2)
	Now         func() time.Time // nil means time.Now; tests inject a clock
}

// Partitions keeps the monthly partitions of journal_entries and events
// created ahead of time (TRD §7.7). There is no DEFAULT partition, so a
// write past the last partition fails: running out is an outage, which is
// why fewer than MinAhead future months already raises an alert.
type Partitions struct {
	store PartitionStore
	cfg   PartitionConfig
	now   func() time.Time
}

func NewPartitions(store PartitionStore, cfg PartitionConfig) (*Partitions, error) {
	if cfg.MinAhead < 1 || cfg.MonthsAhead < cfg.MinAhead {
		return nil, errors.New("partitions: need 1 <= min ahead <= months ahead")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Partitions{store: store, cfg: cfg, now: now}, nil
}

// Run is the ledger-worker job: create the current month and MonthsAhead
// more (idempotent), then check how many future months exist.
func (p *Partitions) Run(ctx context.Context) error {
	log := logger.FromContext(ctx)
	now := p.now().UTC()
	month := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	next := month.AddDate(0, 1, 0)

	created, ensureErr := p.store.Ensure(ctx, month, p.cfg.MonthsAhead+1)
	counts, err := p.store.FuturePartitions(ctx, next)
	if err != nil {
		log.ErrorContext(ctx, "partition check failed", "event", "ledger.partition", "outcome", "failure",
			"reason", "pg_error", "error", errors.Join(ensureErr, err))
		return err
	}
	ahead, table := -1, "" // the table with the fewest future months
	for _, t := range slices.Sorted(maps.Keys(counts)) {
		if ahead < 0 || counts[t] < ahead {
			ahead, table = counts[t], t
		}
	}
	attrs := []any{"event", "ledger.partition", "future_months", ahead, "created", created,
		"current_month", month.Format("2006-01")}
	if ahead < p.cfg.MinAhead {
		attrs = append(attrs, "outcome", "failure", "reason", "insufficient_future_partitions", "table", table,
			"min_ahead", p.cfg.MinAhead)
		if ensureErr != nil {
			attrs = append(attrs, "error", ensureErr)
		}
		log.ErrorContext(ctx, "ledger partitions running out", attrs...)
		return fmt.Errorf("partitions: %s has %d future month(s), want at least %d", table, ahead, p.cfg.MinAhead)
	}
	if ensureErr != nil {
		log.ErrorContext(ctx, "partition creation failed", append(attrs, "outcome", "failure", "reason", "pg_error",
			"error", ensureErr)...)
		return ensureErr
	}
	log.InfoContext(ctx, "ledger partitions ensured", append(attrs, "outcome", "success")...)
	return nil
}
