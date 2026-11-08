package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"auth-api/internal/service"
)

type fakePartitionStore struct {
	counts    map[string]int
	ensureErr error
	countErr  error
	from      time.Time
	months    int
	countFrom time.Time
}

func (f *fakePartitionStore) Ensure(_ context.Context, from time.Time, months int) (int, error) {
	f.from, f.months = from, months
	return 2, f.ensureErr
}

func (f *fakePartitionStore) FuturePartitions(_ context.Context, from time.Time) (map[string]int, error) {
	f.countFrom = from
	return f.counts, f.countErr
}

func TestPartitionsJob(t *testing.T) {
	clock := time.Date(2026, 12, 31, 23, 30, 0, 0, time.FixedZone("WIB", 7*3600)) // 16:30 UTC, still December
	for name, tc := range map[string]struct {
		store   fakePartitionStore
		wantErr bool
		fields  map[string]any
	}{
		"enough ahead": {store: fakePartitionStore{counts: map[string]int{"events": 3, "journal_entries": 3}},
			fields: map[string]any{"outcome": "success", "level": "info", "future_months": 3.0, "created": 2.0, "current_month": "2026-12"}},
		"one table short": {store: fakePartitionStore{counts: map[string]int{"events": 3, "journal_entries": 1}}, wantErr: true,
			fields: map[string]any{"outcome": "failure", "level": "error", "reason": "insufficient_future_partitions",
				"table": "journal_entries", "future_months": 1.0}},
		"ensure failed but still enough": {store: fakePartitionStore{counts: map[string]int{"events": 2, "journal_entries": 2},
			ensureErr: errors.New("permission denied")}, wantErr: true,
			fields: map[string]any{"outcome": "failure", "level": "error", "reason": "pg_error", "future_months": 2.0}},
		"ensure failed and running out": {store: fakePartitionStore{counts: map[string]int{"events": 0, "journal_entries": 0},
			ensureErr: errors.New("permission denied")}, wantErr: true,
			fields: map[string]any{"outcome": "failure", "reason": "insufficient_future_partitions", "table": "events",
				"error": "permission denied"}},
		"count failed": {store: fakePartitionStore{countErr: errors.New("conn reset")}, wantErr: true,
			fields: map[string]any{"outcome": "failure", "reason": "pg_error"}},
	} {
		t.Run(name, func(t *testing.T) {
			f := tc.store
			p, err := service.NewPartitions(&f, service.PartitionConfig{MonthsAhead: 3, MinAhead: 2, Now: func() time.Time { return clock }})
			if err != nil {
				t.Fatal(err)
			}
			ctx, logs := logCtx()
			if err := p.Run(ctx); (err != nil) != tc.wantErr {
				t.Fatalf("Run error = %v, want error %v", err, tc.wantErr)
			}
			wantEvent(t, logs, "ledger.partition", tc.fields)
			if want := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC); !f.from.Equal(want) || f.months != 4 {
				t.Fatalf("Ensure(%v, %d), want (%v, 4)", f.from, f.months, want)
			}
			if want := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC); !f.countFrom.Equal(want) {
				t.Fatalf("future counted from %v, want %v", f.countFrom, want)
			}
		})
	}
}

func TestNewPartitionsValidates(t *testing.T) {
	for _, cfg := range []service.PartitionConfig{{MonthsAhead: 3, MinAhead: 0}, {MonthsAhead: 1, MinAhead: 2}} {
		if _, err := service.NewPartitions(&fakePartitionStore{}, cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
}
