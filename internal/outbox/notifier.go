package outbox

import (
	"context"
	"errors"
	"net"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"auth-api/internal/logger"
)

// Execer runs a statement outside any transaction; *pgxpool.Pool is one.
type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Notifier wakes the relay after commits that wrote outbox rows, coalescing
// them into at most one NOTIFY per interval.
//
// The NOTIFY is deliberately not part of the write transaction. A transaction
// that notifies takes a database-wide lock at COMMIT and holds it through the
// WAL flush, so notifying inside every write made all writes commit one at a
// time (the P6.2 load test: p99 314 ms at 500 TPS). Sent on its own, the
// NOTIFY's transaction writes no WAL and holds that lock only briefly.
//
// A lost notification costs latency, not data: the relay also polls every
// OUTBOX_POLL_INTERVAL.
type Notifier struct {
	db    Execer
	every time.Duration
	wake  chan struct{}
}

// NewNotifier returns a Notifier that sends at most one NOTIFY per every.
// Start it with Run.
func NewNotifier(db Execer, every time.Duration) *Notifier {
	return &Notifier{db: db, every: max(every, time.Millisecond), wake: make(chan struct{}, 1)}
}

// Notify asks Run to send a NOTIFY. It never blocks; calls that arrive while
// one is pending are merged into it. A nil Notifier does nothing.
func (n *Notifier) Notify() {
	if n == nil {
		return
	}
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// Run sends one NOTIFY per request until ctx ends, waiting at least every
// between them, so a burst of commits costs one NOTIFY per interval. Every
// Notify, made after its commit, is followed by a NOTIFY: the pending one, or
// the next.
func (n *Notifier) Run(ctx context.Context) {
	log := logger.FromContext(ctx)
	failing := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.wake:
		}
		start := time.Now()
		sctx, cancel := context.WithTimeout(ctx, time.Second)
		_, err := n.db.Exec(sctx, `SELECT pg_notify($1, '')`, NotifyChannel)
		cancel()
		switch {
		case err != nil && ctx.Err() != nil:
			return
		case err != nil && !failing:
			// Logged once per outage: Postgres being down is already loud elsewhere.
			failing = true
			log.WarnContext(ctx, "outbox notify failed, the relay falls back to polling", "event", "dependency.call",
				"outcome", "failure", "reason", notifyReason(err), "dependency", "postgres", "operation", "outbox_notify",
				"duration_ms", float64(time.Since(start).Microseconds())/1000, "error", err)
		case err == nil:
			failing = false
		}
		t := time.NewTimer(n.every)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func notifyReason(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	}
	return "unknown"
}
