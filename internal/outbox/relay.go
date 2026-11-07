package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"auth-api/internal/kafka"
	"auth-api/internal/logger"
)

// Pending is an unpublished outbox row.
type Pending struct {
	ID      int64
	EventID string
	Topic   string
	Key     string // account_id
	Value   []byte // the envelope
	Headers map[string]string
}

// Listener waits for NOTIFY on the relay channel.
type Listener interface {
	Wait(ctx context.Context, d time.Duration) (notified bool, err error)
	Close()
}

// Store is the relay's view of ledger.outbox.
type Store interface {
	Unpublished(ctx context.Context, limit int) ([]Pending, error)
	MarkPublished(ctx context.Context, ids []int64) error
	Lag(ctx context.Context) (pending int64, oldest time.Duration, err error)
	Purge(ctx context.Context, cutoff time.Time, limit int) (int64, error)
	Listen(ctx context.Context) (Listener, error)
}

// Publisher writes records and returns once the broker acknowledged all.
type Publisher interface {
	Publish(ctx context.Context, recs []kafka.Record) error
}

type RelayConfig struct {
	PollInterval  time.Duration // OUTBOX_POLL_INTERVAL: wake even without a NOTIFY
	BatchSize     int           // OUTBOX_BATCH_SIZE
	LagEvery      time.Duration // how often outbox.lag is logged
	Retention     time.Duration // OUTBOX_RETENTION: published rows older than this are purged
	PurgeBatch    int
	MaxBackoff    time.Duration // ceiling for retries while Kafka or Postgres fails
	EscalateAfter time.Duration // failing this long turns outbox.publish warn into error
	// PublishTimeout bounds one batch: franz-go retries delivery
	// indefinitely by default, so without it an outage blocks silently
	// instead of being logged and retried.
	PublishTimeout time.Duration
}

// Relay publishes outbox rows to Kafka, at least once and in id order
// (spec/TRD §7.3). It must run on one leader at a time (worker.Runner).
type Relay struct {
	store Store
	pub   Publisher
	cfg   RelayConfig
	now   func() time.Time
}

func NewRelay(store Store, pub Publisher, cfg RelayConfig) (*Relay, error) {
	if cfg.PollInterval <= 0 || cfg.BatchSize < 1 || cfg.LagEvery <= 0 || cfg.MaxBackoff < cfg.PollInterval ||
		cfg.Retention <= 0 || cfg.PurgeBatch < 1 || cfg.PublishTimeout <= 0 {
		return nil, errors.New("outbox relay: invalid config")
	}
	return &Relay{store: store, pub: pub, cfg: cfg, now: time.Now}, nil
}

// Run relays until ctx ends: it drains on every NOTIFY and every
// PollInterval, backs off while publishing fails, and logs outbox.lag every
// LagEvery.
func (r *Relay) Run(ctx context.Context) error {
	log := logger.FromContext(ctx)
	var listener Listener
	defer func() {
		if listener != nil {
			listener.Close()
		}
	}()
	backoff := time.Duration(0)
	var failingSince time.Time
	nextLag := r.now()

	for ctx.Err() == nil {
		if listener == nil { // (re)subscribe; polling covers the gap
			l, err := r.store.Listen(ctx)
			if err == nil {
				listener = l
			} else if ctx.Err() == nil {
				log.WarnContext(ctx, "outbox listen failed, polling only", "event", "outbox.publish", "outcome", "failure",
					"reason", "pg_error", "error", err)
			}
		}

		reason, err := r.Drain(ctx)
		switch {
		case err == nil:
			backoff, failingSince = 0, time.Time{}
		case ctx.Err() != nil:
			return nil
		default:
			if failingSince.IsZero() {
				failingSince = r.now()
			}
			backoff = min(max(backoff*2, r.cfg.PollInterval), r.cfg.MaxBackoff)
			failing := r.now().Sub(failingSince)
			attrs := []any{"event", "outbox.publish", "outcome", "failure", "reason", reason, "error", err,
				"retry_in", backoff.String(), "failing_ms", float64(failing.Milliseconds())}
			if failing >= r.cfg.EscalateAfter {
				log.ErrorContext(ctx, "outbox publish still failing, retrying", attrs...)
			} else {
				log.WarnContext(ctx, "outbox publish failed, retrying", attrs...)
			}
		}

		if !r.now().Before(nextLag) {
			r.logLag(ctx)
			nextLag = r.now().Add(r.cfg.LagEvery)
		}

		wait := r.cfg.PollInterval
		if backoff > 0 {
			wait = backoff
		}
		if listener != nil && backoff == 0 {
			if _, err := listener.Wait(ctx, wait); err != nil && ctx.Err() == nil {
				listener.Close()
				listener = nil
			}
			continue
		}
		t := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			t.Stop()
			return nil
		case <-t.C:
		}
	}
	return nil
}

// Drain publishes batches until none is full. On failure it returns the
// catalog reason; the failed batch stays unpublished and is retried whole.
func (r *Relay) Drain(ctx context.Context) (reason string, err error) {
	for {
		batch, err := r.store.Unpublished(ctx, r.cfg.BatchSize)
		if err != nil {
			return "pg_error", err
		}
		if len(batch) == 0 {
			return "", nil
		}
		recs := make([]kafka.Record, len(batch))
		ids := make([]int64, len(batch))
		for i, p := range batch {
			recs[i] = kafka.Record{Topic: p.Topic, Key: p.Key, Value: p.Value, Headers: p.Headers}
			ids[i] = p.ID
		}
		pctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
		err = r.pub.Publish(pctx, recs)
		cancel()
		if err != nil {
			return kafkaReason(err), fmt.Errorf("publish %d records: %w", len(recs), err)
		}
		// A crash here re-publishes the batch: consumers deduplicate on event_id.
		if err := r.store.MarkPublished(ctx, ids); err != nil {
			return "pg_error", err
		}
		if len(batch) < r.cfg.BatchSize {
			return "", nil
		}
	}
}

func (r *Relay) logLag(ctx context.Context) {
	log := logger.FromContext(ctx)
	pending, oldest, err := r.store.Lag(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.WarnContext(ctx, "outbox lag check failed", "event", "outbox.lag", "outcome", "failure",
				"reason", "pg_error", "error", err)
		}
		return
	}
	lagMs := float64(oldest.Microseconds()) / 1000
	if lagMs > 5000 {
		log.WarnContext(ctx, "outbox lagging", "event", "outbox.lag", "outcome", "success", "pending", pending, "lag_ms", lagMs)
		return
	}
	log.InfoContext(ctx, "outbox lag", "event", "outbox.lag", "outcome", "success", "pending", pending, "lag_ms", lagMs)
}

// Purge deletes published rows older than Retention, in batches.
func (r *Relay) Purge(ctx context.Context) error {
	cutoff := r.now().Add(-r.cfg.Retention)
	var total int64
	for ctx.Err() == nil {
		n, err := r.store.Purge(ctx, cutoff, r.cfg.PurgeBatch)
		if err != nil {
			logger.FromContext(ctx).ErrorContext(ctx, "outbox purge failed", "event", "outbox.purge", "outcome", "failure",
				"reason", "pg_error", "error", err, "deleted", total)
			return err
		}
		total += n
		if n < int64(r.cfg.PurgeBatch) {
			break
		}
	}
	if total > 0 {
		logger.FromContext(ctx).InfoContext(ctx, "published outbox rows purged", "event", "outbox.purge",
			"outcome", "success", "deleted", total)
	}
	return nil
}

// kafkaReason classifies a publish error: the broker refusing a record
// (too large, unknown topic) versus not being reachable.
func kafkaReason(err error) string {
	if kafka.IsRejected(err) {
		return "kafka_rejected"
	}
	return "kafka_unavailable"
}
