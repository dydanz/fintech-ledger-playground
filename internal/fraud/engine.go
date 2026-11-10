package fraud

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
)

// Consumer is the engine's view of the Kafka consumer group.
type Consumer interface {
	Poll(ctx context.Context) ([]kafka.Message, error)
	Commit(ctx context.Context, msgs []kafka.Message) error
}

// SeenStore remembers processed event ids (TRD §7.4: fraud:seen:<event_id>).
type SeenStore interface {
	Seen(ctx context.Context, eventID string) (bool, error)
	Mark(ctx context.Context, eventID string) error
}

type RedisSeenStore struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

func NewRedisSeenStore(rdb redis.UniversalClient, ttl time.Duration) *RedisSeenStore {
	return &RedisSeenStore{rdb: rdb, ttl: ttl}
}

func (s *RedisSeenStore) Seen(ctx context.Context, eventID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, "fraud:seen:"+eventID).Result()
	if err != nil {
		return false, fmt.Errorf("redis seen: %w", err)
	}
	return n == 1, nil
}

func (s *RedisSeenStore) Mark(ctx context.Context, eventID string) error {
	if err := s.rdb.SetNX(ctx, "fraud:seen:"+eventID, 1, s.ttl).Err(); err != nil {
		return fmt.Errorf("redis mark seen: %w", err)
	}
	return nil
}

type EngineConfig struct {
	FreezeRetry time.Duration // FRAUD_FREEZE_RETRY: how long to retry a freeze before reporting failure
	MinBackoff  time.Duration
	MaxBackoff  time.Duration
}

// Engine consumes ledger.events.v1, evaluates the rules and freezes
// offending accounts. It processes a polled batch in order and commits it
// only once every event in it is handled: an event whose freeze keeps
// failing blocks its batch (and is retried) instead of being skipped, so a
// crash or restart re-evaluates it.
type Engine struct {
	consumer Consumer
	seen     SeenStore
	eval     *Evaluator
	freezer  Freezer
	cfg      EngineConfig
	now      func() time.Time
}

func NewEngine(c Consumer, seen SeenStore, eval *Evaluator, f Freezer, cfg EngineConfig) (*Engine, error) {
	if cfg.FreezeRetry <= 0 || cfg.MinBackoff <= 0 || cfg.MaxBackoff < cfg.MinBackoff {
		return nil, errors.New("fraud engine: invalid config")
	}
	return &Engine{consumer: c, seen: seen, eval: eval, freezer: f, cfg: cfg, now: time.Now}, nil
}

// Run consumes until ctx ends.
func (e *Engine) Run(ctx context.Context) error {
	log := logger.FromContext(ctx)
	backoff := e.cfg.MinBackoff
	for ctx.Err() == nil {
		msgs, err := e.consumer.Poll(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.WarnContext(ctx, "kafka fetch failed, retrying", "event", "dependency.call", "outcome", "failure",
				"reason", depReason(err), "dependency", "kafka", "operation", "fraud_poll", "error", err)
			if len(msgs) == 0 {
				backoff = e.sleep(ctx, backoff)
				continue
			}
		}
		for _, m := range msgs {
			if !e.handle(ctx, m) {
				return nil // ctx ended mid-batch: nothing committed, the batch is redelivered
			}
		}
		for {
			err := e.consumer.Commit(ctx, msgs)
			if err == nil || ctx.Err() != nil {
				break
			}
			log.WarnContext(ctx, "kafka commit failed, retrying", "event", "dependency.call", "outcome", "failure",
				"reason", depReason(err), "dependency", "kafka", "operation", "fraud_commit", "error", err)
			backoff = e.sleep(ctx, backoff)
		}
		backoff = e.cfg.MinBackoff
	}
	return nil
}

// handle processes one message to completion; false means ctx ended first.
func (e *Engine) handle(ctx context.Context, m kafka.Message) bool {
	log := logger.FromContext(ctx)
	s, ok, err := ParseSettlement(m.Value)
	if err != nil {
		log.WarnContext(ctx, "unreadable event skipped", "event", "fraud.event", "outcome", "failure",
			"reason", "malformed_event", "partition", m.Partition, "offset", m.Offset, "error", err)
		return true
	}
	if !ok {
		return true
	}
	// Continue the triggering event's trace with a new span, as the HTTP
	// middleware does for requests.
	span := newSpanID()
	log = log.With("trace_id", s.TraceID, "span_id", span)
	ctx = logger.WithContext(ctx, log)

	var seen bool
	if !e.retryRedis(ctx, "fraud_seen", func() (err error) { seen, err = e.seen.Seen(ctx, s.EventID); return err }) {
		return false
	}
	if seen {
		return true
	}
	var d Decision
	var fire bool
	if !e.retryRedis(ctx, "fraud_window", func() (err error) { d, fire, err = e.eval.Evaluate(ctx, s); return err }) {
		return false
	}
	if fire && !e.freeze(ctx, d, s, span) {
		return false
	}
	return e.retryRedis(ctx, "fraud_mark_seen", func() error { return e.seen.Mark(ctx, s.EventID) })
}

// freeze retries until the freeze succeeds, is rejected, or ctx ends.
// Each FreezeRetry window that passes without success logs a failure; the
// event stays unprocessed (and its batch uncommitted) meanwhile.
func (e *Engine) freeze(ctx context.Context, d Decision, s Settlement, span string) bool {
	log := logger.FromContext(ctx)
	attrs := []any{"event", "fraud.decision", "rule", d.Rule, "account_id", d.AccountID, "transaction_id", s.TransactionID,
		"count", d.Count, "window_start", d.WindowStart.UTC().Format(time.RFC3339Nano)}
	if d.Currency != "" {
		attrs = append(attrs, "currency", d.Currency, "amount", d.Sum)
	}
	windowStart := e.now()
	backoff := e.cfg.MinBackoff
	for attempt := 1; ; attempt++ {
		err := e.freezer.Freeze(ctx, d, s.TraceID, span)
		switch {
		case err == nil:
			log.WarnContext(ctx, "account frozen by fraud rule", append(attrs, "outcome", "success", "attempts", attempt,
				"detect_ms", float64(e.now().Sub(s.OccurredAt).Microseconds())/1000)...)
			return true
		case ctx.Err() != nil:
			return false
		case errors.Is(err, ErrFreezeRejected):
			// Retrying can't fix a 4xx (token, grant, unknown account): report it
			// for the alert and move on rather than block the partition forever.
			log.ErrorContext(ctx, "fraud freeze rejected", append(attrs, "outcome", "failure", "reason", "freeze_rejected",
				"attempts", attempt, "error", err)...)
			return true
		}
		if e.now().Sub(windowStart) >= e.cfg.FreezeRetry {
			log.ErrorContext(ctx, "fraud freeze still failing, retrying", append(attrs, "outcome", "failure",
				"reason", "freeze_call_failed", "attempts", attempt, "error", err)...)
			windowStart = e.now()
		}
		backoff = e.sleep(ctx, backoff)
		if ctx.Err() != nil {
			return false
		}
	}
}

// retryRedis runs fn until it succeeds or ctx ends, logging each failure.
func (e *Engine) retryRedis(ctx context.Context, op string, fn func() error) bool {
	backoff := e.cfg.MinBackoff
	for {
		err := fn()
		if err == nil {
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		logger.FromContext(ctx).WarnContext(ctx, "redis call failed, retrying", "event", "dependency.call", "outcome", "failure",
			"reason", depReason(err), "dependency", "redis", "operation", op, "error", err)
		backoff = e.sleep(ctx, backoff)
	}
}

// sleep waits backoff (or until ctx ends) and returns the next backoff.
func (e *Engine) sleep(ctx context.Context, backoff time.Duration) time.Duration {
	t := time.NewTimer(backoff)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
	return min(backoff*2, e.cfg.MaxBackoff)
}

func depReason(err error) string {
	var netErr net.Error
	switch {
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	}
	return "unknown"
}

func newSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
