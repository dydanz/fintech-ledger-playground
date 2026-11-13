package service

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"fintech-ledger-playground/internal/logger"
)

// defaultCacheRetry is LedgerConfig.CacheRetry when it's unset.
const defaultCacheRetry = 5 * time.Second

// cacheBreaker stops calls to the idempotency cache after one fails, so a
// Redis outage costs at most one cacheTimeout per CacheRetry instead of two
// per write (chaos runbook §6.2, #60). Writes go straight to Postgres, which
// is the authority anyway. After CacheRetry one call probes Redis; the rest
// keep skipping until it answers. The outage is logged when it starts and
// when it ends, not per write.
type cacheBreaker struct {
	retry time.Duration
	open  atomic.Bool // fast path: closed means call the cache

	mu      sync.Mutex
	since   time.Time // the failure that opened it
	probeAt time.Time // the next call after this probes Redis
	skipped int64     // calls skipped while open
}

// allow reports whether to call the cache now: always while closed; while
// open, only for the one probe per retry interval.
func (b *cacheBreaker) allow(now time.Time) bool {
	if !b.open.Load() {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open.Load() {
		return true
	}
	if now.Before(b.probeAt) {
		b.skipped++
		return false
	}
	b.probeAt = now.Add(b.retry) // other callers skip while this one probes
	return true
}

// failed records a failed call and reports whether it opened the breaker.
func (b *cacheBreaker) failed(now time.Time) (opened bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probeAt = now.Add(b.retry)
	if b.open.Load() {
		return false
	}
	b.since, b.skipped = now, 0
	b.open.Store(true)
	return true
}

// succeeded records a successful call; if it closed the breaker it returns
// how long the cache was out and how many calls were skipped.
func (b *cacheBreaker) succeeded(now time.Time) (closed bool, down time.Duration, skipped int64) {
	if !b.open.Load() {
		return false, 0, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.open.Load() {
		return false, 0, 0
	}
	b.open.Store(false)
	return true, now.Sub(b.since), b.skipped
}

func (s *LedgerService) cacheFailed(ctx context.Context, operation string, err error) {
	if s.breaker.failed(s.now()) {
		cacheUnavailable(ctx, operation, err, s.breaker.retry)
	}
}

func (s *LedgerService) cacheOK(ctx context.Context, operation string) {
	if closed, down, skipped := s.breaker.succeeded(s.now()); closed {
		logger.FromContext(ctx).InfoContext(ctx, "idempotency cache available again", "event", "dependency.call",
			"outcome", "success", "dependency", "redis", "operation", operation,
			"down_ms", float64(down.Microseconds())/1000, "skipped", skipped)
	}
}
