package routing

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"fintech-ledger-playground/internal/logger"
)

// Watcher holds the live chain and reloads routing.yaml when it changes
// (P§3.1: reloadable without a code change). A file that doesn't read or
// validate keeps the previous chain, and is logged once (Review Focus 4).
type Watcher struct {
	path    string
	current atomic.Pointer[Chain]
	seen    [sha256.Size]byte // the content last acted on, good or bad
	missing bool
}

// NewWatcher loads path. At startup there's no previous chain, so a missing
// or invalid file is an error.
func NewWatcher(ctx context.Context, path string) (*Watcher, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("routing: %w", err)
	}
	c, err := Parse(b)
	if err != nil {
		return nil, err
	}
	w := &Watcher{path: path, seen: sha256.Sum256(b)}
	w.current.Store(c)
	log := logger.FromContext(ctx)
	log.InfoContext(ctx, "routing configuration loaded", "event", "transfer.routing_config", "outcome", "success",
		"reason", "loaded", "config_version", c.Version(), "rules", len(c.rules))
	return w, nil
}

// Chain is the live chain.
func (w *Watcher) Chain() *Chain { return w.current.Load() }

// Run checks the file every interval until ctx ends.
func (w *Watcher) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.Check(ctx)
		}
	}
}

// Check reloads the file if its content changed since the last check. Run
// calls it; tests call it directly. Not safe for concurrent use.
func (w *Watcher) Check(ctx context.Context) {
	log := logger.FromContext(ctx)
	b, err := os.ReadFile(w.path)
	if err != nil {
		if !w.missing {
			w.missing = true
			log.ErrorContext(ctx, "routing configuration unreadable; keeping the live chain", "event", "transfer.routing_config",
				"outcome", "failure", "reason", "unreadable", "config_version", w.Chain().Version(), "error", err)
		}
		return
	}
	w.missing = false
	sum := sha256.Sum256(b)
	if sum == w.seen {
		return
	}
	w.seen = sum
	c, err := Parse(b)
	if err != nil {
		log.ErrorContext(ctx, "routing configuration rejected; keeping the live chain", "event", "transfer.routing_config",
			"outcome", "failure", "reason", "invalid_config", "config_version", w.Chain().Version(), "error", err)
		return
	}
	old := w.Chain().Version()
	w.current.Store(c)
	log.InfoContext(ctx, "routing configuration reloaded", "event", "transfer.routing_config", "outcome", "success",
		"reason", "reloaded", "config_version", c.Version(), "previous_version", old, "rules", len(c.rules))
}
