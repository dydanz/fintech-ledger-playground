package service

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/bmwclient"
	"fintech-ledger-playground/internal/moneyout/breaker"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
)

// Breakers are the circuit breakers (P§3.3, spec/money-out TRD §6.3),
// stored in MOM's database so every instance shares them. The dispatcher
// asks before each new attempt and reports each answer; inquiries never ask.
type Breakers struct {
	store *repository.Store
	cfg   breaker.Config
	now   func() time.Time
	view  atomic.Pointer[map[string]breaker.Row] // for the routing rule, refreshed
}

func NewBreakers(store *repository.Store, cfg breaker.Config) *Breakers {
	b := &Breakers{store: store, cfg: cfg, now: time.Now}
	b.view.Store(&map[string]breaker.Row{})
	return b
}

// Open is routing.BreakerView: whether the breaker was OPEN at the last
// Refresh and is still inside its cool-down. Past the cool-down routing
// keeps the candidate, so the dispatcher can move it to half-open and send
// the probe; otherwise an open breaker would never be asked again. The
// dispatcher's check is the authoritative one.
func (b *Breakers) Open(bmw, rail string) bool {
	r, ok := (*b.view.Load())[bmw+"/"+rail]
	return ok && r.State == breaker.Open && b.now().Before(r.OpenedAt.Add(b.cfg.Cooldown))
}

// Refresh reloads the routing view.
func (b *Breakers) Refresh(ctx context.Context) error {
	rows, err := b.store.ListBreakers(ctx)
	if err != nil {
		return err
	}
	m := make(map[string]breaker.Row, len(rows))
	for _, r := range rows {
		m[r.BankMiddleware+"/"+r.Rail] = r
	}
	b.view.Store(&m)
	return nil
}

// RunRefresh refreshes every interval until ctx ends.
func (b *Breakers) RunRefresh(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		_ = b.Refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// List is every breaker, for GET /bank-middlewares/health.
func (b *Breakers) List(ctx context.Context) ([]breaker.Row, error) { return b.store.ListBreakers(ctx) }

// allowTx decides a new dispatch inside the dispatcher's transaction.
func (b *Breakers) allowTx(ctx context.Context, tx *repository.Tx, bmw, rail string) (breaker.Decision, []note, error) {
	r, err := tx.LockBreaker(ctx, bmw, rail)
	if err != nil {
		return breaker.Decision{}, nil, err
	}
	next, d := b.cfg.Allow(r, b.now())
	if next == r {
		return d, nil, nil
	}
	n, err := b.save(ctx, tx, r, next, d.Change)
	return d, n, err
}

// Record counts one dispatch answer, in its own transaction.
func (b *Breakers) Record(ctx context.Context, bmw, rail string, k breaker.Kind, probe bool) error {
	var notes []note
	err := b.store.InTx(ctx, func(tx *repository.Tx) error {
		r, err := tx.LockBreaker(ctx, bmw, rail)
		if err != nil {
			return err
		}
		next, c := b.cfg.Record(r, k, probe, b.now())
		if next == r {
			return nil
		}
		notes, err = b.save(ctx, tx, r, next, c)
		return err
	})
	if err != nil {
		logger.FromContext(ctx).ErrorContext(ctx, "breaker not updated", "event", "transfer.breaker", "outcome", "failure",
			"reason", "pg_error", "bank_middleware", bmw, "rail", rail, "error", err)
		return err
	}
	for _, n := range notes {
		n(ctx)
	}
	return nil
}

// save writes next (read as prev) and journals c, if any, as BreakerChanged.
func (b *Breakers) save(ctx context.Context, tx *repository.Tx, prev, next breaker.Row, c *breaker.Change) ([]note, error) {
	if c == nil {
		return nil, tx.SaveBreaker(ctx, next, nil)
	}
	id := prev.BankMiddleware + "/" + prev.Rail
	ev, err := contract.NewEvent(contract.EventBreakerChanged, contract.StreamBreaker, id, prev.Version+1, b.now(), "", map[string]any{
		"bank_middleware": prev.BankMiddleware, "rail": prev.Rail, "from_state": c.From, "to_state": c.To, "reason": c.Reason,
		"failures": prev.Failures, "calls": prev.Calls, "consecutive": prev.Consecutive})
	if err != nil {
		return nil, err
	}
	if err := tx.SaveBreaker(ctx, next, &ev); err != nil {
		return nil, fmt.Errorf("breaker %s: %w", id, err)
	}
	return []note{func(ctx context.Context) {
		attrs := []any{"event", "transfer.breaker", "outcome", "success", "reason", c.Reason, "bank_middleware", prev.BankMiddleware,
			"rail", prev.Rail, "from_state", string(c.From), "to_state", string(c.To), "consecutive", prev.Consecutive,
			"calls", prev.Calls, "failures", prev.Failures}
		log := logger.FromContext(ctx)
		if c.To == breaker.Open {
			log.WarnContext(ctx, "circuit breaker opened", attrs...)
			return
		}
		log.InfoContext(ctx, "circuit breaker changed", attrs...)
	}}, nil
}

// breakerKind is what a dispatch answer means for the breaker (P§3.3):
// connection failures, timeouts and RAIL_UNAVAILABLE / BANK_REJECTED /
// AUTH_ERROR count; a business refusal means the bank works; MOM's own
// bugs don't count. The client already turns a refused connection or a
// missing endpoint into RAIL_UNAVAILABLE, a 401 into AUTH_ERROR, and a
// timeout or 5xx into UNKNOWN.
func breakerKind(out bmwclient.Outcome) breaker.Kind {
	if out.Reason == "invalid_input" || out.Reason == "invalid_response" {
		return breaker.Ignore
	}
	switch out.Result.Status {
	case contract.StatusUnknown: // the bank didn't answer in time
		return breaker.Failure
	case contract.StatusFailed:
		switch out.Result.Reason {
		case contract.ReasonRailUnavailable, contract.ReasonBankRejected, contract.ReasonAuthError:
			return breaker.Failure
		}
	}
	return breaker.Success
}
