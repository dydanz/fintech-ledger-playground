package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/routing"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

// RouteSource is the live routing chain (routing.Watcher).
type RouteSource interface {
	Chain() *routing.Chain
}

// RouterStore is what the router needs from the repository.
type RouterStore interface {
	TxStore
	ListInState(ctx context.Context, state transfer.State, limit int) ([]string, error)
}

// Router is the job that routes RECEIVED transfers (P§3.1): RECEIVED →
// ROUTED with the decision stored on the transfer, or, when no candidate is
// left, on to FAILED with the core's result in the same transaction.
type Router struct {
	store  RouterStore
	routes RouteSource
	now    func() time.Time
	batch  int
}

func NewRouter(store RouterStore, routes RouteSource, now func() time.Time) *Router {
	return &Router{store: store, routes: routes, now: now, batch: 100}
}

// Run routes up to one batch. Each transfer is its own transaction, locked
// with SKIP LOCKED and re-checked, so concurrent routers route it once.
func (r *Router) Run(ctx context.Context) error {
	ids, err := r.store.ListInState(ctx, transfer.Received, r.batch)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := r.route(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *Router) route(ctx context.Context, id string) error {
	var (
		res     routing.Result
		changes []change
		amount  int64
		curr    string
	)
	err := r.store.InTx(ctx, func(tx *repository.Tx) error {
		t, ok, err := tx.TryLockTransfer(ctx, id)
		if err != nil || !ok || t.State != transfer.Received {
			return err
		}
		amount, curr = t.Amount, t.Currency
		now := r.now()
		res = r.routes.Chain().Route(ctx, routing.Request{TransferID: t.ID, Amount: t.Amount, Currency: t.Currency, BankCode: t.BankCode, Now: now})
		stored, err := json.Marshal(res)
		if err != nil {
			return err
		}
		if err := tx.SetRouting(ctx, t.ID, stored); err != nil {
			return err
		}
		detail, err := json.Marshal(map[string]any{"config_version": res.ConfigVersion, "candidates": res.Candidates})
		if err != nil {
			return err
		}
		c, err := transition(ctx, tx, &t, transfer.Routed, contract.EventTransferRouted, now, contract.JournalPayload{Detail: detail})
		if err != nil {
			return err
		}
		changes = append(changes, c)
		if len(res.Candidates) > 0 {
			return nil
		}
		c, err = finish(ctx, tx, &t, now, contract.TransferResult{Status: contract.StatusFailed, Reason: contract.Reason(res.NoCandidateReason())})
		changes = append(changes, c)
		return err
	})
	log := logger.FromContext(ctx)
	attrs := []any{"event", "transfer.route", "transfer_id", id}
	switch {
	case err != nil:
		log.ErrorContext(ctx, "transfer not routed", append(attrs, "outcome", "failure", "reason", "pg_error", "error", err)...)
		return fmt.Errorf("route %s: %w", id, err)
	case len(changes) == 0: // another router had it, or it had moved on
		return nil
	case len(res.Candidates) == 0:
		log.WarnContext(ctx, "transfer has no route; failed", append(attrs, "outcome", "denied", "reason", "no_candidate",
			"result_reason", res.NoCandidateReason(), "config_version", res.ConfigVersion, "amount", amount, "currency", curr)...)
	default:
		first := res.Candidates[0]
		log.InfoContext(ctx, "transfer routed", append(attrs, "outcome", "success", "candidates", len(res.Candidates),
			"bank_middleware", first.BankMiddleware, "rail", first.Rail, "config_version", res.ConfigVersion)...)
	}
	logChanges(ctx, changes)
	return nil
}
