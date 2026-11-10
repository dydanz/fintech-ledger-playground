package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
)

// RecoveryStore is what the ledger-worker jobs read and delete.
type RecoveryStore interface {
	ListStuckTransactions(ctx context.Context, olderThan time.Time, limit int) ([]repository.StuckTransaction, error)
	PurgeIdempotencyKeys(ctx context.Context, limit int) (int64, error)
}

type RecoveryConfig struct {
	SweepAfter    time.Duration // LEDGER_SWEEP_AFTER: PENDING this long is stuck
	PendingMaxAge time.Duration // LEDGER_PENDING_MAX_AGE: then FAILED processing_timeout
	SweepBatch    int           // transactions per sweep
	PurgeBatch    int           // idempotency keys deleted per statement
}

// Recovery holds the ledger-worker jobs that keep the write path whole: the
// stuck-transaction sweeper (TRD §7.5) and the idempotency-key purge.
type Recovery struct {
	ledger *LedgerService
	store  RecoveryStore
	cfg    RecoveryConfig
	now    func() time.Time
}

func NewRecovery(l *LedgerService, store RecoveryStore, cfg RecoveryConfig) (*Recovery, error) {
	if cfg.SweepAfter < 0 || cfg.PendingMaxAge < cfg.SweepAfter {
		return nil, errors.New("recovery: need 0 <= sweep after <= pending max age")
	}
	if cfg.SweepBatch < 1 || cfg.PurgeBatch < 1 {
		return nil, errors.New("recovery: batch sizes must be positive")
	}
	return &Recovery{ledger: l, store: store, cfg: cfg, now: time.Now}, nil
}

// Sweep re-runs Tx2 for every transaction PENDING longer than SweepAfter, or
// fails it with processing_timeout once it is older than PendingMaxAge. Each
// transaction it moves emits ledger.recover (and, through Process, its own
// terminal event). It returns the first error only after trying them all.
func (r *Recovery) Sweep(ctx context.Context) error {
	log := logger.FromContext(ctx)
	now := r.now()
	stuck, err := r.store.ListStuckTransactions(ctx, now.Add(-r.cfg.SweepAfter), r.cfg.SweepBatch)
	if err != nil {
		log.ErrorContext(ctx, "sweep failed to list stuck transactions", "event", "ledger.recover",
			"outcome", "failure", "reason", "pg_error", "error", err)
		return err
	}
	var first error
	for _, t := range stuck {
		age := now.Sub(t.CreatedAt)
		expire := age >= r.cfg.PendingMaxAge
		attrs := []any{"event", "ledger.recover", "transaction_id", t.ID, "account_id", t.AccountID,
			"age_ms", float64(age.Microseconds()) / 1000}
		_, final, err := r.ledger.process(ctx, ProcessInput{TransactionID: t.ID, ClientID: t.ClientID,
			KeyHash: t.KeyHash, RequestID: "sweeper", Expire: expire})
		switch {
		case err != nil:
			reason := "pg_error"
			if errors.Is(err, repository.ErrTimeout) {
				reason = "timeout"
			}
			log.ErrorContext(ctx, "stuck transaction recovery failed", append(attrs, "outcome", "failure",
				"reason", reason, "error", err)...)
			if first == nil {
				first = fmt.Errorf("recover %s: %w", t.ID, err)
			}
		case final.ID == "":
			// The request path finished it between the list and the lock.
		default:
			reason := "stuck_pending"
			if expire {
				reason = "processing_timeout"
			}
			log.WarnContext(ctx, "stuck transaction recovered", append(attrs, "outcome", "success", "reason", reason,
				"status", string(final.Status), "key_found", t.KeyHash != "")...)
		}
	}
	return first
}

// PurgeIdempotencyKeys deletes expired keys in batches until none are left
// or ctx ends. A key past IDEMPOTENCY_TTL is treated as new anyway (§6.4).
func (r *Recovery) PurgeIdempotencyKeys(ctx context.Context) error {
	var total int64
	for ctx.Err() == nil {
		n, err := r.store.PurgeIdempotencyKeys(ctx, r.cfg.PurgeBatch)
		if err != nil {
			logger.FromContext(ctx).ErrorContext(ctx, "idempotency purge failed", "event", "ledger.purge",
				"outcome", "failure", "reason", "pg_error", "error", err, "deleted", total)
			return err
		}
		total += n
		if n < int64(r.cfg.PurgeBatch) {
			break
		}
	}
	if total > 0 {
		logger.FromContext(ctx).InfoContext(ctx, "expired idempotency keys purged", "event", "ledger.purge",
			"outcome", "success", "deleted", total)
	}
	return nil
}
