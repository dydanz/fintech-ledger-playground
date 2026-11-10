package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
)

// RebuildStore is what rebuild-projections reads and writes (repository.RebuildRepo).
type RebuildStore interface {
	ListTransactionIDs(ctx context.Context, since time.Time, after string, limit int) ([]string, error)
	ListAccountIDs(ctx context.Context, since time.Time, after string, limit int) ([]string, error)
	ListUserLedgerIDs(ctx context.Context, after string, limit int) ([]string, error)
	ListAccountTransactionIDs(ctx context.Context, accountID, after string, limit int) ([]string, error)
	ListAccountLedgerIDs(ctx context.Context, accountID string) ([]string, error)
	EventsHorizon(ctx context.Context) (time.Time, error)
	InTx(ctx context.Context, commit bool, fn func(repository.RebuildTx) error) error
}

// RebuildOptions selects what rebuild-projections does.
type RebuildOptions struct {
	Apply     bool   // write the rebuilt rows; otherwise only report the diff
	AccountID string // only this account, its ledgers and its transactions; "" is everything
}

// RebuildDiff is one column whose projection differs from what the source
// of truth implies.
type RebuildDiff struct {
	Kind    string // account, transaction or ledger
	ID      string
	Field   string
	Current any
	Rebuilt any
}

// RebuildReport is the outcome of one rebuild.
type RebuildReport struct {
	Apply   bool
	Checked int           // rows examined
	Changed int           // rows that differed (written when Apply)
	Failed  int           // rows that could not be rebuilt
	Skipped int           // accounts and transactions whose streams start in an archived month
	Diffs   []RebuildDiff // every differing column
}

// Rebuilder recomputes the projections (ledger.accounts, ledger.ledgers,
// ledger.transactions) from the events and the journal: the reviewed repair
// path for what the reconciler reports (TRD §7.6). By default it only
// reports; it writes only with RebuildOptions.Apply.
//
// Each row is rebuilt in its own transaction under the row lock the write
// path takes, so the result is exact while ledger-api keeps serving. The
// event streams and journal are never touched.
type Rebuilder struct {
	store   RebuildStore
	batch   int
	horizon time.Time // streams created before it may have archived events
}

func NewRebuilder(store RebuildStore, batch int) (*Rebuilder, error) {
	if batch < 1 {
		return nil, errors.New("rebuilder: batch must be positive")
	}
	return &Rebuilder{store: store, batch: batch}, nil
}

var (
	errUnrebuildable = errors.New("source of truth does not fold")
	// errArchivedStream: the row's stream starts in an archived month, so its
	// events aren't all in Postgres; the archive's manifest is its record.
	errArchivedStream = errors.New("stream partly archived")
)

// Rebuild rebuilds accounts, then transactions, then USER ledgers. A row
// that fails is logged and skipped; the error returned reports how many.
func (b *Rebuilder) Rebuild(ctx context.Context, opts RebuildOptions) (RebuildReport, error) {
	log := logger.FromContext(ctx)
	start := time.Now()
	rep := RebuildReport{Apply: opts.Apply}
	mode := "dry_run"
	if opts.Apply {
		mode = "apply"
	}
	run := func(kind string, ids []string, one func(context.Context, repository.RebuildTx, string) ([]RebuildDiff, error)) {
		for _, id := range ids {
			if ctx.Err() != nil {
				return
			}
			var diffs []RebuildDiff
			err := b.store.InTx(ctx, opts.Apply, func(tx repository.RebuildTx) error {
				var err error
				diffs, err = one(ctx, tx, id)
				return err
			})
			rep.Checked++
			if errors.Is(err, errArchivedStream) {
				rep.Skipped++
				continue
			}
			if err != nil {
				rep.Failed++
				reason := "pg_error"
				switch {
				case errors.Is(err, repository.ErrTimeout):
					reason = "timeout"
				case errors.Is(err, errUnrebuildable):
					reason = "invalid_stream"
				}
				log.ErrorContext(ctx, "projection rebuild failed", "event", "ledger.rebuild", "outcome", "failure",
					"reason", reason, "error", err, "mode", mode, "kind", kind, kindIDKey(kind), id)
				continue
			}
			if len(diffs) == 0 {
				continue
			}
			rep.Changed++
			rep.Diffs = append(rep.Diffs, diffs...)
			for _, d := range diffs {
				log.WarnContext(ctx, "projection differs from its source", "event", "ledger.rebuild", "outcome", "success",
					"mode", mode, "applied", opts.Apply, "kind", kind, kindIDKey(kind), id,
					"field", d.Field, "current", d.Current, "rebuilt", d.Rebuilt)
			}
		}
	}

	accounts, transactions, ledgers, err := b.scope(ctx, opts.AccountID)
	if err == nil {
		b.horizon, err = b.store.EventsHorizon(ctx)
	}
	if err != nil {
		log.ErrorContext(ctx, "projection rebuild could not list rows", "event", "ledger.rebuild", "outcome", "failure",
			"reason", "pg_error", "error", err, "mode", mode)
		return rep, err
	}
	run("account", accounts, b.account)
	run("transaction", transactions, b.transaction)
	run("ledger", ledgers, b.ledger)

	summary := []any{"event", "ledger.rebuild", "mode", mode, "checked", rep.Checked, "changed", rep.Changed,
		"failed", rep.Failed, "skipped_archived", rep.Skipped, "duration_ms", float64(time.Since(start).Microseconds()) / 1000}
	if opts.AccountID != "" {
		summary = append(summary, "account_id", opts.AccountID)
	}
	if err := ctx.Err(); err != nil {
		log.ErrorContext(ctx, "projection rebuild interrupted", append(summary, "outcome", "failure", "reason", "interrupted")...)
		return rep, err
	}
	if rep.Failed > 0 {
		log.ErrorContext(ctx, "projection rebuild finished with failures", append(summary, "outcome", "failure", "reason", "rows_failed")...)
		return rep, fmt.Errorf("rebuild: %d row(s) failed", rep.Failed)
	}
	log.InfoContext(ctx, "projection rebuild finished", append(summary, "outcome", "success")...)
	return rep, nil
}

func kindIDKey(kind string) string { return kind + "_id" }

func (b *Rebuilder) scope(ctx context.Context, accountID string) (accounts, transactions, ledgers []string, err error) {
	all := func(list func(after string) ([]string, error)) ([]string, error) {
		return collect(ctx, list, nil)
	}
	if accountID != "" {
		accounts = []string{accountID}
		if transactions, err = all(func(after string) ([]string, error) {
			return b.store.ListAccountTransactionIDs(ctx, accountID, after, b.batch)
		}); err != nil {
			return
		}
		ledgers, err = b.store.ListAccountLedgerIDs(ctx, accountID)
		return
	}
	if accounts, err = all(func(after string) ([]string, error) {
		return b.store.ListAccountIDs(ctx, time.Time{}, after, b.batch)
	}); err != nil {
		return
	}
	if transactions, err = all(func(after string) ([]string, error) {
		return b.store.ListTransactionIDs(ctx, time.Time{}, after, b.batch)
	}); err != nil {
		return
	}
	ledgers, err = all(func(after string) ([]string, error) {
		return b.store.ListUserLedgerIDs(ctx, after, b.batch)
	})
	return
}

func (b *Rebuilder) account(ctx context.Context, tx repository.RebuildTx, id string) ([]RebuildDiff, error) {
	cur, err := tx.LockAccount(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.CreatedAt.Before(b.horizon) {
		return nil, errArchivedStream
	}
	stored, err := tx.StreamEvents(ctx, ledger.StreamAccount, id)
	if err != nil {
		return nil, err
	}
	s, err := foldStored(stored, ledger.FoldAccount)
	if err != nil {
		return nil, err
	}
	d := differ{kind: "account", id: id}
	d.check("user_id", cur.UserID, s.UserID)
	d.check("status", string(cur.Status), string(s.Status))
	d.check("version", cur.Version, s.Version)
	if len(d.out) > 0 {
		return d.out, tx.SetAccount(ctx, id, s.UserID, s.Status, s.Version)
	}
	return nil, nil
}

func (b *Rebuilder) transaction(ctx context.Context, tx repository.RebuildTx, id string) ([]RebuildDiff, error) {
	cur, err := tx.LockTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if cur.CreatedAt.Before(b.horizon) {
		return nil, errArchivedStream
	}
	stored, err := tx.StreamEvents(ctx, ledger.StreamTransaction, id)
	if err != nil {
		return nil, err
	}
	s, err := foldStored(stored, ledger.FoldTransaction)
	if err != nil {
		return nil, err
	}
	counterparty, err := tx.SystemLedgerID(ctx, s.Counterparty, s.Currency)
	if err != nil {
		return nil, fmt.Errorf("counterparty %s/%s: %w", s.Counterparty, s.Currency, err)
	}
	want := repository.TransactionProjection{ID: id, AccountID: s.AccountID, LedgerID: s.LedgerID,
		CounterpartyLedgerID: counterparty, Type: s.Type, Status: s.Status, Amount: s.Amount, Currency: s.Currency,
		OriginalTransactionID: s.OriginalTransactionID, FailureReason: s.FailureReason, RequestedBy: s.RequestedBy,
		Version: s.Version}
	if s.Status == ledger.StatusSettled || s.Status == ledger.StatusVoided {
		// settled_at is Tx2's now(), which also stamped the TransactionSettled event.
		for _, e := range stored {
			if e.Type == ledger.EventTransactionSettled {
				at := e.OccurredAt
				want.SettledAt = &at
			}
		}
	}
	d := differ{kind: "transaction", id: id}
	d.check("account_id", cur.AccountID, want.AccountID)
	d.check("ledger_id", cur.LedgerID, want.LedgerID)
	d.check("counterparty_ledger_id", cur.CounterpartyLedgerID, want.CounterpartyLedgerID)
	d.check("type", string(cur.Type), string(want.Type))
	d.check("status", string(cur.Status), string(want.Status))
	d.check("amount", cur.Amount, want.Amount)
	d.check("currency", cur.Currency, want.Currency)
	d.check("original_transaction_id", cur.OriginalTransactionID, want.OriginalTransactionID)
	d.check("failure_reason", string(cur.FailureReason), string(want.FailureReason))
	d.check("requested_by", cur.RequestedBy, want.RequestedBy)
	d.check("version", cur.Version, want.Version)
	d.check("settled_at", timeString(cur.SettledAt), timeString(want.SettledAt))
	if len(d.out) > 0 {
		return d.out, tx.SetTransaction(ctx, want)
	}
	return nil, nil
}

func (b *Rebuilder) ledger(ctx context.Context, tx repository.RebuildTx, id string) ([]RebuildDiff, error) {
	cur, err := tx.LockLedger(ctx, id)
	if err != nil {
		return nil, err
	}
	balance, last, err := tx.LedgerFromJournal(ctx, id)
	if err != nil {
		return nil, err
	}
	d := differ{kind: "ledger", id: id}
	d.check("balance", cur.Balance, balance)
	d.check("last_sequence", cur.LastSequence, last)
	if len(d.out) > 0 {
		return d.out, tx.SetLedger(ctx, id, balance, last)
	}
	return nil, nil
}

type differ struct {
	kind, id string
	out      []RebuildDiff
}

func (d *differ) check(field string, current, rebuilt any) {
	if current != rebuilt {
		d.out = append(d.out, RebuildDiff{Kind: d.kind, ID: d.id, Field: field, Current: current, Rebuilt: rebuilt})
	}
}

func timeString(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func foldStored[S any](stored []repository.StoredEvent, fold func([]ledger.Event) (S, error)) (S, error) {
	var zero S
	events, bad := decodeStream(stored)
	switch {
	case bad != "":
		return zero, fmt.Errorf("%w: %s payload does not decode", errUnrebuildable, bad)
	case len(events) == 0:
		return zero, fmt.Errorf("%w: no events", errUnrebuildable)
	}
	s, err := fold(events)
	if err != nil {
		return zero, fmt.Errorf("%w: %w", errUnrebuildable, err)
	}
	return s, nil
}
