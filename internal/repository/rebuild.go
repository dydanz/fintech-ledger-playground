package repository

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"fintech-ledger-playground/internal/ledger"
)

// rebuild-projections (spec/TRD §7.6): recompute each projection row from the
// source of truth under that row's lock, one row per transaction, so it can
// run while ledger-api is serving.

// TransactionProjection is every column of a ledger.transactions row that
// the event stream determines. reference, metadata and trace_id are caller
// data the rebuild leaves alone.
type TransactionProjection struct {
	ID                    string
	AccountID             string
	LedgerID              string // "" is NULL
	CounterpartyLedgerID  string
	Type                  ledger.TransactionType
	Status                ledger.Status
	Amount                int64
	Currency              string
	OriginalTransactionID string // "" is NULL
	FailureReason         ledger.FailureReason
	RequestedBy           string
	Version               int64
	SettledAt             *time.Time
}

// RebuildTx is one row's rebuild: lock it, read its sources, maybe write it.
type RebuildTx interface {
	LockAccount(ctx context.Context, id string) (AccountRecord, error)
	LockTransaction(ctx context.Context, id string) (TransactionRecord, error)
	LockLedger(ctx context.Context, id string) (LedgerRecord, error)
	StreamEvents(ctx context.Context, stream ledger.StreamType, id string) ([]StoredEvent, error)
	SystemLedgerID(ctx context.Context, code, currency string) (string, error)
	// LedgerFromJournal is the balance and last sequence the journal implies:
	// the latest checkpoint plus every later line.
	LedgerFromJournal(ctx context.Context, id string) (balance, lastSequence int64, err error)
	SetAccount(ctx context.Context, id, userID string, status ledger.AccountStatus, version int64) error
	SetTransaction(ctx context.Context, p TransactionProjection) error
	SetLedger(ctx context.Context, id string, balance, lastSequence int64) error
}

// RebuildRepo lists rows like ReconcileRepo and runs each row's rebuild.
type RebuildRepo struct {
	*ReconcileRepo
	db      *pgxpool.Pool
	timeout time.Duration
}

// NewRebuildRepo bounds every lock wait and statement by timeout.
func NewRebuildRepo(db *pgxpool.Pool, timeout time.Duration) *RebuildRepo {
	return &RebuildRepo{ReconcileRepo: NewReconcileRepo(db), db: db, timeout: timeout}
}

// ListAccountTransactionIDs pages through one account's transactions.
func (r *RebuildRepo) ListAccountTransactionIDs(ctx context.Context, accountID, after string, limit int) ([]string, error) {
	return listIDs(ctx, r.db, "list account transactions", `SELECT id::text FROM ledger.transactions
		WHERE account_id = $3 AND id > $1 ORDER BY id LIMIT $2`, afterID(after), limit, accountID)
}

// EventsHorizon is the end of the latest archived events month (§7.8): a
// stream that started before it may have lost its first events. Zero when
// nothing is archived.
func (r *RebuildRepo) EventsHorizon(ctx context.Context) (time.Time, error) {
	var t *time.Time
	if err := r.db.QueryRow(ctx, `SELECT max(month) + interval '1 month' FROM ledger.archives
		WHERE table_name = 'events'`).Scan(&t); err != nil {
		return time.Time{}, fmt.Errorf("events horizon: %w", err)
	}
	if t == nil {
		return time.Time{}, nil
	}
	return *t, nil
}

// ListAccountLedgerIDs returns one account's USER ledgers.
func (r *RebuildRepo) ListAccountLedgerIDs(ctx context.Context, accountID string) ([]string, error) {
	return listIDs(ctx, r.db, "list account ledgers", `SELECT id::text FROM ledger.ledgers
		WHERE kind = 'USER' AND account_id = $1 ORDER BY id`, accountID)
}

// InTx runs fn in a READ COMMITTED transaction. It commits only when commit
// is true; a dry run rolls back, releasing the locks it took.
func (r *RebuildRepo) InTx(ctx context.Context, commit bool, fn func(RebuildTx) error) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapTxErr(fmt.Errorf("begin: %w", err))
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after Commit
	ms := strconv.FormatInt(max(r.timeout.Milliseconds(), 1), 10) + "ms"
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $1, true)`, ms); err != nil {
		return mapTxErr(fmt.Errorf("set timeouts: %w", err))
	}
	if err := fn(rebuildQueries{q: ledgerQueries{db: tx}, r: reconcileQueries{db: tx}}); err != nil {
		return mapTxErr(err)
	}
	if !commit {
		return nil
	}
	if err := tx.Commit(ctx); err != nil {
		return mapTxErr(fmt.Errorf("commit: %w", err))
	}
	return nil
}

type rebuildQueries struct {
	q ledgerQueries
	r reconcileQueries
}

// LockAccount takes the same exclusive advisory and row lock as a freeze.
func (b rebuildQueries) LockAccount(ctx context.Context, id string) (AccountRecord, error) {
	return b.q.lockAccount(ctx, id, LockUpdate)
}

func (b rebuildQueries) LockTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	return b.q.lockTransaction(ctx, id)
}

func (b rebuildQueries) LockLedger(ctx context.Context, id string) (LedgerRecord, error) {
	l, err := scanLedger(b.q.db.QueryRow(ctx, `SELECT `+ledgerColumns+` FROM ledger.ledgers
		WHERE id = $1 AND kind = 'USER' FOR UPDATE`, id))
	return l, wrapRead("lock ledger", err)
}

func (b rebuildQueries) StreamEvents(ctx context.Context, stream ledger.StreamType, id string) ([]StoredEvent, error) {
	return b.r.StreamEvents(ctx, stream, []string{id})
}

func (b rebuildQueries) SystemLedgerID(ctx context.Context, code, currency string) (string, error) {
	l, err := b.q.getSystemLedger(ctx, code, currency)
	return l.ID, err
}

func (b rebuildQueries) LedgerFromJournal(ctx context.Context, id string) (balance, lastSequence int64, err error) {
	err = b.q.db.QueryRow(ctx, `
		WITH cp AS (
			SELECT coalesce(max(as_of_sequence), 0) AS seq,
			       coalesce((SELECT c.balance FROM ledger.checkpoints c WHERE c.ledger_id = $1
			                  ORDER BY c.as_of_sequence DESC LIMIT 1), 0) AS balance
			  FROM ledger.checkpoints WHERE ledger_id = $1
		)
		SELECT cp.balance + coalesce(sum(j.amount), 0)::bigint, greatest(cp.seq, coalesce(max(j.ledger_sequence), 0))
		  FROM cp LEFT JOIN ledger.journal_entries j ON j.ledger_id = $1 AND j.ledger_sequence > cp.seq
		 GROUP BY cp.balance, cp.seq`, id).Scan(&balance, &lastSequence)
	if err != nil {
		return 0, 0, fmt.Errorf("ledger from journal: %w", err)
	}
	return balance, lastSequence, nil
}

func (b rebuildQueries) SetAccount(ctx context.Context, id, userID string, status ledger.AccountStatus, version int64) error {
	tag, err := b.q.db.Exec(ctx, `UPDATE ledger.accounts SET user_id = $2, status = $3, version = $4, updated_at = now()
		WHERE id = $1`, id, userID, status, version)
	return wrapWrite("rebuild account", tag, err)
}

func (b rebuildQueries) SetTransaction(ctx context.Context, p TransactionProjection) error {
	tag, err := b.q.db.Exec(ctx, `UPDATE ledger.transactions SET
		account_id = $2, ledger_id = $3, counterparty_ledger_id = $4, type = $5, status = $6, amount = $7,
		currency = $8, original_transaction_id = $9, failure_reason = $10, requested_by = $11, version = $12,
		settled_at = $13, updated_at = now()
		WHERE id = $1`,
		p.ID, p.AccountID, nullIfEmpty(p.LedgerID), p.CounterpartyLedgerID, p.Type, p.Status, p.Amount,
		p.Currency, nullIfEmpty(p.OriginalTransactionID), nullIfEmpty(string(p.FailureReason)), p.RequestedBy, p.Version,
		p.SettledAt)
	return wrapWrite("rebuild transaction", tag, err)
}

func (b rebuildQueries) SetLedger(ctx context.Context, id string, balance, lastSequence int64) error {
	return b.q.updateLedgerBalance(ctx, id, balance, lastSequence)
}
