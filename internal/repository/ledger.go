package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/outbox"
)

var (
	ErrAccountExists = errors.New("account already exists for user")
	ErrLedgerExists  = errors.New("ledger already exists for currency")
	ErrUserNotFound  = errors.New("user not found")
	ErrVoidExists    = errors.New("an active void already exists for the transaction")
	// ErrTimeout means a lock wait or statement exceeded the transaction's
	// timeout. The transaction was rolled back and may be retried.
	ErrTimeout = errors.New("ledger transaction timed out")
)

const (
	pgForeignKeyViolation = "23503"
	pgLockNotAvailable    = "55P03"
	pgQueryCanceled       = "57014"
)

// AccountRecord is a row of the ledger.accounts projection.
type AccountRecord struct {
	ID        string
	UserID    string
	Status    ledger.AccountStatus
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// LedgerKind values match ledger.ledgers.kind.
type LedgerKind string

const (
	LedgerUser   LedgerKind = "USER"
	LedgerSystem LedgerKind = "SYSTEM"
)

// LedgerRecord is a row of ledger.ledgers. Balance and LastSequence are only
// maintained for USER ledgers.
type LedgerRecord struct {
	ID             string
	Kind           LedgerKind
	AccountID      string // USER only
	Code           string // SYSTEM only
	Currency       string
	OverdraftLimit int64
	Balance        int64
	LastSequence   int64
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// TransactionRecord is a row of the ledger.transactions projection.
// LedgerSequence and BalanceAfter come from the user journal line and are
// only filled by reads, for settled transactions.
type TransactionRecord struct {
	ID                    string
	AccountID             string
	LedgerID              string // empty until Tx2 resolves it
	CounterpartyLedgerID  string
	Type                  ledger.TransactionType
	Status                ledger.Status
	Amount                int64
	Currency              string
	OriginalTransactionID string
	FailureReason         ledger.FailureReason
	Reference             string
	Metadata              json.RawMessage // nil when absent
	RequestedBy           string
	Version               int64
	TraceID               string
	CreatedAt             time.Time
	UpdatedAt             time.Time
	SettledAt             *time.Time
	LedgerSequence        int64
	BalanceAfter          int64
}

// TransactionUpdate moves a transaction projection to a new state. Settled
// stamps settled_at = now(), which equals the journal lines' created_at
// because both use the transaction's start time.
type TransactionUpdate struct {
	ID            string
	Status        ledger.Status
	Version       int64
	LedgerID      string // "" leaves it unchanged
	FailureReason ledger.FailureReason
	Settled       bool
}

// JournalLine is one row of ledger.journal_entries. LedgerSequence and
// BalanceAfter are set on the user line and nil on the system line.
type JournalLine struct {
	ID             string
	TransactionID  string
	LedgerID       string
	Amount         int64
	Currency       string
	LedgerSequence *int64
	BalanceAfter   *int64
	TraceID        string
}

// IdempotencyKey is a row of ledger.idempotency_keys. ResponseBody is nil
// while the request is in flight.
type IdempotencyKey struct {
	ClientID       string
	KeyHash        string
	Fingerprint    string
	Operation      string
	ResourceID     string
	ResponseStatus int
	ResponseBody   []byte // exact bytes, replayed verbatim (bytea since migration 003)
	CreatedAt      time.Time
	ExpiresAt      time.Time
}

// LockMode selects the row lock taken by LockAccount.
type LockMode int

const (
	LockShare  LockMode = iota // Tx2: conflicts with a concurrent freeze
	LockUpdate                 // freeze/unfreeze and ledger open
)

// TransactionQuery selects one page of an account's transactions, newest
// first. After, when set, is the (created_at, id) of the last row seen.
type TransactionQuery struct {
	AccountID string
	Currency  string // "" for all currencies
	Limit     int
	After     *TransactionCursor
}

type TransactionCursor struct {
	CreatedAt time.Time
	ID        string
}

// LedgerTx is the set of ledger operations available inside one database
// transaction (TRD §7.1). The service drives Tx1 and Tx2 through it; tests
// can substitute a fake.
type LedgerTx interface {
	InsertAccount(ctx context.Context, a AccountRecord) error
	GetAccountByUserID(ctx context.Context, userID string) (AccountRecord, error)
	LockAccount(ctx context.Context, id string, mode LockMode) (AccountRecord, error)
	UpdateAccount(ctx context.Context, id string, status ledger.AccountStatus, version int64) error

	InsertLedger(ctx context.Context, l LedgerRecord) error
	GetSystemLedger(ctx context.Context, code, currency string) (LedgerRecord, error)
	GetLedger(ctx context.Context, id string) (LedgerRecord, error)
	LockUserLedger(ctx context.Context, accountID, currency string) (LedgerRecord, error)
	UpdateLedgerBalance(ctx context.Context, id string, balance, lastSequence int64) error
	ListUserLedgers(ctx context.Context, accountID string) ([]LedgerRecord, error)

	InsertTransaction(ctx context.Context, t TransactionRecord) error
	LockTransaction(ctx context.Context, id string) (TransactionRecord, error)
	// UpdateTransaction returns the updated row (without journal fields).
	UpdateTransaction(ctx context.Context, u TransactionUpdate) (TransactionRecord, error)

	// PostJournal writes both lines of a transaction in one statement.
	PostJournal(ctx context.Context, user, counterparty JournalLine) error
	// AppendEvent writes e to ledger.events and its envelope to ledger.outbox;
	// the relay is woken once the transaction commits. It returns e with the
	// stored OccurredAt.
	AppendEvent(ctx context.Context, e ledger.Event, ledgerID string) (ledger.Event, error)

	// InsertIdempotencyKey claims a key. It reports false if an unexpired row
	// already holds it; an expired row is replaced.
	InsertIdempotencyKey(ctx context.Context, k IdempotencyKey) (bool, error)
	GetIdempotencyKey(ctx context.Context, clientID, keyHash string) (IdempotencyKey, error)
	// CompleteIdempotencyKey stores the final response and returns the
	// completed row (fingerprint, operation, expires_at), for the Redis cache.
	CompleteIdempotencyKey(ctx context.Context, clientID, keyHash string, status int, body []byte) (IdempotencyKey, error)
}

// dbtx is satisfied by both *pgxpool.Pool and pgx.Tx.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// LedgerRepo is the PostgreSQL access layer for schema ledger.
type LedgerRepo struct {
	db       *pgxpool.Pool
	topic    string
	notifier *outbox.Notifier
}

// NewLedgerRepo returns a repository whose outbox rows target topic. After a
// transaction that wrote outbox rows commits, it calls notifier.Notify to wake
// the relay; a nil notifier leaves the relay to its poll.
func NewLedgerRepo(db *pgxpool.Pool, topic string, notifier *outbox.Notifier) *LedgerRepo {
	return &LedgerRepo{db: db, topic: topic, notifier: notifier}
}

// InTx runs fn in one READ COMMITTED transaction and commits if fn returns
// nil. timeout bounds every lock wait and statement (lock_timeout and
// statement_timeout); exceeding it returns ErrTimeout. Errors raised at
// COMMIT, such as the journal balance check, are returned wrapped.
func (r *LedgerRepo) InTx(ctx context.Context, timeout time.Duration, fn func(LedgerTx) error) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return mapTxErr(fmt.Errorf("begin: %w", err))
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) // no-op after Commit

	ms := strconv.FormatInt(max(timeout.Milliseconds(), 1), 10) + "ms"
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', $1, true), set_config('statement_timeout', $1, true)`, ms); err != nil {
		return mapTxErr(fmt.Errorf("set timeouts: %w", err))
	}
	var wrote bool
	if err := fn(&pgLedgerTx{q: ledgerQueries{db: tx, topic: r.topic, wrote: &wrote}}); err != nil {
		return mapTxErr(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return mapTxErr(fmt.Errorf("commit: %w", err))
	}
	if wrote {
		r.notifier.Notify()
	}
	return nil
}

func mapTxErr(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == pgLockNotAvailable || pgErr.Code == pgQueryCanceled) {
		return fmt.Errorf("%w: %w", ErrTimeout, err)
	}
	return err
}

// Pool-level reads, outside any transaction. They see committed state only.

func (r *LedgerRepo) GetAccount(ctx context.Context, id string) (AccountRecord, error) {
	return r.q().getAccount(ctx, id)
}

func (r *LedgerRepo) GetAccountByUserID(ctx context.Context, userID string) (AccountRecord, error) {
	return r.q().GetAccountByUserID(ctx, userID)
}

func (r *LedgerRepo) ListUserLedgers(ctx context.Context, accountID string) ([]LedgerRecord, error) {
	return r.q().listUserLedgers(ctx, accountID)
}

func (r *LedgerRepo) GetTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	return r.q().getTransaction(ctx, id)
}

func (r *LedgerRepo) ListTransactions(ctx context.Context, q TransactionQuery) ([]TransactionRecord, error) {
	return r.q().listTransactions(ctx, q)
}

// StuckTransaction is a PENDING transaction for the sweeper, with the
// idempotency key whose stored response its Tx2 completes. ClientID and
// KeyHash are empty if the key is gone.
type StuckTransaction struct {
	ID        string
	AccountID string
	CreatedAt time.Time
	ClientID  string
	KeyHash   string
}

// ListStuckTransactions returns up to limit PENDING transactions created
// before olderThan, oldest first (spec/TRD §7.5).
func (r *LedgerRepo) ListStuckTransactions(ctx context.Context, olderThan time.Time, limit int) ([]StuckTransaction, error) {
	return r.q().listStuckTransactions(ctx, olderThan, limit)
}

// PurgeIdempotencyKeys deletes up to limit expired keys and reports how many.
func (r *LedgerRepo) PurgeIdempotencyKeys(ctx context.Context, limit int) (int64, error) {
	return r.q().purgeIdempotencyKeys(ctx, limit)
}

// ListStreamEvents returns a stream's events in version order, for folding.
func (r *LedgerRepo) ListStreamEvents(ctx context.Context, streamID string) ([]ledger.Event, error) {
	return r.q().listStreamEvents(ctx, streamID)
}

func (r *LedgerRepo) q() ledgerQueries { return ledgerQueries{db: r.db, topic: r.topic} }

// pgLedgerTx implements LedgerTx over a pgx.Tx.
type pgLedgerTx struct{ q ledgerQueries }

func (t *pgLedgerTx) InsertAccount(ctx context.Context, a AccountRecord) error {
	return t.q.insertAccount(ctx, a)
}
func (t *pgLedgerTx) GetAccountByUserID(ctx context.Context, userID string) (AccountRecord, error) {
	return t.q.GetAccountByUserID(ctx, userID)
}
func (t *pgLedgerTx) LockAccount(ctx context.Context, id string, mode LockMode) (AccountRecord, error) {
	return t.q.lockAccount(ctx, id, mode)
}
func (t *pgLedgerTx) UpdateAccount(ctx context.Context, id string, status ledger.AccountStatus, version int64) error {
	return t.q.updateAccount(ctx, id, status, version)
}
func (t *pgLedgerTx) InsertLedger(ctx context.Context, l LedgerRecord) error {
	return t.q.insertLedger(ctx, l)
}
func (t *pgLedgerTx) GetLedger(ctx context.Context, id string) (LedgerRecord, error) {
	return t.q.getLedger(ctx, id)
}
func (t *pgLedgerTx) GetSystemLedger(ctx context.Context, code, currency string) (LedgerRecord, error) {
	return t.q.getSystemLedger(ctx, code, currency)
}
func (t *pgLedgerTx) LockUserLedger(ctx context.Context, accountID, currency string) (LedgerRecord, error) {
	return t.q.lockUserLedger(ctx, accountID, currency)
}
func (t *pgLedgerTx) UpdateLedgerBalance(ctx context.Context, id string, balance, lastSequence int64) error {
	return t.q.updateLedgerBalance(ctx, id, balance, lastSequence)
}
func (t *pgLedgerTx) InsertTransaction(ctx context.Context, tr TransactionRecord) error {
	return t.q.insertTransaction(ctx, tr)
}
func (t *pgLedgerTx) LockTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	return t.q.lockTransaction(ctx, id)
}
func (t *pgLedgerTx) UpdateTransaction(ctx context.Context, u TransactionUpdate) (TransactionRecord, error) {
	return t.q.updateTransaction(ctx, u)
}
func (t *pgLedgerTx) ListUserLedgers(ctx context.Context, accountID string) ([]LedgerRecord, error) {
	return t.q.listUserLedgers(ctx, accountID)
}
func (t *pgLedgerTx) PostJournal(ctx context.Context, user, counterparty JournalLine) error {
	return t.q.postJournal(ctx, user, counterparty)
}
func (t *pgLedgerTx) AppendEvent(ctx context.Context, e ledger.Event, ledgerID string) (ledger.Event, error) {
	return t.q.appendEvent(ctx, e, ledgerID)
}
func (t *pgLedgerTx) InsertIdempotencyKey(ctx context.Context, k IdempotencyKey) (bool, error) {
	return t.q.insertIdempotencyKey(ctx, k)
}
func (t *pgLedgerTx) GetIdempotencyKey(ctx context.Context, clientID, keyHash string) (IdempotencyKey, error) {
	return t.q.getIdempotencyKey(ctx, clientID, keyHash)
}
func (t *pgLedgerTx) CompleteIdempotencyKey(ctx context.Context, clientID, keyHash string, status int, body []byte) (IdempotencyKey, error) {
	return t.q.completeIdempotencyKey(ctx, clientID, keyHash, status, body)
}
