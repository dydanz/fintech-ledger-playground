// Package repository is money-out-middleware's Postgres access (spec/money-out
// TRD §5): transfers, attempts, the outbox and consumer deduplication. Like
// the core's repositories it never logs; it returns wrapped errors.
package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

var (
	ErrNotFound        = errors.New("money-out: transfer not found")
	ErrVersionConflict = errors.New("money-out: transfer changed concurrently")
	ErrStreamVersion   = errors.New("money-out: journal event does not follow the transfer's version")
	ErrAttemptOpen     = errors.New("money-out: the transfer already has an open attempt")
)

const pgUniqueViolation = "23505"

// Transfer is a row of transfers.
type Transfer struct {
	ID                string
	PayloadHash       string
	Amount            int64
	Currency          string
	BankCode          string
	AccountNo         string
	Metadata          json.RawMessage
	CoreTransactionID string
	State             transfer.State
	Version           int64
	Routing           json.RawMessage
	CurrentAttempt    int
	CoreAck           string
	TraceID           string
	RequestedAt       time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Attempt is a row of attempts.
type Attempt struct {
	TransferID     string
	AttemptNo      int
	RequestID      string
	BankMiddleware string
	Rail           string
	Status         string
	Reason         string
	RetryElsewhere bool
	ReachedBank    bool
	BankReference  string
	BankRawCode    string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// Notifier wakes the outbox relay after a commit that wrote outbox rows
// (outbox.Notifier); nil leaves the relay to its poll.
type Notifier interface{ Notify() }

type Store struct {
	db       *pgxpool.Pool
	notifier Notifier
}

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

// WithNotifier sets the relay notifier.
func (s *Store) WithNotifier(n Notifier) *Store { s.notifier = n; return s }

// DB is the pool, for the outbox relay's repository and for tests.
func (s *Store) DB() *pgxpool.Pool { return s.db }

// InTx runs fn in one transaction and commits if fn returns nil. After a
// commit that wrote outbox rows it wakes the relay, never from inside the
// transaction (TRD L§7.3).
func (s *Store) InTx(ctx context.Context, fn func(*Tx) error) error {
	ptx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer ptx.Rollback(context.WithoutCancel(ctx)) // no-op after Commit
	tx := &Tx{tx: ptx}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ptx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	if tx.wrote && s.notifier != nil {
		s.notifier.Notify()
	}
	return nil
}

const transferColumns = `transfer_id, payload_hash, amount, currency, bank_code, account_no, metadata,
	coalesce(core_transaction_id, ''), state, version, routing, current_attempt, core_ack, trace_id,
	requested_at, created_at, updated_at`

func scanTransfer(row pgx.Row) (Transfer, error) {
	var t Transfer
	var state string
	err := row.Scan(&t.ID, &t.PayloadHash, &t.Amount, &t.Currency, &t.BankCode, &t.AccountNo, &t.Metadata,
		&t.CoreTransactionID, &state, &t.Version, &t.Routing, &t.CurrentAttempt, &t.CoreAck, &t.TraceID,
		&t.RequestedAt, &t.CreatedAt, &t.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transfer{}, ErrNotFound
	}
	t.State = transfer.State(state)
	return t, err
}

// GetTransfer reads a transfer and its attempts, outside any transaction.
func (s *Store) GetTransfer(ctx context.Context, id string) (Transfer, []Attempt, error) {
	t, err := scanTransfer(s.db.QueryRow(ctx, `SELECT `+transferColumns+` FROM transfers WHERE transfer_id = $1`, id))
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return Transfer{}, nil, err
		}
		return Transfer{}, nil, fmt.Errorf("get transfer: %w", err)
	}
	rows, err := s.db.Query(ctx, `SELECT transfer_id, attempt_no, request_id, bank_middleware, rail, status,
		coalesce(reason, ''), retry_elsewhere, reached_bank, coalesce(bank_reference, ''), coalesce(bank_raw_code, ''),
		created_at, updated_at FROM attempts WHERE transfer_id = $1 ORDER BY attempt_no`, id)
	if err != nil {
		return Transfer{}, nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()
	var out []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.TransferID, &a.AttemptNo, &a.RequestID, &a.BankMiddleware, &a.Rail, &a.Status, &a.Reason,
			&a.RetryElsewhere, &a.ReachedBank, &a.BankReference, &a.BankRawCode, &a.CreatedAt, &a.UpdatedAt); err != nil {
			return Transfer{}, nil, fmt.Errorf("list attempts: %w", err)
		}
		out = append(out, a)
	}
	return t, out, rows.Err()
}

// Tx is one transaction's view of the store.
type Tx struct {
	tx    pgx.Tx
	wrote bool
}

// InsertTransfer inserts t at version 1 with its TransferReceived journal
// event. created is false, and nothing is written, when the transfer_id
// already exists: the idempotency gate.
func (x *Tx) InsertTransfer(ctx context.Context, t Transfer, received contract.Envelope) (created bool, err error) {
	if received.StreamVersion != 1 {
		return false, ErrStreamVersion
	}
	tag, err := x.tx.Exec(ctx, `INSERT INTO transfers (transfer_id, payload_hash, amount, currency, bank_code, account_no,
		metadata, core_transaction_id, state, trace_id, requested_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, nullif($8, ''), $9, $10, $11) ON CONFLICT (transfer_id) DO NOTHING`,
		t.ID, t.PayloadHash, t.Amount, t.Currency, t.BankCode, t.AccountNo, nullJSON(t.Metadata), t.CoreTransactionID,
		string(t.State), t.TraceID, t.RequestedAt)
	if err != nil {
		return false, fmt.Errorf("insert transfer: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return false, nil
	}
	return true, x.Enqueue(ctx, contract.TopicJournal, received)
}

// LockTransfer reads a transfer FOR UPDATE.
func (x *Tx) LockTransfer(ctx context.Context, id string) (Transfer, error) {
	t, err := scanTransfer(x.tx.QueryRow(ctx, `SELECT `+transferColumns+` FROM transfers WHERE transfer_id = $1 FOR UPDATE`, id))
	if err != nil && !errors.Is(err, ErrNotFound) {
		return Transfer{}, fmt.Errorf("lock transfer: %w", err)
	}
	return t, err
}

// Transition moves the transfer from state `from` at `version` to `to`, as a
// compare-and-set, and journals ev (whose stream_version must be version+1)
// in the same transaction. It returns the new version.
func (x *Tx) Transition(ctx context.Context, id string, from transfer.State, version int64, to transfer.State, ev contract.Envelope) (int64, error) {
	if err := transfer.Apply(from, to); err != nil {
		return 0, err
	}
	if ev.StreamVersion != version+1 || ev.StreamID != id {
		return 0, ErrStreamVersion
	}
	tag, err := x.tx.Exec(ctx, `UPDATE transfers SET state = $4, version = version + 1, updated_at = now()
		WHERE transfer_id = $1 AND state = $2 AND version = $3`, id, string(from), version, string(to))
	if err != nil {
		return 0, fmt.Errorf("transition: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return 0, ErrVersionConflict
	}
	return version + 1, x.Enqueue(ctx, contract.TopicJournal, ev)
}

// Record journals ev without a state change (a rejected duplicate, a bank
// event): the version still moves on, so the stream stays gapless.
func (x *Tx) Record(ctx context.Context, id string, version int64, ev contract.Envelope) (int64, error) {
	if ev.StreamVersion != version+1 || ev.StreamID != id {
		return 0, ErrStreamVersion
	}
	tag, err := x.tx.Exec(ctx, `UPDATE transfers SET version = version + 1, updated_at = now()
		WHERE transfer_id = $1 AND version = $2`, id, version)
	if err != nil {
		return 0, fmt.Errorf("record: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return 0, ErrVersionConflict
	}
	return version + 1, x.Enqueue(ctx, contract.TopicJournal, ev)
}

// InsertAttempt adds an attempt. A second open attempt for the transfer is
// ErrAttemptOpen (the partial unique index, P§3.2).
func (x *Tx) InsertAttempt(ctx context.Context, a Attempt) error {
	_, err := x.tx.Exec(ctx, `INSERT INTO attempts (transfer_id, attempt_no, request_id, bank_middleware, rail, status)
		VALUES ($1, $2, $3, $4, $5, $6)`, a.TransferID, a.AttemptNo, a.RequestID, a.BankMiddleware, a.Rail, a.Status)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation && pgErr.ConstraintName == "attempts_one_open" {
		return ErrAttemptOpen
	}
	if err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}
	return nil
}

// Enqueue writes envelopes to the outbox for topic.
func (x *Tx) Enqueue(ctx context.Context, topic string, events ...contract.Envelope) error {
	for _, ev := range events {
		msg, err := contract.Encode(ev)
		if err != nil {
			return err
		}
		headers, err := json.Marshal(msg.Headers)
		if err != nil {
			return err
		}
		if _, err := x.tx.Exec(ctx, `INSERT INTO outbox (event_id, topic, msg_key, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
			msg.EventID, topic, msg.Key, msg.Value, headers); err != nil {
			return fmt.Errorf("enqueue %s: %w", ev.EventType, err)
		}
		x.wrote = true
	}
	return nil
}

// MarkProcessed records that a consumed event was handled. first is false
// when it already was: the caller skips it.
func (x *Tx) MarkProcessed(ctx context.Context, eventID, topic string) (first bool, err error) {
	tag, err := x.tx.Exec(ctx, `INSERT INTO processed_events (event_id, topic) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`, eventID, topic)
	if err != nil {
		return false, fmt.Errorf("mark processed: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func nullJSON(b json.RawMessage) any {
	if len(b) == 0 {
		return nil
	}
	return b
}
