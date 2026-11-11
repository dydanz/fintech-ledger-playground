package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/ledger"
)

// Read-only queries for the ops routes (spec/2026-11-10 Web Console Design
// D§7.2). They read committed state across accounts, never inside a ledger
// transaction.

// OpsTransactionQuery selects one page of transactions across accounts,
// newest change first. After, when set, is the (updated_at, id) of the last
// row seen. Status, Type and Since are optional filters.
type OpsTransactionQuery struct {
	Limit  int
	After  *ChangeCursor
	Status ledger.Status
	Type   ledger.TransactionType
	Since  *time.Time
}

// ChangeCursor is a position in the by-last-change order.
type ChangeCursor struct {
	UpdatedAt time.Time
	ID        string
}

// EventRow is a stored event with its payload as JSON, minus caller and
// operator free text (see listEventRows).
type EventRow struct {
	ID         string
	Type       ledger.EventType
	Version    int64
	Payload    json.RawMessage
	TraceID    string
	OccurredAt time.Time
}

// OpsJournalLine is a journal line with the kind of ledger it posts to.
type OpsJournalLine struct {
	ID             string
	LedgerID       string
	LedgerKind     LedgerKind
	LedgerCode     string // SYSTEM ledgers, e.g. topup_clearing
	AccountID      string // USER ledgers
	Amount         int64  // signed; the lines of a transaction sum to 0
	Currency       string
	LedgerSequence *int64
	BalanceAfter   *int64
	CreatedAt      time.Time
}

// OutboxPublication is whether one event has reached Kafka. Rows are purged
// OUTBOX_RETENTION after publishing, so a missing event isn't an error.
type OutboxPublication struct {
	EventID     string
	CreatedAt   time.Time
	PublishedAt *time.Time
}

// TransactionCount is one (type, status, failure_reason) group.
type TransactionCount struct {
	Type          ledger.TransactionType
	Status        ledger.Status
	FailureReason ledger.FailureReason
	Count         int64
}

// Backlog is how many rows wait, and since when (nil when none).
type Backlog struct {
	Count  int64
	Oldest *time.Time
}

// OpsStats is the state the console's tiles summarize.
type OpsStats struct {
	Transactions []TransactionCount // changed since the window's start
	Pending      Backlog            // PENDING transactions (PROCESSING is never committed)
	Outbox       Backlog            // unpublished outbox rows
}

// ListAccounts returns up to limit accounts after the given id, in id order
// (UUIDv7, so creation order). afterID "" starts at the beginning.
func (r *LedgerRepo) ListAccounts(ctx context.Context, afterID string, limit int) ([]AccountRecord, error) {
	rows, err := r.db.Query(ctx, `SELECT `+accountColumns+` FROM ledger.accounts
		WHERE ($1::text = '' OR id > $1::uuid) ORDER BY id LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, fmt.Errorf("list accounts: %w", err)
	}
	defer rows.Close()
	var out []AccountRecord
	for rows.Next() {
		a, err := scanAccount(rows)
		if err != nil {
			return nil, fmt.Errorf("list accounts: %w", err)
		}
		out = append(out, a)
	}
	return out, wrapRead("list accounts", rows.Err())
}

// ListLedgersOfAccounts returns the USER ledgers of the given accounts, in one
// query, by account and currency.
func (r *LedgerRepo) ListLedgersOfAccounts(ctx context.Context, accountIDs []string) ([]LedgerRecord, error) {
	if len(accountIDs) == 0 {
		return nil, nil
	}
	rows, err := r.db.Query(ctx, `SELECT `+ledgerColumns+` FROM ledger.ledgers
		WHERE kind = 'USER' AND account_id = ANY($1::uuid[]) ORDER BY account_id, currency`, accountIDs)
	if err != nil {
		return nil, fmt.Errorf("list ledgers of accounts: %w", err)
	}
	defer rows.Close()
	var out []LedgerRecord
	for rows.Next() {
		l, err := scanLedger(rows)
		if err != nil {
			return nil, fmt.Errorf("list ledgers of accounts: %w", err)
		}
		out = append(out, l)
	}
	return out, wrapRead("list ledgers of accounts", rows.Err())
}

// ListTransactionsByChange returns one page across accounts, newest change
// first, on transactions_updated_idx. A row that changes moves to the top,
// so a page read after it changed doesn't repeat it.
func (r *LedgerRepo) ListTransactionsByChange(ctx context.Context, q OpsTransactionQuery) ([]TransactionRecord, error) {
	var afterAt *time.Time
	var afterID any
	if q.After != nil {
		afterAt, afterID = &q.After.UpdatedAt, q.After.ID
	}
	rows, err := r.db.Query(ctx,
		`SELECT `+transactionColumns+`, j.ledger_sequence, j.balance_after
		   FROM ledger.transactions t`+withJournal+`
		  WHERE ($1::timestamptz IS NULL OR (t.updated_at, t.id) < ($1::timestamptz, $2::uuid))
		    AND ($3::text = '' OR t.status = $3::text)
		    AND ($4::text = '' OR t.type = $4::text)
		    AND ($5::timestamptz IS NULL OR t.updated_at >= $5::timestamptz)
		  ORDER BY t.updated_at DESC, t.id DESC
		  LIMIT $6`,
		afterAt, afterID, string(q.Status), string(q.Type), q.Since, q.Limit)
	if err != nil {
		return nil, fmt.Errorf("list transactions by change: %w", err)
	}
	defer rows.Close()
	var out []TransactionRecord
	for rows.Next() {
		t, err := scanTransaction(rows, true)
		if err != nil {
			return nil, fmt.Errorf("list transactions by change: %w", err)
		}
		out = append(out, t)
	}
	return out, wrapRead("list transactions by change", rows.Err())
}

// ListEventRows returns a stream's events in version order, with the payload
// as stored minus free text: reference and metadata (caller-supplied), and
// the reason of a freeze or unfreeze (operator-typed). A failure's reason is
// a fixed code and is kept.
func (r *LedgerRepo) ListEventRows(ctx context.Context, streamID string) ([]EventRow, error) {
	rows, err := r.db.Query(ctx,
		`SELECT event_id, event_type, stream_version,
		        CASE WHEN event_type IN ('AccountFrozen', 'AccountUnfrozen') THEN payload - 'reason' ELSE payload END
		          - 'reference' - 'metadata',
		        trace_id, occurred_at
		   FROM ledger.events WHERE stream_id = $1 ORDER BY stream_version`, streamID)
	if err != nil {
		return nil, fmt.Errorf("list event rows: %w", err)
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var e EventRow
		var typ string
		if err := rows.Scan(&e.ID, &typ, &e.Version, &e.Payload, &e.TraceID, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("list event rows: %w", err)
		}
		e.Type = ledger.EventType(typ)
		out = append(out, e)
	}
	return out, wrapRead("list event rows", rows.Err())
}

// ListJournalOfTransaction returns a transaction's journal lines, user line
// first.
func (r *LedgerRepo) ListJournalOfTransaction(ctx context.Context, transactionID string) ([]OpsJournalLine, error) {
	rows, err := r.db.Query(ctx,
		`SELECT j.id, j.ledger_id, l.kind, coalesce(l.code, ''), coalesce(l.account_id::text, ''),
		        j.amount, j.currency, j.ledger_sequence, j.balance_after, j.created_at
		   FROM ledger.journal_entries j JOIN ledger.ledgers l ON l.id = j.ledger_id
		  WHERE j.transaction_id = $1
		  ORDER BY j.created_at, (l.kind = 'SYSTEM'), j.id`, transactionID)
	if err != nil {
		return nil, fmt.Errorf("list journal of transaction: %w", err)
	}
	defer rows.Close()
	var out []OpsJournalLine
	for rows.Next() {
		var j OpsJournalLine
		if err := rows.Scan(&j.ID, &j.LedgerID, &j.LedgerKind, &j.LedgerCode, &j.AccountID,
			&j.Amount, &j.Currency, &j.LedgerSequence, &j.BalanceAfter, &j.CreatedAt); err != nil {
			return nil, fmt.Errorf("list journal of transaction: %w", err)
		}
		out = append(out, j)
	}
	return out, wrapRead("list journal of transaction", rows.Err())
}

// ListOutboxPublications returns the outbox rows of the given events.
func (r *LedgerRepo) ListOutboxPublications(ctx context.Context, eventIDs []string) ([]OutboxPublication, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	rows, err := r.db.Query(ctx,
		`SELECT event_id, created_at, published_at FROM ledger.outbox WHERE event_id = ANY($1::uuid[]) ORDER BY id`, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("list outbox publications: %w", err)
	}
	defer rows.Close()
	var out []OutboxPublication
	for rows.Next() {
		var o OutboxPublication
		if err := rows.Scan(&o.EventID, &o.CreatedAt, &o.PublishedAt); err != nil {
			return nil, fmt.Errorf("list outbox publications: %w", err)
		}
		out = append(out, o)
	}
	return out, wrapRead("list outbox publications", rows.Err())
}

// Stats counts the transactions changed since the window's start (on
// transactions_updated_idx), and the PENDING and outbox backlogs (on their
// partial indexes).
func (r *LedgerRepo) Stats(ctx context.Context, since time.Time) (OpsStats, error) {
	var s OpsStats
	rows, err := r.db.Query(ctx,
		`SELECT type, status, coalesce(failure_reason, ''), count(*) FROM ledger.transactions
		  WHERE updated_at >= $1 GROUP BY 1, 2, 3 ORDER BY 1, 2, 3`, since)
	if err != nil {
		return OpsStats{}, fmt.Errorf("stats: %w", err)
	}
	for rows.Next() {
		var c TransactionCount
		if err := rows.Scan(&c.Type, &c.Status, &c.FailureReason, &c.Count); err != nil {
			rows.Close()
			return OpsStats{}, fmt.Errorf("stats: %w", err)
		}
		s.Transactions = append(s.Transactions, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return OpsStats{}, wrapRead("stats", err)
	}
	if err := r.db.QueryRow(ctx,
		`SELECT count(*), min(created_at) FROM ledger.transactions WHERE status = 'PENDING'`).Scan(&s.Pending.Count, &s.Pending.Oldest); err != nil {
		return OpsStats{}, wrapRead("stats pending", err)
	}
	if err := r.db.QueryRow(ctx,
		`SELECT count(*), min(created_at) FROM ledger.outbox WHERE published_at IS NULL`).Scan(&s.Outbox.Count, &s.Outbox.Oldest); err != nil {
		return OpsStats{}, wrapRead("stats outbox", err)
	}
	return s, nil
}
