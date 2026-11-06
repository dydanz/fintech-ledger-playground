package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"auth-api/internal/ledger"
	"auth-api/internal/outbox"
)

// ledgerQueries holds the SQL for schema ledger, run against a pool (reads)
// or a transaction (everything in LedgerTx).
type ledgerQueries struct {
	db    dbtx
	topic string
}

// --- accounts ---

const accountColumns = `id, user_id, status, version, created_at, updated_at`

func scanAccount(row pgx.Row) (AccountRecord, error) {
	var a AccountRecord
	err := row.Scan(&a.ID, &a.UserID, &a.Status, &a.Version, &a.CreatedAt, &a.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return AccountRecord{}, ErrNotFound
	}
	return a, err
}

func (q ledgerQueries) insertAccount(ctx context.Context, a AccountRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO ledger.accounts (id, user_id, status, version) VALUES ($1, $2, $3, $4)`,
		a.ID, a.UserID, a.Status, a.Version)
	switch pgCode(err) {
	case "":
		return nil
	case pgUniqueViolation:
		return ErrAccountExists
	case pgForeignKeyViolation:
		return ErrUserNotFound
	}
	return fmt.Errorf("insert account: %w", err)
}

func (q ledgerQueries) getAccount(ctx context.Context, id string) (AccountRecord, error) {
	a, err := scanAccount(q.db.QueryRow(ctx, `SELECT `+accountColumns+` FROM ledger.accounts WHERE id = $1`, id))
	return a, wrapRead("get account", err)
}

func (q ledgerQueries) GetAccountByUserID(ctx context.Context, userID string) (AccountRecord, error) {
	a, err := scanAccount(q.db.QueryRow(ctx, `SELECT `+accountColumns+` FROM ledger.accounts WHERE user_id = $1`, userID))
	return a, wrapRead("get account by user", err)
}

func (q ledgerQueries) lockAccount(ctx context.Context, id string, mode LockMode) (AccountRecord, error) {
	lock := "FOR SHARE"
	if mode == LockUpdate {
		lock = "FOR UPDATE"
	}
	a, err := scanAccount(q.db.QueryRow(ctx, `SELECT `+accountColumns+` FROM ledger.accounts WHERE id = $1 `+lock, id))
	return a, wrapRead("lock account", err)
}

func (q ledgerQueries) updateAccount(ctx context.Context, id string, status ledger.AccountStatus, version int64) error {
	tag, err := q.db.Exec(ctx,
		`UPDATE ledger.accounts SET status = $2, version = $3, updated_at = now() WHERE id = $1`, id, status, version)
	return wrapWrite("update account", tag, err)
}

// --- ledgers ---

const ledgerColumns = `id, kind, account_id, code, currency, overdraft_limit, balance, last_sequence, created_at, updated_at`

func scanLedger(row pgx.Row) (LedgerRecord, error) {
	var l LedgerRecord
	var accountID, code *string
	err := row.Scan(&l.ID, &l.Kind, &accountID, &code, &l.Currency, &l.OverdraftLimit,
		&l.Balance, &l.LastSequence, &l.CreatedAt, &l.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return LedgerRecord{}, ErrNotFound
	}
	l.AccountID, l.Code = deref(accountID), deref(code)
	return l, err
}

func (q ledgerQueries) insertLedger(ctx context.Context, l LedgerRecord) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO ledger.ledgers (id, kind, account_id, code, currency, overdraft_limit)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		l.ID, l.Kind, nullIfEmpty(l.AccountID), nullIfEmpty(l.Code), l.Currency, l.OverdraftLimit)
	switch pgCode(err) {
	case "":
		return nil
	case pgUniqueViolation:
		return ErrLedgerExists
	}
	return fmt.Errorf("insert ledger: %w", err)
}

func (q ledgerQueries) getSystemLedger(ctx context.Context, code, currency string) (LedgerRecord, error) {
	l, err := scanLedger(q.db.QueryRow(ctx, `SELECT `+ledgerColumns+` FROM ledger.ledgers
		WHERE kind = 'SYSTEM' AND code = $1 AND currency = $2`, code, currency))
	return l, wrapRead("get system ledger", err)
}

func (q ledgerQueries) lockUserLedger(ctx context.Context, accountID, currency string) (LedgerRecord, error) {
	l, err := scanLedger(q.db.QueryRow(ctx, `SELECT `+ledgerColumns+` FROM ledger.ledgers
		WHERE kind = 'USER' AND account_id = $1 AND currency = $2 FOR UPDATE`, accountID, currency))
	return l, wrapRead("lock user ledger", err)
}

func (q ledgerQueries) updateLedgerBalance(ctx context.Context, id string, balance, lastSequence int64) error {
	tag, err := q.db.Exec(ctx, `UPDATE ledger.ledgers SET balance = $2, last_sequence = $3, updated_at = now()
		WHERE id = $1 AND kind = 'USER'`, id, balance, lastSequence)
	return wrapWrite("update ledger balance", tag, err)
}

func (q ledgerQueries) listUserLedgers(ctx context.Context, accountID string) ([]LedgerRecord, error) {
	rows, err := q.db.Query(ctx, `SELECT `+ledgerColumns+` FROM ledger.ledgers
		WHERE kind = 'USER' AND account_id = $1 ORDER BY currency`, accountID)
	if err != nil {
		return nil, fmt.Errorf("list ledgers: %w", err)
	}
	defer rows.Close()
	var out []LedgerRecord
	for rows.Next() {
		l, err := scanLedger(rows)
		if err != nil {
			return nil, fmt.Errorf("list ledgers: %w", err)
		}
		out = append(out, l)
	}
	return out, wrapRead("list ledgers", rows.Err())
}

// --- transactions ---

const transactionColumns = `t.id, t.account_id, t.ledger_id, t.counterparty_ledger_id, t.type, t.status, t.amount,
	t.currency, t.original_transaction_id, t.failure_reason, t.reference, t.metadata, t.requested_by,
	t.version, t.trace_id, t.created_at, t.updated_at, t.settled_at`

// withJournal attaches the user journal line. Matching on created_at =
// settled_at is exact (both are Tx2's now()) and prunes to one partition.
const withJournal = ` LEFT JOIN LATERAL (
	SELECT j.ledger_sequence, j.balance_after FROM ledger.journal_entries j
	 WHERE j.transaction_id = t.id AND j.created_at = t.settled_at AND j.ledger_sequence IS NOT NULL
	) j ON true`

func scanTransaction(row pgx.Row, withJournalLine bool) (TransactionRecord, error) {
	var t TransactionRecord
	var ledgerID, original, reason, reference *string
	var seq, after *int64
	dest := []any{&t.ID, &t.AccountID, &ledgerID, &t.CounterpartyLedgerID, &t.Type, &t.Status, &t.Amount,
		&t.Currency, &original, &reason, &reference, &t.Metadata, &t.RequestedBy,
		&t.Version, &t.TraceID, &t.CreatedAt, &t.UpdatedAt, &t.SettledAt}
	if withJournalLine {
		dest = append(dest, &seq, &after)
	}
	if err := row.Scan(dest...); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return TransactionRecord{}, ErrNotFound
		}
		return TransactionRecord{}, err
	}
	t.LedgerID, t.OriginalTransactionID, t.Reference = deref(ledgerID), deref(original), deref(reference)
	t.FailureReason = ledger.FailureReason(deref(reason))
	if seq != nil {
		t.LedgerSequence = *seq
	}
	if after != nil {
		t.BalanceAfter = *after
	}
	return t, nil
}

func (q ledgerQueries) insertTransaction(ctx context.Context, t TransactionRecord) error {
	var metadata any
	if len(t.Metadata) > 0 {
		metadata = []byte(t.Metadata)
	}
	_, err := q.db.Exec(ctx,
		`INSERT INTO ledger.transactions
		 (id, account_id, ledger_id, counterparty_ledger_id, type, status, amount, currency,
		  original_transaction_id, failure_reason, reference, metadata, requested_by, version, trace_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		t.ID, t.AccountID, nullIfEmpty(t.LedgerID), t.CounterpartyLedgerID, t.Type, t.Status, t.Amount, t.Currency,
		nullIfEmpty(t.OriginalTransactionID), nullIfEmpty(string(t.FailureReason)), nullIfEmpty(t.Reference),
		metadata, t.RequestedBy, t.Version, t.TraceID)
	if pgCode(err) == pgUniqueViolation && pgConstraint(err) == "transactions_single_void_key" {
		return ErrVoidExists
	}
	if err != nil {
		return fmt.Errorf("insert transaction: %w", err)
	}
	return nil
}

func (q ledgerQueries) lockTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	t, err := scanTransaction(q.db.QueryRow(ctx,
		`SELECT `+transactionColumns+` FROM ledger.transactions t WHERE t.id = $1 FOR UPDATE`, id), false)
	return t, wrapRead("lock transaction", err)
}

func (q ledgerQueries) updateTransaction(ctx context.Context, u TransactionUpdate) (TransactionRecord, error) {
	t, err := scanTransaction(q.db.QueryRow(ctx,
		`UPDATE ledger.transactions t
		    SET status = $2, version = $3,
		        ledger_id = COALESCE($4, ledger_id),
		        failure_reason = $5,
		        settled_at = CASE WHEN $6 THEN now() ELSE settled_at END,
		        updated_at = now()
		  WHERE id = $1
		RETURNING `+transactionColumns,
		u.ID, u.Status, u.Version, nullIfEmpty(u.LedgerID), nullIfEmpty(string(u.FailureReason)), u.Settled), false)
	return t, wrapRead("update transaction", err)
}

func (q ledgerQueries) getTransaction(ctx context.Context, id string) (TransactionRecord, error) {
	t, err := scanTransaction(q.db.QueryRow(ctx,
		`SELECT `+transactionColumns+`, j.ledger_sequence, j.balance_after
		   FROM ledger.transactions t`+withJournal+` WHERE t.id = $1`, id), true)
	return t, wrapRead("get transaction", err)
}

func (q ledgerQueries) listTransactions(ctx context.Context, tq TransactionQuery) ([]TransactionRecord, error) {
	var afterAt *time.Time
	var afterID any
	if tq.After != nil {
		afterAt, afterID = &tq.After.CreatedAt, tq.After.ID
	}
	rows, err := q.db.Query(ctx,
		`SELECT `+transactionColumns+`, j.ledger_sequence, j.balance_after
		   FROM ledger.transactions t`+withJournal+`
		  WHERE t.account_id = $1
		    AND ($2::text = '' OR t.currency = $2::text)
		    AND ($3::timestamptz IS NULL OR (t.created_at, t.id) < ($3::timestamptz, $4::uuid))
		  ORDER BY t.created_at DESC, t.id DESC
		  LIMIT $5`,
		tq.AccountID, tq.Currency, afterAt, afterID, tq.Limit)
	if err != nil {
		return nil, fmt.Errorf("list transactions: %w", err)
	}
	defer rows.Close()
	var out []TransactionRecord
	for rows.Next() {
		t, err := scanTransaction(rows, true)
		if err != nil {
			return nil, fmt.Errorf("list transactions: %w", err)
		}
		out = append(out, t)
	}
	return out, wrapRead("list transactions", rows.Err())
}

// --- journal ---

func (q ledgerQueries) postJournal(ctx context.Context, user, counterparty JournalLine) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO ledger.journal_entries
		 (id, transaction_id, ledger_id, amount, currency, ledger_sequence, balance_after, trace_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8), ($9, $10, $11, $12, $13, $14, $15, $16)`,
		user.ID, user.TransactionID, user.LedgerID, user.Amount, user.Currency, user.LedgerSequence, user.BalanceAfter, user.TraceID,
		counterparty.ID, counterparty.TransactionID, counterparty.LedgerID, counterparty.Amount, counterparty.Currency,
		counterparty.LedgerSequence, counterparty.BalanceAfter, counterparty.TraceID)
	if err != nil {
		return fmt.Errorf("post journal: %w", err)
	}
	return nil
}

// --- events and outbox ---

func (q ledgerQueries) appendEvent(ctx context.Context, e ledger.Event, ledgerID string) (ledger.Event, error) {
	stream, err := e.Type().Stream()
	if err != nil {
		return e, err
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return e, fmt.Errorf("marshal %s payload: %w", e.Type(), err)
	}
	if e.ID == "" {
		e.ID = ledger.NewID()
	}
	var at *time.Time
	if !e.OccurredAt.IsZero() {
		at = &e.OccurredAt
	}
	if err := q.db.QueryRow(ctx,
		`INSERT INTO ledger.events
		 (event_id, stream_type, stream_id, stream_version, event_type, account_id, schema_version, payload, trace_id, occurred_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, COALESCE($10, now()))
		 RETURNING occurred_at`,
		e.ID, stream, e.StreamID, e.StreamVersion, e.Type(), e.AccountID, ledger.SchemaVersion, payload, e.TraceID, at,
	).Scan(&e.OccurredAt); err != nil {
		return e, fmt.Errorf("append %s event: %w", e.Type(), err)
	}

	msg, err := outbox.Encode(e, ledgerID)
	if err != nil {
		return e, err
	}
	headers, err := json.Marshal(msg.Headers)
	if err != nil {
		return e, fmt.Errorf("marshal outbox headers: %w", err)
	}
	if _, err := q.db.Exec(ctx,
		`INSERT INTO ledger.outbox (event_id, topic, msg_key, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
		msg.EventID, q.topic, msg.Key, msg.Value, headers); err != nil {
		return e, fmt.Errorf("insert outbox: %w", err)
	}
	// Delivered on COMMIT; repeated notifications in one transaction collapse into one.
	if _, err := q.db.Exec(ctx, `SELECT pg_notify($1, '')`, outbox.NotifyChannel); err != nil {
		return e, fmt.Errorf("notify outbox: %w", err)
	}
	return e, nil
}

func (q ledgerQueries) listStreamEvents(ctx context.Context, streamID string) ([]ledger.Event, error) {
	rows, err := q.db.Query(ctx,
		`SELECT event_id, stream_id, stream_version, event_type, account_id, payload, trace_id, occurred_at
		   FROM ledger.events WHERE stream_id = $1 ORDER BY stream_version`, streamID)
	if err != nil {
		return nil, fmt.Errorf("list stream events: %w", err)
	}
	defer rows.Close()
	var out []ledger.Event
	for rows.Next() {
		var e ledger.Event
		var typ string
		var payload []byte
		if err := rows.Scan(&e.ID, &e.StreamID, &e.StreamVersion, &typ, &e.AccountID, &payload, &e.TraceID, &e.OccurredAt); err != nil {
			return nil, fmt.Errorf("list stream events: %w", err)
		}
		if e.Payload, err = ledger.DecodePayload(ledger.EventType(typ), payload); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, wrapRead("list stream events", rows.Err())
}

// --- idempotency ---

func (q ledgerQueries) insertIdempotencyKey(ctx context.Context, k IdempotencyKey) (bool, error) {
	// A concurrent claim of the same key waits on the first one's row lock,
	// then sees it and gets no row back. An expired row is taken over.
	var one int
	err := q.db.QueryRow(ctx,
		`INSERT INTO ledger.idempotency_keys
		 (client_id, key_hash, request_fingerprint, operation, resource_id, expires_at)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 ON CONFLICT (client_id, key_hash) DO UPDATE
		    SET request_fingerprint = EXCLUDED.request_fingerprint, operation = EXCLUDED.operation,
		        resource_id = EXCLUDED.resource_id, response_status = NULL, response_body = NULL,
		        created_at = now(), expires_at = EXCLUDED.expires_at
		  WHERE ledger.idempotency_keys.expires_at <= now()
		 RETURNING 1`,
		k.ClientID, k.KeyHash, k.Fingerprint, k.Operation, k.ResourceID, k.ExpiresAt).Scan(&one)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert idempotency key: %w", err)
	}
	return true, nil
}

func (q ledgerQueries) getIdempotencyKey(ctx context.Context, clientID, keyHash string) (IdempotencyKey, error) {
	var k IdempotencyKey
	var status *int16
	err := q.db.QueryRow(ctx,
		`SELECT client_id, key_hash, request_fingerprint, operation, resource_id, response_status, response_body,
		        created_at, expires_at
		   FROM ledger.idempotency_keys WHERE client_id = $1 AND key_hash = $2`, clientID, keyHash,
	).Scan(&k.ClientID, &k.KeyHash, &k.Fingerprint, &k.Operation, &k.ResourceID, &status, &k.ResponseBody,
		&k.CreatedAt, &k.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return IdempotencyKey{}, ErrNotFound
	}
	if err != nil {
		return IdempotencyKey{}, fmt.Errorf("get idempotency key: %w", err)
	}
	if status != nil {
		k.ResponseStatus = int(*status)
	}
	return k, nil
}

func (q ledgerQueries) completeIdempotencyKey(ctx context.Context, clientID, keyHash string, status int, body []byte) error {
	tag, err := q.db.Exec(ctx,
		`UPDATE ledger.idempotency_keys SET response_status = $3, response_body = $4
		  WHERE client_id = $1 AND key_hash = $2 AND response_status IS NULL`,
		clientID, keyHash, status, body)
	return wrapWrite("complete idempotency key", tag, err)
}

// --- helpers ---

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	if err != nil {
		return "?"
	}
	return ""
}

func pgConstraint(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.ConstraintName
	}
	return ""
}

// wrapRead passes ErrNotFound through unwrapped, like the user repository.
func wrapRead(op string, err error) error {
	if err == nil || errors.Is(err, ErrNotFound) {
		return err
	}
	return fmt.Errorf("%s: %w", op, err)
}

func wrapWrite(op string, tag pgconn.CommandTag, err error) error {
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func nullIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
