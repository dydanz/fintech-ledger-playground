package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/repository"
)

// OpsReader serves the read-only ops routes (spec/2026-11-10 Web Console
// Design D§7.2): listings across accounts, a transaction's full detail, an
// account's events, and the counts behind the console's tiles. These are
// plain reads with no events of their own; the access log covers them
// (logging rule §1). Nothing it returns carries caller or operator free text.
type OpsReader struct {
	store OpsStore
	now   func() time.Time
}

// OpsStore is what OpsReader needs from the ledger repository.
type OpsStore interface {
	GetAccount(ctx context.Context, id string) (repository.AccountRecord, error)
	GetTransaction(ctx context.Context, id string) (repository.TransactionRecord, error)
	ListAccounts(ctx context.Context, afterID string, limit int) ([]repository.AccountRecord, error)
	ListLedgersOfAccounts(ctx context.Context, accountIDs []string) ([]repository.LedgerRecord, error)
	ListTransactionsByChange(ctx context.Context, q repository.OpsTransactionQuery) ([]repository.TransactionRecord, error)
	ListEventRows(ctx context.Context, streamID string) ([]repository.EventRow, error)
	ListJournalOfTransaction(ctx context.Context, transactionID string) ([]repository.OpsJournalLine, error)
	ListOutboxPublications(ctx context.Context, eventIDs []string) ([]repository.OutboxPublication, error)
	Stats(ctx context.Context, since time.Time) (repository.OpsStats, error)
}

func NewOpsReader(store OpsStore) *OpsReader { return &OpsReader{store: store, now: time.Now} }

// DefaultStatsWindow is the stats window when no since is given.
const DefaultStatsWindow = 10 * time.Minute

// OpsTransactionView is a transaction as the ops routes show it: the
// TransactionView fields minus reference, plus who requested it and when it
// last changed.
type OpsTransactionView struct {
	ID             string     `json:"id"`
	Type           string     `json:"type"`
	Status         string     `json:"status"`
	AccountID      string     `json:"account_id"`
	LedgerID       string     `json:"ledger_id,omitempty"`
	Amount         int64      `json:"amount"`
	Currency       string     `json:"currency"`
	BalanceAfter   *int64     `json:"balance_after,omitempty"`
	LedgerSequence *int64     `json:"ledger_sequence,omitempty"`
	FailureReason  string     `json:"failure_reason,omitempty"`
	OriginalID     string     `json:"original_transaction_id,omitempty"`
	RequestedBy    string     `json:"requested_by"`
	TraceID        string     `json:"trace_id"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	SettledAt      *time.Time `json:"settled_at,omitempty"`
}

func toOpsTransactionView(t repository.TransactionRecord) OpsTransactionView {
	v := ToTransactionView(t)
	return OpsTransactionView{ID: v.ID, Type: v.Type, Status: v.Status, AccountID: v.AccountID, LedgerID: v.LedgerID,
		Amount: v.Amount, Currency: v.Currency, BalanceAfter: v.BalanceAfter, LedgerSequence: v.LedgerSequence,
		FailureReason: v.FailureReason, OriginalID: v.OriginalID, RequestedBy: t.RequestedBy, TraceID: v.TraceID,
		CreatedAt: v.CreatedAt, UpdatedAt: t.UpdatedAt.UTC(), SettledAt: v.SettledAt}
}

type EventView struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Version    int64           `json:"version"`
	Payload    json.RawMessage `json:"payload"`
	TraceID    string          `json:"trace_id"`
	OccurredAt time.Time       `json:"occurred_at"`
}

type JournalLineView struct {
	ID             string    `json:"id"`
	LedgerID       string    `json:"ledger_id"`
	LedgerKind     string    `json:"ledger_kind"`
	LedgerCode     string    `json:"ledger_code,omitempty"`
	AccountID      string    `json:"account_id,omitempty"`
	Amount         int64     `json:"amount"`
	Currency       string    `json:"currency"`
	LedgerSequence *int64    `json:"ledger_sequence,omitempty"`
	BalanceAfter   *int64    `json:"balance_after,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type OutboxView struct {
	EventID     string     `json:"event_id"`
	CreatedAt   time.Time  `json:"created_at"`
	PublishedAt *time.Time `json:"published_at"` // null until the relay publishes it
}

type TransactionDetail struct {
	Transaction OpsTransactionView `json:"transaction"`
	Events      []EventView        `json:"events"`
	Journal     []JournalLineView  `json:"journal"` // empty unless settled
	Outbox      []OutboxView       `json:"outbox"`  // empty once purged (OUTBOX_RETENTION)
}

type AccountPage struct {
	Accounts   []AccountView `json:"accounts"`
	NextCursor string        `json:"next_cursor,omitempty"`
}

type OpsTransactionPage struct {
	Transactions []OpsTransactionView `json:"transactions"`
	NextCursor   string               `json:"next_cursor,omitempty"`
}

type CountView struct {
	Type          string `json:"type"`
	Status        string `json:"status"`
	FailureReason string `json:"failure_reason"`
	Count         int64  `json:"count"`
}

// PendingView is the PENDING transactions (PROCESSING is never committed:
// Tx2 moves through it inside one database transaction).
type PendingView struct {
	Count       int64 `json:"count"`
	OldestAgeMs int64 `json:"oldest_age_ms"` // 0 when none
}

// OutboxBacklogView is the events not yet published to Kafka.
type OutboxBacklogView struct {
	Pending     int64 `json:"pending"`
	OldestAgeMs int64 `json:"oldest_age_ms"` // 0 when none
}

type StatsView struct {
	Since        time.Time         `json:"since"`
	Transactions []CountView       `json:"transactions"`
	Pending      PendingView       `json:"pending"`
	Outbox       OutboxBacklogView `json:"outbox"`
}

// OpsTransactionsQuery is a request for GET /internal/v1/transactions.
type OpsTransactionsQuery struct {
	Limit  int    // 0 means DefaultPageLimit
	Cursor string // opaque; "" for the first page
	Status string // "" or a transaction status
	Type   string // "" or a transaction type
	Since  string // "" or RFC 3339
}

var (
	opsStatuses = map[string]bool{string(ledger.StatusPending): true, string(ledger.StatusSettled): true,
		string(ledger.StatusFailed): true, string(ledger.StatusVoided): true}
	opsTypes = map[string]bool{string(ledger.Credit): true, string(ledger.Debit): true, string(ledger.Void): true}
)

func pageLimit(n int) (int, error) {
	if n == 0 {
		return DefaultPageLimit, nil
	}
	if n < 1 || n > MaxPageLimit {
		return 0, &ValidationError{Msg: fmt.Sprintf("limit must be from 1 to %d", MaxPageLimit)}
	}
	return n, nil
}

// Transactions lists transactions across accounts, newest change first.
func (r *OpsReader) Transactions(ctx context.Context, q OpsTransactionsQuery) (OpsTransactionPage, error) {
	limit, err := pageLimit(q.Limit)
	if err != nil {
		return OpsTransactionPage{}, err
	}
	rq := repository.OpsTransactionQuery{Limit: limit + 1}
	if q.Status != "" {
		if !opsStatuses[q.Status] {
			return OpsTransactionPage{}, &ValidationError{Msg: "status must be PENDING, SETTLED, FAILED or VOIDED"}
		}
		rq.Status = ledger.Status(q.Status)
	}
	if q.Type != "" {
		if !opsTypes[q.Type] {
			return OpsTransactionPage{}, &ValidationError{Msg: "type must be CREDIT, DEBIT or VOID"}
		}
		rq.Type = ledger.TransactionType(q.Type)
	}
	if q.Since != "" {
		t, err := time.Parse(time.RFC3339, q.Since)
		if err != nil {
			return OpsTransactionPage{}, &ValidationError{Msg: "since must be an RFC 3339 time"}
		}
		rq.Since = &t
	}
	if q.Cursor != "" {
		c, err := decodeChangeCursor(q.Cursor)
		if err != nil {
			return OpsTransactionPage{}, &ValidationError{Msg: "cursor is invalid"}
		}
		rq.After = &c
	}
	rows, err := r.store.ListTransactionsByChange(ctx, rq)
	if err != nil {
		return OpsTransactionPage{}, err
	}
	page := OpsTransactionPage{Transactions: []OpsTransactionView{}}
	if len(rows) > limit { // the extra row says another page exists
		rows = rows[:limit]
		last := rows[len(rows)-1]
		page.NextCursor = encodeChangeCursor(repository.ChangeCursor{UpdatedAt: last.UpdatedAt, ID: last.ID})
	}
	for _, t := range rows {
		page.Transactions = append(page.Transactions, toOpsTransactionView(t))
	}
	return page, nil
}

// Accounts lists accounts with their ledgers, in creation order.
func (r *OpsReader) Accounts(ctx context.Context, limit int, cursor string) (AccountPage, error) {
	limit, err := pageLimit(limit)
	if err != nil {
		return AccountPage{}, err
	}
	var after string
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil || !uuidPattern.MatchString(string(raw)) {
			return AccountPage{}, &ValidationError{Msg: "cursor is invalid"}
		}
		after = string(raw)
	}
	rows, err := r.store.ListAccounts(ctx, after, limit+1)
	if err != nil {
		return AccountPage{}, err
	}
	page := AccountPage{Accounts: []AccountView{}}
	if len(rows) > limit {
		rows = rows[:limit]
		page.NextCursor = base64.RawURLEncoding.EncodeToString([]byte(rows[len(rows)-1].ID))
	}
	ids := make([]string, len(rows))
	for i, a := range rows {
		ids[i] = a.ID
	}
	ledgers, err := r.store.ListLedgersOfAccounts(ctx, ids)
	if err != nil {
		return AccountPage{}, err
	}
	byAccount := map[string][]repository.LedgerRecord{}
	for _, l := range ledgers {
		byAccount[l.AccountID] = append(byAccount[l.AccountID], l)
	}
	for _, a := range rows {
		page.Accounts = append(page.Accounts, ToAccountView(a, byAccount[a.ID]))
	}
	return page, nil
}

// TransactionDetail returns a transaction with its events, journal lines and
// outbox rows.
func (r *OpsReader) TransactionDetail(ctx context.Context, id string) (TransactionDetail, error) {
	if !uuidPattern.MatchString(id) {
		return TransactionDetail{}, ErrTransactionNotFound
	}
	t, err := r.store.GetTransaction(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return TransactionDetail{}, ErrTransactionNotFound
	}
	if err != nil {
		return TransactionDetail{}, err
	}
	events, err := r.events(ctx, id)
	if err != nil {
		return TransactionDetail{}, err
	}
	lines, err := r.store.ListJournalOfTransaction(ctx, id)
	if err != nil {
		return TransactionDetail{}, err
	}
	eventIDs := make([]string, len(events))
	for i, e := range events {
		eventIDs[i] = e.ID
	}
	pubs, err := r.store.ListOutboxPublications(ctx, eventIDs)
	if err != nil {
		return TransactionDetail{}, err
	}
	d := TransactionDetail{Transaction: toOpsTransactionView(t), Events: events, Journal: []JournalLineView{}, Outbox: []OutboxView{}}
	for _, j := range lines {
		d.Journal = append(d.Journal, JournalLineView{ID: j.ID, LedgerID: j.LedgerID, LedgerKind: string(j.LedgerKind), LedgerCode: j.LedgerCode,
			AccountID: j.AccountID, Amount: j.Amount, Currency: j.Currency, LedgerSequence: j.LedgerSequence, BalanceAfter: j.BalanceAfter,
			CreatedAt: j.CreatedAt.UTC()})
	}
	for _, o := range pubs {
		v := OutboxView{EventID: o.EventID, CreatedAt: o.CreatedAt.UTC()}
		if o.PublishedAt != nil {
			at := o.PublishedAt.UTC()
			v.PublishedAt = &at
		}
		d.Outbox = append(d.Outbox, v)
	}
	return d, nil
}

// AccountEvents returns an account's event stream (opened, ledger opened,
// frozen, unfrozen), without the free-text reasons.
func (r *OpsReader) AccountEvents(ctx context.Context, accountID string) ([]EventView, error) {
	if !uuidPattern.MatchString(accountID) {
		return nil, ErrAccountNotFound
	}
	if _, err := r.store.GetAccount(ctx, accountID); errors.Is(err, repository.ErrNotFound) {
		return nil, ErrAccountNotFound
	} else if err != nil {
		return nil, err
	}
	return r.events(ctx, accountID)
}

func (r *OpsReader) events(ctx context.Context, streamID string) ([]EventView, error) {
	rows, err := r.store.ListEventRows(ctx, streamID)
	if err != nil {
		return nil, err
	}
	out := []EventView{}
	for _, e := range rows {
		out = append(out, EventView{ID: e.ID, Type: string(e.Type), Version: e.Version, Payload: e.Payload,
			TraceID: e.TraceID, OccurredAt: e.OccurredAt.UTC()})
	}
	return out, nil
}

// Stats returns the counts for transactions changed since the window's start
// ("" means the last DefaultStatsWindow), and the current backlogs.
func (r *OpsReader) Stats(ctx context.Context, since string) (StatsView, error) {
	now := r.now()
	from := now.Add(-DefaultStatsWindow)
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return StatsView{}, &ValidationError{Msg: "since must be an RFC 3339 time"}
		}
		from = t
	}
	s, err := r.store.Stats(ctx, from)
	if err != nil {
		return StatsView{}, err
	}
	v := StatsView{Since: from.UTC(), Transactions: []CountView{},
		Pending: PendingView{Count: s.Pending.Count, OldestAgeMs: ageMs(now, s.Pending.Oldest)},
		Outbox:  OutboxBacklogView{Pending: s.Outbox.Count, OldestAgeMs: ageMs(now, s.Outbox.Oldest)}}
	for _, c := range s.Transactions {
		v.Transactions = append(v.Transactions, CountView{Type: string(c.Type), Status: string(c.Status),
			FailureReason: string(c.FailureReason), Count: c.Count})
	}
	return v, nil
}

func ageMs(now time.Time, oldest *time.Time) int64 {
	if oldest == nil {
		return 0
	}
	return max(now.Sub(*oldest).Milliseconds(), 0)
}

// The change cursor is base64url("<updated_at RFC 3339, ns>|<id>"), opaque to
// clients like the user cursor.
func encodeChangeCursor(c repository.ChangeCursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(c.UpdatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID))
}

func decodeChangeCursor(v string) (repository.ChangeCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return repository.ChangeCursor{}, err
	}
	at, id, ok := strings.Cut(string(raw), "|")
	if !ok || !uuidPattern.MatchString(id) {
		return repository.ChangeCursor{}, errors.New("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return repository.ChangeCursor{}, err
	}
	return repository.ChangeCursor{UpdatedAt: t, ID: id}, nil
}
