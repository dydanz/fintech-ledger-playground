package service

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"fintech-ledger-playground/internal/repository"
)

// User reads (TRD §6.1): the account is always resolved from the session's
// user_id, so a user can never name another user's account or transaction.
// These are plain reads; the access log covers them (logging rule §1).

const (
	DefaultPageLimit = 20
	MaxPageLimit     = 100
)

// TransactionPageQuery is one page request from GET /api/v1/ledger/transactions.
type TransactionPageQuery struct {
	Currency string // "" for all currencies
	Limit    int    // 0 means DefaultPageLimit
	Cursor   string // opaque; "" for the first page
}

type TransactionPage struct {
	Transactions []TransactionView `json:"transactions"`
	NextCursor   string            `json:"next_cursor,omitempty"` // empty on the last page
}

// UserTransactions lists the caller's transactions, newest first, with keyset
// pagination on (created_at, id).
func (s *LedgerService) UserTransactions(ctx context.Context, userID string, q TransactionPageQuery) (TransactionPage, error) {
	if q.Limit == 0 {
		q.Limit = DefaultPageLimit
	}
	if q.Limit < 1 || q.Limit > MaxPageLimit {
		return TransactionPage{}, &ValidationError{Msg: fmt.Sprintf("limit must be from 1 to %d", MaxPageLimit)}
	}
	if q.Currency != "" && !slices.Contains(s.cfg.Currencies, q.Currency) {
		return TransactionPage{}, &ValidationError{Msg: "currency is not supported"}
	}
	var after *repository.TransactionCursor
	if q.Cursor != "" {
		c, err := decodeCursor(q.Cursor)
		if err != nil {
			return TransactionPage{}, &ValidationError{Msg: "cursor is invalid"}
		}
		after = &c
	}

	acct, err := s.userAccountRecord(ctx, userID)
	if err != nil {
		return TransactionPage{}, err
	}
	// One extra row tells us whether another page exists.
	rows, err := s.store.ListTransactions(ctx, repository.TransactionQuery{
		AccountID: acct.ID, Currency: q.Currency, Limit: q.Limit + 1, After: after})
	if err != nil {
		return TransactionPage{}, err
	}
	page := TransactionPage{Transactions: []TransactionView{}}
	if len(rows) > q.Limit {
		rows = rows[:q.Limit]
		last := rows[len(rows)-1]
		page.NextCursor = encodeCursor(repository.TransactionCursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	for _, t := range rows {
		page.Transactions = append(page.Transactions, ToTransactionView(t))
	}
	return page, nil
}

// UserTransaction returns one of the caller's transactions. Someone else's
// transaction is reported as not found, never as forbidden, so ids can't be
// probed.
func (s *LedgerService) UserTransaction(ctx context.Context, userID, transactionID string) (TransactionView, error) {
	if !uuidPattern.MatchString(transactionID) {
		return TransactionView{}, ErrTransactionNotFound
	}
	acct, err := s.userAccountRecord(ctx, userID)
	if errors.Is(err, ErrAccountNotFound) {
		return TransactionView{}, ErrTransactionNotFound
	}
	if err != nil {
		return TransactionView{}, err
	}
	t, err := s.store.GetTransaction(ctx, transactionID)
	if errors.Is(err, repository.ErrNotFound) || err == nil && t.AccountID != acct.ID {
		return TransactionView{}, ErrTransactionNotFound
	}
	if err != nil {
		return TransactionView{}, err
	}
	return ToTransactionView(t), nil
}

func (s *LedgerService) userAccountRecord(ctx context.Context, userID string) (repository.AccountRecord, error) {
	if !uuidPattern.MatchString(userID) {
		return repository.AccountRecord{}, ErrAccountNotFound
	}
	a, err := s.store.GetAccountByUserID(ctx, userID)
	if errors.Is(err, repository.ErrNotFound) {
		return repository.AccountRecord{}, ErrAccountNotFound
	}
	return a, err
}

// The cursor is base64url("<created_at RFC 3339, µs>|<id>"). It is opaque to
// clients; only its round trip matters.
func encodeCursor(c repository.TransactionCursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.ID))
}

func decodeCursor(v string) (repository.TransactionCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return repository.TransactionCursor{}, err
	}
	at, id, ok := strings.Cut(string(raw), "|")
	if !ok || !uuidPattern.MatchString(id) {
		return repository.TransactionCursor{}, errors.New("malformed cursor")
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return repository.TransactionCursor{}, err
	}
	return repository.TransactionCursor{CreatedAt: t, ID: id}, nil
}
