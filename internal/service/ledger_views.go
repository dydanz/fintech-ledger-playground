package service

import (
	"encoding/json"
	"net/http"
	"time"

	"auth-api/internal/ledger"
	"auth-api/internal/repository"
)

// Response is a finished HTTP response for a mutating ledger request. It is
// stored with the idempotency key and replayed byte for byte (TRD §6.4).
type Response struct {
	Status   int
	Body     []byte
	Replayed bool // true when served from a stored idempotent response
	cached   bool // the replay came from the Redis cache
}

type TransactionView struct {
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
	Reference      string     `json:"reference,omitempty"`
	TraceID        string     `json:"trace_id"`
	CreatedAt      time.Time  `json:"created_at"`
	SettledAt      *time.Time `json:"settled_at,omitempty"`
}

type LedgerView struct {
	ID             string    `json:"id"`
	Currency       string    `json:"currency"`
	Balance        int64     `json:"balance"`
	OverdraftLimit int64     `json:"overdraft_limit"`
	CreatedAt      time.Time `json:"created_at"`
}

type AccountView struct {
	ID        string       `json:"id"`
	UserID    string       `json:"user_id"`
	Status    string       `json:"status"`
	Ledgers   []LedgerView `json:"ledgers"`
	CreatedAt time.Time    `json:"created_at"`
}

// ToTransactionView maps a projection row. Sequence and balance appear once
// the transaction has settled.
func ToTransactionView(t repository.TransactionRecord) TransactionView {
	v := TransactionView{
		ID: t.ID, Type: string(t.Type), Status: string(t.Status), AccountID: t.AccountID, LedgerID: t.LedgerID,
		Amount: t.Amount, Currency: t.Currency, FailureReason: string(t.FailureReason),
		OriginalID: t.OriginalTransactionID, Reference: t.Reference, TraceID: t.TraceID,
		CreatedAt: t.CreatedAt.UTC(),
	}
	if t.SettledAt != nil {
		at := t.SettledAt.UTC()
		v.SettledAt = &at
		seq, bal := t.LedgerSequence, t.BalanceAfter
		v.LedgerSequence, v.BalanceAfter = &seq, &bal
	}
	return v
}

func ToLedgerView(l repository.LedgerRecord) LedgerView {
	return LedgerView{ID: l.ID, Currency: l.Currency, Balance: l.Balance, OverdraftLimit: l.OverdraftLimit, CreatedAt: l.CreatedAt.UTC()}
}

func ToAccountView(a repository.AccountRecord, ledgers []repository.LedgerRecord) AccountView {
	v := AccountView{ID: a.ID, UserID: a.UserID, Status: string(a.Status), Ledgers: []LedgerView{}, CreatedAt: a.CreatedAt.UTC()}
	for _, l := range ledgers {
		v.Ledgers = append(v.Ledgers, ToLedgerView(l))
	}
	return v
}

// failureStatus maps a FAILED transaction's reason to its HTTP status (TRD §6.5).
var failureStatus = map[ledger.FailureReason]int{
	ledger.ReasonInsufficientFunds: http.StatusUnprocessableEntity,
	ledger.ReasonAccountFrozen:     http.StatusUnprocessableEntity,
	ledger.ReasonNotVoidable:       http.StatusUnprocessableEntity,
	ledger.ReasonLedgerNotFound:    http.StatusNotFound,
	ledger.ReasonProcessingTimeout: http.StatusInternalServerError,
}

var failureMessage = map[ledger.FailureReason]string{
	ledger.ReasonInsufficientFunds: "insufficient funds",
	ledger.ReasonAccountFrozen:     "account is frozen; debits are blocked",
	ledger.ReasonNotVoidable:       "transaction cannot be voided",
	ledger.ReasonLedgerNotFound:    "account has no ledger for this currency",
	ledger.ReasonProcessingTimeout: "transaction could not be processed in time",
}

// errorBody matches the API's error envelope, plus transaction_id.
type errorBody struct {
	Error struct {
		Code          string `json:"code"`
		Message       string `json:"message"`
		RequestID     string `json:"request_id,omitempty"`
		TransactionID string `json:"transaction_id,omitempty"`
	} `json:"error"`
}

// renderTransaction builds the final response for a transaction in a
// terminal state: 201 with the transaction, or the failure envelope.
func renderTransaction(t repository.TransactionRecord, requestID string) (Response, error) {
	if t.Status == ledger.StatusFailed {
		var b errorBody
		b.Error.Code, b.Error.Message = string(t.FailureReason), failureMessage[t.FailureReason]
		b.Error.RequestID, b.Error.TransactionID = requestID, t.ID
		status := failureStatus[t.FailureReason]
		if status == 0 {
			status = http.StatusUnprocessableEntity
		}
		return marshalResponse(status, b)
	}
	status := http.StatusCreated
	if t.Status == ledger.StatusPending || t.Status == ledger.StatusProcessing {
		status = http.StatusAccepted
	}
	return marshalResponse(status, map[string]TransactionView{"transaction": ToTransactionView(t)})
}

func marshalResponse(status int, v any) (Response, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return Response{}, err
	}
	return Response{Status: status, Body: b}, nil
}
