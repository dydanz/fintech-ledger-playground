package ledger

import "github.com/google/uuid"

// TransactionType values match ledger.transactions.type.
type TransactionType string

const (
	Credit TransactionType = "CREDIT"
	Debit  TransactionType = "DEBIT"
	Void   TransactionType = "VOID"
)

// Status is a transaction's lifecycle state (TRD §4.4). Values match
// ledger.transactions.status.
type Status string

const (
	StatusNone       Status = "" // before TransactionRequested
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusSettled    Status = "SETTLED"
	StatusFailed     Status = "FAILED"
	StatusVoided     Status = "VOIDED"
)

// AccountStatus values match ledger.accounts.status.
type AccountStatus string

const (
	AccountStatusNone   AccountStatus = "" // before AccountOpened
	AccountStatusActive AccountStatus = "ACTIVE"
	AccountStatusFrozen AccountStatus = "FROZEN"
)

// FailureReason is the fixed set stored in ledger.transactions.failure_reason,
// carried by TransactionFailed, and used as the log `reason` (TRD §6.5, §11.1).
type FailureReason string

const (
	ReasonInsufficientFunds FailureReason = "insufficient_funds"
	ReasonAccountFrozen     FailureReason = "account_frozen"
	ReasonNotVoidable       FailureReason = "not_voidable"
	ReasonLedgerNotFound    FailureReason = "ledger_not_found"
	ReasonProcessingTimeout FailureReason = "processing_timeout"
)

// Default system ledger codes used as counterparties (TRD §4.3).
const (
	CounterpartyTopupClearing = "topup_clearing"
	CounterpartySettlement    = "settlement"
)

// DefaultCounterparty is the system ledger a credit or debit posts against
// when the caller doesn't name one.
func DefaultCounterparty(t TransactionType) string {
	if t == Debit {
		return CounterpartySettlement
	}
	return CounterpartyTopupClearing
}

// NewID returns a UUIDv7, which sorts by creation time and indexes well.
func NewID() string { return uuid.Must(uuid.NewV7()).String() }
