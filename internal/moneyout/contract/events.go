package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

// Event types (T§7). Commands, results and acks cross services; the rest are
// MOM's journal.
const (
	EventTransferRequested    = "TransferRequested"    // money-out.commands.v1
	EventTransferSucceeded    = "TransferSucceeded"    // money-out.results.v1 and the journal
	EventTransferFailed       = "TransferFailed"       // money-out.results.v1 and the journal
	EventTransferAcknowledged = "TransferAcknowledged" // money-out.core-acks.v1
	EventBankStatusReported   = "BankStatusReported"   // money-out.bank-events.v1

	EventTransferReceived     = "TransferReceived"
	EventTransferRejected     = "TransferRejected"
	EventTransferRouted       = "TransferRouted"
	EventAttemptDispatching   = "AttemptDispatching"
	EventAttemptAccepted      = "AttemptAccepted"
	EventAttemptProcessing    = "AttemptProcessing"
	EventAttemptUnknown       = "AttemptUnknown"
	EventAttemptFailed        = "AttemptFailed"
	EventTransferNeedsReview  = "TransferNeedsReview"
	EventBankStatusReceived   = "BankStatusReceived"
	EventCoreAcknowledged     = "CoreAcknowledged"
	EventBreakerChanged       = "BreakerChanged"
	EventReconExceptionRaised = "ReconExceptionRaised"
)

// Topics (T§7).
const (
	TopicCommands   = "money-out.commands.v1"
	TopicResults    = "money-out.results.v1"
	TopicCoreAcks   = "money-out.core-acks.v1"
	TopicBankEvents = "money-out.bank-events.v1"
	TopicJournal    = "money-out.journal.v1"
)

// CommandDoTransfer is the only command (P§2.1).
const CommandDoTransfer = "do_transfer"

// Beneficiary is where the money goes. No bank, rail or channel: routing is
// MOM's (P§2.1).
type Beneficiary struct {
	BankCode  string `json:"bank_code"`
	AccountNo string `json:"account_no"`
}

// TransferRequested is the do_transfer command (P§2.1, T§3.2).
type TransferRequested struct {
	Command     string          `json:"command"`
	TransferID  string          `json:"transfer_id"`
	Amount      Amount          `json:"amount"`
	Beneficiary Beneficiary     `json:"beneficiary"`
	RequestedAt time.Time       `json:"requested_at"`
	Metadata    json.RawMessage `json:"metadata,omitempty"` // passed through untouched
}

var (
	uuidPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	bankCodePattern  = regexp.MustCompile(`^[0-9]{3}$`)
	accountNoPattern = regexp.MustCompile(`^[0-9]{5,34}$`)
)

var ErrInvalidCommand = errors.New("contract: invalid command")

// Validate checks the command's shape and its amount (DefaultMoneyRules).
func (p TransferRequested) Validate() error {
	switch {
	case p.Command != CommandDoTransfer:
		return fmt.Errorf("%w: command %q", ErrInvalidCommand, p.Command)
	case !uuidPattern.MatchString(p.TransferID):
		return fmt.Errorf("%w: transfer_id must be a lowercase UUID", ErrInvalidCommand)
	case !bankCodePattern.MatchString(p.Beneficiary.BankCode):
		return fmt.Errorf("%w: bank_code must be 3 digits", ErrInvalidCommand)
	case !accountNoPattern.MatchString(p.Beneficiary.AccountNo):
		return fmt.Errorf("%w: account_no must be 5-34 digits", ErrInvalidCommand)
	case p.RequestedAt.IsZero():
		return fmt.Errorf("%w: requested_at is required", ErrInvalidCommand)
	}
	if _, err := p.Amount.Money(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidCommand, err)
	}
	if len(p.Metadata) > 0 {
		var obj map[string]any
		if json.Unmarshal(p.Metadata, &obj) != nil || obj == nil {
			return fmt.Errorf("%w: metadata must be a JSON object", ErrInvalidCommand)
		}
	}
	return nil
}

// TransferResult is the terminal result to the core (T§3.3): exactly one per
// transfer, carried by TransferSucceeded or TransferFailed.
type TransferResult struct {
	TransferID     string    `json:"transfer_id"`
	Status         Status    `json:"status"` // SUCCEEDED or FAILED
	Reason         Reason    `json:"reason,omitempty"`
	BankMiddleware string    `json:"bank_middleware,omitempty"`
	Rail           string    `json:"rail,omitempty"`
	BankReference  string    `json:"bank_reference,omitempty"`
	Attempts       int       `json:"attempts"`
	CompletedAt    time.Time `json:"completed_at"`
}

// Core entries an acknowledgement maps to (T§2.3, H1).
const (
	CoreEntryDebitFinal = "DEBIT_FINAL" // SUCCEEDED: the debit already was the final debit
	CoreEntryVoid       = "VOID"        // FAILED: the debit was voided
)

// TransferAcknowledged is the core's acknowledgement of a result (T§3.4).
type TransferAcknowledged struct {
	TransferID        string `json:"transfer_id"`
	ResultEventID     string `json:"result_event_id"`
	CoreEntry         string `json:"core_entry"`
	CoreTransactionID string `json:"core_transaction_id"`
}

// Rails a bank middleware may be asked to use (P§2.3).
const (
	RailBIFAST    = "BIFAST"
	RailRTOL      = "RTOL"
	RailSKNBI     = "SKNBI"
	RailRTGS      = "RTGS"
	RailIntrabank = "INTRABANK"
)

// TransferRequest is MOM's signed request to a bank middleware, one per
// attempt (P§2.3).
type TransferRequest struct {
	RequestID   string      `json:"request_id"`
	TransferID  string      `json:"transfer_id"`
	AttemptNo   int         `json:"attempt_no"`
	Rail        string      `json:"rail"`
	Amount      Amount      `json:"amount"`
	Beneficiary Beneficiary `json:"beneficiary"`
	IssuedAt    time.Time   `json:"issued_at"`
	Nonce       string      `json:"nonce"`
}

// StatusInquiry asks a bank middleware for an attempt's status (P§2.4).
type StatusInquiry struct {
	TransferID string `json:"transfer_id"`
	RequestID  string `json:"request_id"`
}

// BankStatusReported is an asynchronous status from a bank middleware
// (money-out.bank-events.v1, keyed by transfer_id).
type BankStatusReported struct {
	TransferID     string     `json:"transfer_id"`
	RequestID      string     `json:"request_id"`
	BankMiddleware string     `json:"bank_middleware"`
	Result         BankResult `json:"result"`
	ReportedAt     time.Time  `json:"reported_at"`
}

// JournalPayload is the payload of a state transition in money-out.journal.v1
// (P§3.5): the states, the attempt, and any detail.
type JournalPayload struct {
	TransferID        string          `json:"transfer_id,omitempty"`
	AttemptNo         int             `json:"attempt_no,omitempty"`
	FromState         string          `json:"from_state,omitempty"`
	ToState           string          `json:"to_state"`
	CoreTransactionID string          `json:"core_transaction_id,omitempty"`
	Detail            json.RawMessage `json:"detail,omitempty"`
}
