package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// StreamType values match ledger.events.stream_type.
type StreamType string

const (
	StreamTransaction StreamType = "transaction"
	StreamAccount     StreamType = "account"
)

// EventType values match ledger.events.event_type (TRD §4.5).
type EventType string

const (
	EventTransactionRequested  EventType = "TransactionRequested"
	EventTransactionProcessing EventType = "TransactionProcessing"
	EventTransactionSettled    EventType = "TransactionSettled"
	EventTransactionFailed     EventType = "TransactionFailed"
	EventTransactionVoided     EventType = "TransactionVoided"
	EventAccountOpened         EventType = "AccountOpened"
	EventLedgerOpened          EventType = "LedgerOpened"
	EventAccountFrozen         EventType = "AccountFrozen"
	EventAccountUnfrozen       EventType = "AccountUnfrozen"
)

// SchemaVersion is the payload schema version written with every event.
const SchemaVersion = 1

var ErrUnknownEventType = errors.New("ledger: unknown event type")

// Stream returns the stream an event type belongs to.
func (t EventType) Stream() (StreamType, error) {
	switch t {
	case EventTransactionRequested, EventTransactionProcessing, EventTransactionSettled,
		EventTransactionFailed, EventTransactionVoided:
		return StreamTransaction, nil
	case EventAccountOpened, EventLedgerOpened, EventAccountFrozen, EventAccountUnfrozen:
		return StreamAccount, nil
	}
	return "", fmt.Errorf("%w: %q", ErrUnknownEventType, t)
}

// Payload is an event body. Each payload type knows its event type, so an
// event can't be built with a mismatched type and body.
type Payload interface {
	EventType() EventType
}

// Event is one immutable record in a stream. StreamVersion starts at 1 and is
// assigned under the stream's row lock (TRD §4.5).
type Event struct {
	ID            string
	StreamID      string
	StreamVersion int64
	AccountID     string
	Payload       Payload
	TraceID       string
	OccurredAt    time.Time
}

func (e Event) Type() EventType { return e.Payload.EventType() }

// --- transaction stream payloads ---

type TransactionRequested struct {
	Type                  TransactionType `json:"type"`
	Amount                int64           `json:"amount"`
	Currency              string          `json:"currency"`
	Counterparty          string          `json:"counterparty"`
	OriginalTransactionID string          `json:"original_transaction_id,omitempty"`
	Reference             string          `json:"reference,omitempty"`
	RequestedBy           string          `json:"requested_by"`
}

type TransactionProcessing struct {
	LedgerID string `json:"ledger_id,omitempty"` // empty when the ledger doesn't exist
}

type TransactionSettled struct {
	JournalEntryIDs []string `json:"journal_entry_ids"`
	LedgerSequence  int64    `json:"ledger_sequence"`
	BalanceAfter    int64    `json:"balance_after"`
}

type TransactionFailed struct {
	Reason FailureReason `json:"reason"`
}

type TransactionVoided struct {
	VoidTransactionID string `json:"void_transaction_id"`
}

// --- account stream payloads ---

type AccountOpened struct {
	UserID string `json:"user_id"`
}

type LedgerOpened struct {
	LedgerID       string `json:"ledger_id"`
	Currency       string `json:"currency"`
	OverdraftLimit int64  `json:"overdraft_limit"`
}

type AccountFrozen struct {
	Reason string `json:"reason"`
	Actor  string `json:"actor"`
	Rule   string `json:"rule,omitempty"`
}

type AccountUnfrozen struct {
	Reason string `json:"reason"`
	Actor  string `json:"actor"`
}

func (TransactionRequested) EventType() EventType  { return EventTransactionRequested }
func (TransactionProcessing) EventType() EventType { return EventTransactionProcessing }
func (TransactionSettled) EventType() EventType    { return EventTransactionSettled }
func (TransactionFailed) EventType() EventType     { return EventTransactionFailed }
func (TransactionVoided) EventType() EventType     { return EventTransactionVoided }
func (AccountOpened) EventType() EventType         { return EventAccountOpened }
func (LedgerOpened) EventType() EventType          { return EventLedgerOpened }
func (AccountFrozen) EventType() EventType         { return EventAccountFrozen }
func (AccountUnfrozen) EventType() EventType       { return EventAccountUnfrozen }

// DecodePayload parses a stored or published payload of the given type.
// Unknown fields are rejected so a schema drift surfaces instead of being
// silently dropped.
func DecodePayload(t EventType, raw []byte) (Payload, error) {
	var p Payload
	switch t {
	case EventTransactionRequested:
		p = &TransactionRequested{}
	case EventTransactionProcessing:
		p = &TransactionProcessing{}
	case EventTransactionSettled:
		p = &TransactionSettled{}
	case EventTransactionFailed:
		p = &TransactionFailed{}
	case EventTransactionVoided:
		p = &TransactionVoided{}
	case EventAccountOpened:
		p = &AccountOpened{}
	case EventLedgerOpened:
		p = &LedgerOpened{}
	case EventAccountFrozen:
		p = &AccountFrozen{}
	case EventAccountUnfrozen:
		p = &AccountUnfrozen{}
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownEventType, t)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(p); err != nil {
		return nil, fmt.Errorf("ledger: decode %s payload: %w", t, err)
	}
	return deref(p), nil
}

// deref returns the payload by value, so decoded and constructed events compare equal.
func deref(p Payload) Payload {
	switch v := p.(type) {
	case *TransactionRequested:
		return *v
	case *TransactionProcessing:
		return *v
	case *TransactionSettled:
		return *v
	case *TransactionFailed:
		return *v
	case *TransactionVoided:
		return *v
	case *AccountOpened:
		return *v
	case *LedgerOpened:
		return *v
	case *AccountFrozen:
		return *v
	case *AccountUnfrozen:
		return *v
	}
	return p
}
