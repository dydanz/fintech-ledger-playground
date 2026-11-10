// Package fraud holds the fraud engine's velocity rules (spec/TRD §7.4).
// Rules read only settled credits and debits, and only event time
// (occurred_at), so replaying the feed reproduces the same decisions.
package fraud

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/outbox"
)

// Settlement is a settled credit or debit, read from the event feed.
type Settlement struct {
	EventID       string
	AccountID     string
	TransactionID string
	Type          ledger.TransactionType // Credit or Debit
	Amount        int64
	Currency      string
	OccurredAt    time.Time
	TraceID       string
}

// ErrMalformed marks an envelope that can't be read. The engine logs and
// skips it rather than blocking the partition on it forever.
var ErrMalformed = errors.New("fraud: malformed event")

// ParseSettlement reads an envelope from ledger.events.v1. ok is false for
// every event the rules ignore: anything but TransactionSettled, voids, and
// settlements written before the payload carried type/amount/currency.
func ParseSettlement(value []byte) (s Settlement, ok bool, err error) {
	var env outbox.Envelope
	if err := json.Unmarshal(value, &env); err != nil {
		return Settlement{}, false, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if env.EventType != string(ledger.EventTransactionSettled) {
		return Settlement{}, false, nil
	}
	var p ledger.TransactionSettled
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		return Settlement{}, false, fmt.Errorf("%w: payload: %v", ErrMalformed, err)
	}
	if p.Type != ledger.Credit && p.Type != ledger.Debit {
		return Settlement{}, false, nil
	}
	if env.EventID == "" || env.AccountID == "" || env.StreamID == "" || env.OccurredAt.IsZero() || p.Amount <= 0 || p.Currency == "" {
		return Settlement{}, false, fmt.Errorf("%w: missing fields", ErrMalformed)
	}
	return Settlement{EventID: env.EventID, AccountID: env.AccountID, TransactionID: env.StreamID, Type: p.Type,
		Amount: p.Amount, Currency: p.Currency, OccurredAt: env.OccurredAt, TraceID: env.TraceID}, true, nil
}
