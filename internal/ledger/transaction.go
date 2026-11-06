package ledger

import (
	"errors"
	"fmt"
)

var (
	ErrIllegalTransition = errors.New("ledger: illegal transition")
	ErrVersionMismatch   = errors.New("ledger: stream version is not the next one")
	ErrWrongStream       = errors.New("ledger: event belongs to another stream")
)

// Transaction is the fold of one transaction stream: the state the
// ledger.transactions projection must equal (TRD §3.2, reconciler check 4).
type Transaction struct {
	ID                    string
	AccountID             string
	Status                Status
	Version               int64
	Type                  TransactionType
	Amount                int64
	Currency              string
	Counterparty          string
	OriginalTransactionID string
	Reference             string
	RequestedBy           string
	LedgerID              string
	FailureReason         FailureReason
	LedgerSequence        int64
	BalanceAfter          int64
	VoidedBy              string
}

// ApplyTransaction returns s with e applied, enforcing the state machine:
//
//	(none) ─Requested→ PENDING ─Processing→ PROCESSING ─Settled→ SETTLED ─Voided→ VOIDED
//	                                                   └─Failed→ FAILED
//
// A VOID transaction can't itself be voided.
func ApplyTransaction(s Transaction, e Event) (Transaction, error) {
	if s.Version > 0 && e.StreamID != s.ID {
		return s, ErrWrongStream
	}
	if e.StreamVersion != s.Version+1 {
		return s, fmt.Errorf("%w: have %d, got %d", ErrVersionMismatch, s.Version, e.StreamVersion)
	}
	illegal := func() (Transaction, error) {
		return s, fmt.Errorf("%w: %s in state %q", ErrIllegalTransition, e.Type(), s.Status)
	}

	next := s
	switch p := e.Payload.(type) {
	case TransactionRequested:
		if s.Status != StatusNone {
			return illegal()
		}
		next = Transaction{
			ID: e.StreamID, AccountID: e.AccountID, Status: StatusPending,
			Type: p.Type, Amount: p.Amount, Currency: p.Currency, Counterparty: p.Counterparty,
			OriginalTransactionID: p.OriginalTransactionID, Reference: p.Reference, RequestedBy: p.RequestedBy,
		}
	case TransactionProcessing:
		if s.Status != StatusPending {
			return illegal()
		}
		next.Status, next.LedgerID = StatusProcessing, p.LedgerID
	case TransactionSettled:
		if s.Status != StatusProcessing {
			return illegal()
		}
		next.Status, next.LedgerSequence, next.BalanceAfter = StatusSettled, p.LedgerSequence, p.BalanceAfter
	case TransactionFailed:
		if s.Status != StatusProcessing {
			return illegal()
		}
		next.Status, next.FailureReason = StatusFailed, p.Reason
	case TransactionVoided:
		if s.Status != StatusSettled || s.Type == Void {
			return illegal()
		}
		next.Status, next.VoidedBy = StatusVoided, p.VoidTransactionID
	default:
		return illegal()
	}
	next.Version = e.StreamVersion
	return next, nil
}

// FoldTransaction is the left-fold of a transaction stream, in version order.
func FoldTransaction(events []Event) (Transaction, error) {
	var s Transaction
	for _, e := range events {
		var err error
		if s, err = ApplyTransaction(s, e); err != nil {
			return s, err
		}
	}
	return s, nil
}
