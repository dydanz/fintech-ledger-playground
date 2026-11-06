package ledger

import "errors"

var ErrNotPending = errors.New("ledger: decide needs a PENDING transaction")

// DecideInput is what Tx2 has read under its locks (TRD §7.1).
type DecideInput struct {
	Tx             Transaction   // the transaction being processed (PENDING)
	AccountStatus  AccountStatus // read FOR SHARE
	LedgerFound    bool          // the USER ledger for Tx.Currency exists
	Balance        int64         // that ledger's balance, read FOR UPDATE
	OverdraftLimit int64
	Original       *Transaction // VOID only: the original, read FOR UPDATE; nil if not found
}

// Decision is the outcome of processing. When Settle is true, Delta is the
// signed change to the user ledger (the counterparty line is -Delta) and
// BalanceAfter the resulting balance. Otherwise Reason says why it failed.
type Decision struct {
	Settle       bool
	Delta        int64
	BalanceAfter int64
	Reason       FailureReason
}

// Decide applies the business rules in TRD §7.1, in order:
//
//	ledger missing                         → FAILED ledger_not_found
//	VOID of anything but a SETTLED non-void → FAILED not_voidable
//	DEBIT on a FROZEN account              → FAILED account_frozen
//	result below −overdraft_limit          → FAILED insufficient_funds (also for voids, TRD §16 Q3)
//
// Credits settle on frozen accounts (FR-09), and voids are allowed on frozen
// accounts (Q3 default). An int64 overflow is an error, not a business failure.
func Decide(in DecideInput) (Decision, error) {
	if in.Tx.Status != StatusPending {
		return Decision{}, ErrNotPending
	}
	if !in.LedgerFound {
		return Decision{Reason: ReasonLedgerNotFound}, nil
	}

	var delta int64
	switch in.Tx.Type {
	case Credit:
		delta = in.Tx.Amount
	case Debit:
		if in.AccountStatus == AccountStatusFrozen {
			return Decision{Reason: ReasonAccountFrozen}, nil
		}
		delta = -in.Tx.Amount
	case Void:
		o := in.Original
		if o == nil || o.Status != StatusSettled || o.Type == Void ||
			o.AccountID != in.Tx.AccountID || o.Currency != in.Tx.Currency || o.Amount != in.Tx.Amount {
			return Decision{Reason: ReasonNotVoidable}, nil
		}
		delta = o.Amount // voiding a debit gives the money back
		if o.Type == Credit {
			delta = -o.Amount
		}
	default:
		return Decision{}, ErrIllegalTransition
	}

	after, err := addInt64(in.Balance, delta)
	if err != nil {
		return Decision{}, err
	}
	if delta < 0 && after < -in.OverdraftLimit {
		return Decision{Reason: ReasonInsufficientFunds}, nil
	}
	return Decision{Settle: true, Delta: delta, BalanceAfter: after}, nil
}
