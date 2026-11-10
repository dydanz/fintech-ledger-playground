package ledger_test

import (
	"errors"
	"math"
	"testing"

	"fintech-ledger-playground/internal/ledger"
)

func pending(typ ledger.TransactionType, amount int64) ledger.Transaction {
	return ledger.Transaction{ID: txID, AccountID: accountID, Status: ledger.StatusPending, Version: 1,
		Type: typ, Amount: amount, Currency: "IDR"}
}

func settledOriginal(typ ledger.TransactionType, amount int64) *ledger.Transaction {
	o := pending(typ, amount)
	o.ID, o.Status, o.Version = "orig", ledger.StatusSettled, 3
	return &o
}

func TestDecide(t *testing.T) {
	frozen, active := ledger.AccountStatusFrozen, ledger.AccountStatusActive
	failedOrig := settledOriginal(ledger.Credit, 30)
	failedOrig.Status = ledger.StatusFailed
	voidOrig := settledOriginal(ledger.Void, 30)
	otherAccount := settledOriginal(ledger.Credit, 30)
	otherAccount.AccountID = "other"
	wrongAmount := settledOriginal(ledger.Credit, 31)

	cases := []struct {
		name   string
		in     ledger.DecideInput
		settle bool
		delta  int64
		after  int64
		reason ledger.FailureReason
	}{
		{"credit", ledger.DecideInput{Tx: pending(ledger.Credit, 30), AccountStatus: active, LedgerFound: true, Balance: 10}, true, 30, 40, ""},
		{"credit on frozen account settles (FR-09)", ledger.DecideInput{Tx: pending(ledger.Credit, 30), AccountStatus: frozen, LedgerFound: true}, true, 30, 30, ""},
		{"debit", ledger.DecideInput{Tx: pending(ledger.Debit, 30), AccountStatus: active, LedgerFound: true, Balance: 100}, true, -30, 70, ""},
		{"debit to exactly zero", ledger.DecideInput{Tx: pending(ledger.Debit, 100), AccountStatus: active, LedgerFound: true, Balance: 100}, true, -100, 0, ""},
		{"debit one over", ledger.DecideInput{Tx: pending(ledger.Debit, 101), AccountStatus: active, LedgerFound: true, Balance: 100}, false, 0, 0, ledger.ReasonInsufficientFunds},
		{"debit into overdraft", ledger.DecideInput{Tx: pending(ledger.Debit, 150), AccountStatus: active, LedgerFound: true, Balance: 100, OverdraftLimit: 50}, true, -150, -50, ""},
		{"debit past overdraft", ledger.DecideInput{Tx: pending(ledger.Debit, 151), AccountStatus: active, LedgerFound: true, Balance: 100, OverdraftLimit: 50}, false, 0, 0, ledger.ReasonInsufficientFunds},
		{"debit on frozen account", ledger.DecideInput{Tx: pending(ledger.Debit, 1), AccountStatus: frozen, LedgerFound: true, Balance: 100}, false, 0, 0, ledger.ReasonAccountFrozen},
		{"frozen wins over insufficient funds", ledger.DecideInput{Tx: pending(ledger.Debit, 500), AccountStatus: frozen, LedgerFound: true}, false, 0, 0, ledger.ReasonAccountFrozen},
		{"ledger missing", ledger.DecideInput{Tx: pending(ledger.Credit, 1), AccountStatus: active}, false, 0, 0, ledger.ReasonLedgerNotFound},
		{"void of credit", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Balance: 50, Original: settledOriginal(ledger.Credit, 30)}, true, -30, 20, ""},
		{"void of debit", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Balance: 0, Original: settledOriginal(ledger.Debit, 30)}, true, 30, 30, ""},
		{"void of spent credit breaches floor (Q3)", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Balance: 10, Original: settledOriginal(ledger.Credit, 30)}, false, 0, 0, ledger.ReasonInsufficientFunds},
		{"void on frozen account allowed (Q3)", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: frozen, LedgerFound: true, Balance: 30, Original: settledOriginal(ledger.Credit, 30)}, true, -30, 0, ""},
		{"void of missing original", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true}, false, 0, 0, ledger.ReasonNotVoidable},
		{"void of failed original", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Original: failedOrig}, false, 0, 0, ledger.ReasonNotVoidable},
		{"void of a void", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Balance: 100, Original: voidOrig}, false, 0, 0, ledger.ReasonNotVoidable},
		{"void of another account's transaction", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Balance: 100, Original: otherAccount}, false, 0, 0, ledger.ReasonNotVoidable},
		{"void amount differs from original", ledger.DecideInput{Tx: pending(ledger.Void, 30), AccountStatus: active, LedgerFound: true, Balance: 100, Original: wrongAmount}, false, 0, 0, ledger.ReasonNotVoidable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := ledger.Decide(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			want := ledger.Decision{Settle: tc.settle, Delta: tc.delta, BalanceAfter: tc.after, Reason: tc.reason}
			if d != want {
				t.Fatalf("Decide = %+v, want %+v", d, want)
			}
		})
	}
}

func TestDecideErrors(t *testing.T) {
	notPending := pending(ledger.Credit, 1)
	notPending.Status = ledger.StatusProcessing
	if _, err := ledger.Decide(ledger.DecideInput{Tx: notPending, LedgerFound: true}); !errors.Is(err, ledger.ErrNotPending) {
		t.Errorf("not pending: err = %v", err)
	}
	if _, err := ledger.Decide(ledger.DecideInput{Tx: pending(ledger.Credit, 1), LedgerFound: true, Balance: math.MaxInt64}); !errors.Is(err, ledger.ErrOverflow) {
		t.Errorf("overflow: err = %v", err)
	}
	if _, err := ledger.Decide(ledger.DecideInput{Tx: pending("BOGUS", 1), LedgerFound: true}); !errors.Is(err, ledger.ErrIllegalTransition) {
		t.Errorf("unknown type: err = %v", err)
	}
}
