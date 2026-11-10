package ledger_test

import (
	"errors"
	"testing"

	"fintech-ledger-playground/internal/ledger"
)

const (
	txID      = "0192f3c5-0000-7000-8000-000000000001"
	accountID = "0192f3c4-0000-7000-8000-000000000001"
	ledgerID  = "0192f3c4-0000-7000-8000-0000000000aa"
	traceID   = "4bf92f3577b34da6a3ce929d0e0e4736"
)

func txEvent(version int64, p ledger.Payload) ledger.Event {
	return ledger.Event{ID: ledger.NewID(), StreamID: txID, StreamVersion: version, AccountID: accountID, Payload: p, TraceID: traceID}
}

func requested(typ ledger.TransactionType) ledger.TransactionRequested {
	return ledger.TransactionRequested{Type: typ, Amount: 100, Currency: "IDR", Counterparty: ledger.DefaultCounterparty(typ), RequestedBy: "payments"}
}

// txInState folds the shortest legal path to status for a transaction of type typ.
func txInState(t *testing.T, typ ledger.TransactionType, status ledger.Status) ledger.Transaction {
	t.Helper()
	path := map[ledger.Status][]ledger.Payload{
		ledger.StatusNone:       {},
		ledger.StatusPending:    {requested(typ)},
		ledger.StatusProcessing: {requested(typ), ledger.TransactionProcessing{LedgerID: ledgerID}},
		ledger.StatusSettled: {requested(typ), ledger.TransactionProcessing{LedgerID: ledgerID},
			ledger.TransactionSettled{LedgerSequence: 1, BalanceAfter: 100}},
		ledger.StatusFailed: {requested(typ), ledger.TransactionProcessing{LedgerID: ledgerID},
			ledger.TransactionFailed{Reason: ledger.ReasonInsufficientFunds}},
		ledger.StatusVoided: {requested(typ), ledger.TransactionProcessing{LedgerID: ledgerID},
			ledger.TransactionSettled{LedgerSequence: 1, BalanceAfter: 100}, ledger.TransactionVoided{VoidTransactionID: "v"}},
	}[status]
	var events []ledger.Event
	for i, p := range path {
		events = append(events, txEvent(int64(i+1), p))
	}
	s, err := ledger.FoldTransaction(events)
	if err != nil {
		t.Fatalf("building %s: %v", status, err)
	}
	return s
}

func TestTransactionTransitionMatrix(t *testing.T) {
	events := map[string]ledger.Payload{
		"Requested":  requested(ledger.Credit),
		"Processing": ledger.TransactionProcessing{LedgerID: ledgerID},
		"Settled":    ledger.TransactionSettled{LedgerSequence: 1, BalanceAfter: 100},
		"Failed":     ledger.TransactionFailed{Reason: ledger.ReasonAccountFrozen},
		"Voided":     ledger.TransactionVoided{VoidTransactionID: "v"},
	}
	// legal[from][event] = resulting status; anything absent must be ErrIllegalTransition.
	legal := map[ledger.Status]map[string]ledger.Status{
		ledger.StatusNone:       {"Requested": ledger.StatusPending},
		ledger.StatusPending:    {"Processing": ledger.StatusProcessing},
		ledger.StatusProcessing: {"Settled": ledger.StatusSettled, "Failed": ledger.StatusFailed},
		ledger.StatusSettled:    {"Voided": ledger.StatusVoided},
		ledger.StatusFailed:     {},
		ledger.StatusVoided:     {},
	}
	for from, allowed := range legal {
		for name, p := range events {
			t.Run(string(from)+"+"+name, func(t *testing.T) {
				s := txInState(t, ledger.Credit, from)
				got, err := ledger.ApplyTransaction(s, txEvent(s.Version+1, p))
				want, ok := allowed[name]
				switch {
				case ok && err != nil:
					t.Fatalf("want %s, got error %v", want, err)
				case ok && got.Status != want:
					t.Fatalf("status = %s, want %s", got.Status, want)
				case ok && got.Version != s.Version+1:
					t.Fatalf("version = %d, want %d", got.Version, s.Version+1)
				case !ok && !errors.Is(err, ledger.ErrIllegalTransition):
					t.Fatalf("want ErrIllegalTransition, got %v (status %s)", err, got.Status)
				case !ok && got != s:
					t.Fatal("state changed on an illegal transition")
				}
			})
		}
	}
}

func TestVoidOfAVoidIsIllegal(t *testing.T) {
	s := txInState(t, ledger.Void, ledger.StatusSettled)
	_, err := ledger.ApplyTransaction(s, txEvent(s.Version+1, ledger.TransactionVoided{VoidTransactionID: "v"}))
	if !errors.Is(err, ledger.ErrIllegalTransition) {
		t.Fatalf("err = %v, want ErrIllegalTransition", err)
	}
}

func TestTransactionVersionAndStreamChecks(t *testing.T) {
	s := txInState(t, ledger.Debit, ledger.StatusPending)
	proc := ledger.TransactionProcessing{LedgerID: ledgerID}

	for _, v := range []int64{0, 1, 3} {
		if _, err := ledger.ApplyTransaction(s, txEvent(v, proc)); !errors.Is(err, ledger.ErrVersionMismatch) {
			t.Errorf("version %d: err = %v, want ErrVersionMismatch", v, err)
		}
	}
	other := txEvent(2, proc)
	other.StreamID = "someone-else"
	if _, err := ledger.ApplyTransaction(s, other); !errors.Is(err, ledger.ErrWrongStream) {
		t.Errorf("err = %v, want ErrWrongStream", err)
	}
}

func TestFoldTransactionCarriesRequestAndOutcome(t *testing.T) {
	r := requested(ledger.Debit)
	r.Reference = "ORD-1"
	s, err := ledger.FoldTransaction([]ledger.Event{
		txEvent(1, r),
		txEvent(2, ledger.TransactionProcessing{LedgerID: ledgerID}),
		txEvent(3, ledger.TransactionSettled{JournalEntryIDs: []string{"a", "b"}, LedgerSequence: 7, BalanceAfter: 900}),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := ledger.Transaction{
		ID: txID, AccountID: accountID, Status: ledger.StatusSettled, Version: 3,
		Type: ledger.Debit, Amount: 100, Currency: "IDR", Counterparty: ledger.CounterpartySettlement,
		Reference: "ORD-1", RequestedBy: "payments", LedgerID: ledgerID, LedgerSequence: 7, BalanceAfter: 900,
	}
	if s != want {
		t.Fatalf("fold =\n %+v\nwant\n %+v", s, want)
	}
}
