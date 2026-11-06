package ledger_test

import (
	"errors"
	"testing"

	"auth-api/internal/ledger"
)

func acctEvent(version int64, p ledger.Payload) ledger.Event {
	return ledger.Event{ID: ledger.NewID(), StreamID: accountID, StreamVersion: version, AccountID: accountID, Payload: p, TraceID: traceID}
}

func accountInState(t *testing.T, status ledger.AccountStatus) ledger.Account {
	t.Helper()
	path := map[ledger.AccountStatus][]ledger.Payload{
		ledger.AccountStatusNone:   {},
		ledger.AccountStatusActive: {ledger.AccountOpened{UserID: "u1"}},
		ledger.AccountStatusFrozen: {ledger.AccountOpened{UserID: "u1"}, ledger.AccountFrozen{Reason: "fraud", Actor: "fraud-engine"}},
	}[status]
	var events []ledger.Event
	for i, p := range path {
		events = append(events, acctEvent(int64(i+1), p))
	}
	s, err := ledger.FoldAccount(events)
	if err != nil {
		t.Fatalf("building %s: %v", status, err)
	}
	return s
}

func TestAccountTransitionMatrix(t *testing.T) {
	events := map[string]ledger.Payload{
		"Opened":       ledger.AccountOpened{UserID: "u1"},
		"LedgerOpened": ledger.LedgerOpened{LedgerID: ledgerID, Currency: "IDR"},
		"Frozen":       ledger.AccountFrozen{Reason: "fraud", Actor: "fraud-engine", Rule: "topup_velocity"},
		"Unfrozen":     ledger.AccountUnfrozen{Reason: "cleared", Actor: "ops"},
	}
	legal := map[ledger.AccountStatus]map[string]ledger.AccountStatus{
		ledger.AccountStatusNone:   {"Opened": ledger.AccountStatusActive},
		ledger.AccountStatusActive: {"LedgerOpened": ledger.AccountStatusActive, "Frozen": ledger.AccountStatusFrozen},
		ledger.AccountStatusFrozen: {"LedgerOpened": ledger.AccountStatusFrozen, "Unfrozen": ledger.AccountStatusActive},
	}
	for from, allowed := range legal {
		for name, p := range events {
			t.Run(string(from)+"+"+name, func(t *testing.T) {
				s := accountInState(t, from)
				got, err := ledger.ApplyAccount(s, acctEvent(s.Version+1, p))
				want, ok := allowed[name]
				switch {
				case ok && err != nil:
					t.Fatalf("want %s, got error %v", want, err)
				case ok && got.Status != want:
					t.Fatalf("status = %s, want %s", got.Status, want)
				case !ok && !errors.Is(err, ledger.ErrIllegalTransition):
					t.Fatalf("want ErrIllegalTransition, got %v", err)
				}
			})
		}
	}
}

func TestLedgerOpenedOncePerCurrencyWithoutMutatingPriorState(t *testing.T) {
	s := accountInState(t, ledger.AccountStatusActive)
	s1, err := ledger.ApplyAccount(s, acctEvent(2, ledger.LedgerOpened{LedgerID: "idr", Currency: "IDR"}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.ApplyAccount(s1, acctEvent(3, ledger.LedgerOpened{LedgerID: "idr2", Currency: "IDR"})); !errors.Is(err, ledger.ErrIllegalTransition) {
		t.Fatalf("second IDR ledger: err = %v, want ErrIllegalTransition", err)
	}
	s2, err := ledger.ApplyAccount(s1, acctEvent(3, ledger.LedgerOpened{LedgerID: "usd", Currency: "USD"}))
	if err != nil {
		t.Fatal(err)
	}
	if len(s1.Ledgers) != 1 || len(s2.Ledgers) != 2 || s2.Ledgers["USD"] != "usd" {
		t.Fatalf("ledgers: before %v, after %v", s1.Ledgers, s2.Ledgers)
	}
}

func TestAccountVersionCheck(t *testing.T) {
	s := accountInState(t, ledger.AccountStatusActive)
	if _, err := ledger.ApplyAccount(s, acctEvent(3, ledger.AccountFrozen{})); !errors.Is(err, ledger.ErrVersionMismatch) {
		t.Fatalf("err = %v, want ErrVersionMismatch", err)
	}
}
