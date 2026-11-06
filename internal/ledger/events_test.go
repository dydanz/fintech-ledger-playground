package ledger_test

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"github.com/google/uuid"

	"auth-api/internal/ledger"
)

func allPayloads() []ledger.Payload {
	return []ledger.Payload{
		ledger.TransactionRequested{Type: ledger.Void, Amount: 9, Currency: "IDR", Counterparty: "settlement",
			OriginalTransactionID: "o", Reference: "ORD-1", RequestedBy: "ops"},
		ledger.TransactionProcessing{LedgerID: ledgerID},
		ledger.TransactionSettled{JournalEntryIDs: []string{"a", "b"}, LedgerSequence: 4, BalanceAfter: -3},
		ledger.TransactionFailed{Reason: ledger.ReasonNotVoidable},
		ledger.TransactionVoided{VoidTransactionID: "v"},
		ledger.AccountOpened{UserID: "u"},
		ledger.LedgerOpened{LedgerID: ledgerID, Currency: "USD", OverdraftLimit: 50},
		ledger.AccountFrozen{Reason: "velocity", Actor: "fraud-engine", Rule: "topup_velocity"},
		ledger.AccountUnfrozen{Reason: "cleared", Actor: "ops"},
	}
}

func TestPayloadRoundTrip(t *testing.T) {
	for _, p := range allPayloads() {
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ledger.DecodePayload(p.EventType(), raw)
		if err != nil {
			t.Fatalf("%s: %v", p.EventType(), err)
		}
		if !reflect.DeepEqual(got, p) {
			t.Errorf("%s round trip: got %#v, want %#v", p.EventType(), got, p)
		}
	}
}

func TestDecodePayloadRejectsDrift(t *testing.T) {
	if _, err := ledger.DecodePayload(ledger.EventTransactionFailed, []byte(`{"reason":"x","extra":1}`)); err == nil {
		t.Error("unknown field accepted")
	}
	if _, err := ledger.DecodePayload("TransactionTeleported", []byte(`{}`)); !errors.Is(err, ledger.ErrUnknownEventType) {
		t.Errorf("unknown type: err = %v", err)
	}
}

// The stream of each event type must match the events_type_valid CHECK in
// migrations/002_ledger.up.sql.
func TestEventStreams(t *testing.T) {
	want := map[ledger.EventType]ledger.StreamType{
		ledger.EventTransactionRequested: ledger.StreamTransaction, ledger.EventTransactionProcessing: ledger.StreamTransaction,
		ledger.EventTransactionSettled: ledger.StreamTransaction, ledger.EventTransactionFailed: ledger.StreamTransaction,
		ledger.EventTransactionVoided: ledger.StreamTransaction,
		ledger.EventAccountOpened:     ledger.StreamAccount, ledger.EventLedgerOpened: ledger.StreamAccount,
		ledger.EventAccountFrozen: ledger.StreamAccount, ledger.EventAccountUnfrozen: ledger.StreamAccount,
	}
	for _, p := range allPayloads() {
		got, err := p.EventType().Stream()
		if err != nil || got != want[p.EventType()] {
			t.Errorf("%s: stream %q, %v", p.EventType(), got, err)
		}
	}
	if _, err := ledger.EventType("Nope").Stream(); !errors.Is(err, ledger.ErrUnknownEventType) {
		t.Errorf("unknown type: err = %v", err)
	}
}

func TestNewIDIsTimeOrderedUUIDv7(t *testing.T) {
	a, b := ledger.NewID(), ledger.NewID()
	u, err := uuid.Parse(a)
	if err != nil || u.Version() != 7 {
		t.Fatalf("NewID() = %q (version %d, %v), want a UUIDv7", a, u.Version(), err)
	}
	if a >= b {
		t.Fatalf("UUIDv7s not increasing: %s then %s", a, b)
	}
}
