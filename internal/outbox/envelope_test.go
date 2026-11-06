package outbox_test

import (
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"auth-api/internal/ledger"
	"auth-api/internal/outbox"
)

func TestEncode(t *testing.T) {
	at := time.Date(2026, 11, 5, 10, 0, 0, 19_000_000, time.FixedZone("WIB", 7*3600))
	e := ledger.Event{
		ID: "ev-1", StreamID: "tx-1", StreamVersion: 3, AccountID: "acct-1",
		Payload: ledger.TransactionSettled{JournalEntryIDs: []string{"a", "b"}, LedgerSequence: 42, BalanceAfter: 350000},
		TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", OccurredAt: at,
	}
	m, err := outbox.Encode(e, "ledger-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.EventID != "ev-1" || m.Key != "acct-1" {
		t.Fatalf("event id / key = %q / %q", m.EventID, m.Key)
	}

	var env outbox.Envelope
	if err := json.Unmarshal(m.Value, &env); err != nil {
		t.Fatal(err)
	}
	want := outbox.Envelope{
		EventID: "ev-1", EventType: "TransactionSettled", SchemaVersion: 1,
		StreamType: "transaction", StreamID: "tx-1", StreamVersion: 3,
		AccountID: "acct-1", LedgerID: "ledger-1", OccurredAt: at.UTC(), TraceID: e.TraceID,
	}
	got := env
	got.Payload = nil
	if !got.OccurredAt.Equal(want.OccurredAt) || got.OccurredAt.Location() != time.UTC {
		t.Fatalf("occurred_at = %v, want %v in UTC", got.OccurredAt, want.OccurredAt)
	}
	got.OccurredAt, want.OccurredAt = time.Time{}, time.Time{}
	if got.EventID != want.EventID || got.EventType != want.EventType || got.SchemaVersion != want.SchemaVersion ||
		got.StreamType != want.StreamType || got.StreamID != want.StreamID || got.StreamVersion != want.StreamVersion ||
		got.AccountID != want.AccountID || got.LedgerID != want.LedgerID || got.TraceID != want.TraceID {
		t.Fatalf("envelope = %+v, want %+v", got, want)
	}

	p, err := ledger.DecodePayload(ledger.EventType(env.EventType), env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if s := p.(ledger.TransactionSettled); s.LedgerSequence != 42 || s.BalanceAfter != 350000 {
		t.Fatalf("payload = %+v", s)
	}

	if !regexp.MustCompile(`^00-4bf92f3577b34da6a3ce929d0e0e4736-[0-9a-f]{16}-01$`).MatchString(m.Headers["traceparent"]) {
		t.Errorf("traceparent header = %q", m.Headers["traceparent"])
	}
	if m.Headers["event_type"] != "TransactionSettled" || m.Headers["schema_version"] != "1" {
		t.Errorf("headers = %v", m.Headers)
	}
}

func TestEncodeOmitsUnknownLedger(t *testing.T) {
	m, err := outbox.Encode(ledger.Event{ID: "e", StreamID: "a", StreamVersion: 1, AccountID: "a",
		Payload: ledger.AccountOpened{UserID: "u"}, TraceID: "4bf92f3577b34da6a3ce929d0e0e4736"}, "")
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	_ = json.Unmarshal(m.Value, &raw)
	if _, ok := raw["ledger_id"]; ok || raw["stream_type"] != "account" {
		t.Fatalf("envelope = %v", raw)
	}
}
