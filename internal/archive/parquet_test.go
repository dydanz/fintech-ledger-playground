package archive_test

import (
	"reflect"
	"testing"
	"time"

	"auth-api/internal/archive"
)

func TestParquetRoundTrip(t *testing.T) {
	at := time.Date(2019, 3, 31, 23, 59, 59, 123456000, time.UTC)
	seq, bal := int64(7), int64(-250)
	journal := []archive.JournalRow{
		{ID: "a", TransactionID: "t1", LedgerID: "l1", Amount: 100, Currency: "IDR", LedgerSequence: &seq, BalanceAfter: &bal, TraceID: "x", CreatedAt: at},
		{ID: "b", TransactionID: "t1", LedgerID: "l2", Amount: -100, Currency: "IDR", TraceID: "x", CreatedAt: at}, // system line: no sequence
	}
	data, err := archive.Encode(journal)
	if err != nil {
		t.Fatal(err)
	}
	got, err := archive.Decode[archive.JournalRow](data)
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		got[i].CreatedAt = got[i].CreatedAt.UTC()
	}
	if !reflect.DeepEqual(got, journal) {
		t.Fatalf("journal round trip:\n got  %+v\n want %+v", got, journal)
	}

	events := []archive.EventRow{{EventID: "e1", StreamType: "transaction", StreamID: "t1", StreamVersion: 3, EventType: "TransactionSettled",
		AccountID: "a1", SchemaVersion: 1, Payload: `{"amount":100,"type":"CREDIT"}`, TraceID: "x", OccurredAt: at}}
	data, err = archive.Encode(events)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := archive.Decode[archive.EventRow](data)
	if err != nil {
		t.Fatal(err)
	}
	ev[0].OccurredAt = ev[0].OccurredAt.UTC()
	if !reflect.DeepEqual(ev, events) {
		t.Fatalf("events round trip:\n got  %+v\n want %+v", ev, events)
	}
	if archive.SHA256(data) == archive.SHA256(append([]byte{}, data[:len(data)-1]...)) {
		t.Fatal("checksum ignores content")
	}
}
