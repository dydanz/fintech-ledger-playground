package contract_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/moneyout/contract"
)

const transferID = "01a1d000-0000-7000-8000-000000000001"

// Field names follow the core's envelope (T§1.3), with stream_type "transfer".
func TestEnvelopeFieldNames(t *testing.T) {
	at := time.Date(2026, 11, 25, 2, 15, 0, 0, time.UTC)
	e, err := contract.NewEvent(contract.EventTransferReceived, contract.StreamTransfer, transferID, 1, at, "4bf92f3577b34da6a3ce929d0e0e4736",
		contract.JournalPayload{ToState: "RECEIVED"})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(e)
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"event_id", "event_type", "schema_version", "stream_type", "stream_id", "stream_version", "occurred_at", "trace_id", "payload"} {
		if _, ok := m[k]; !ok {
			t.Errorf("envelope has no %q: %s", k, b)
		}
	}
	if m["stream_type"] != "transfer" || m["stream_id"] != transferID || m["event_type"] != "TransferReceived" || m["schema_version"] != 1.0 {
		t.Fatalf("envelope = %s", b)
	}
	if m["occurred_at"] != "2026-11-25T02:15:00Z" {
		t.Fatalf("occurred_at = %v, want UTC RFC 3339", m["occurred_at"])
	}
	if len(e.EventID) != 36 {
		t.Fatalf("event_id = %q, want a UUID", e.EventID)
	}
}

// The outbox message is keyed by transfer_id and carries the core's headers.
func TestEncodeForOutbox(t *testing.T) {
	e, _ := contract.NewEvent(contract.EventTransferReceived, contract.StreamTransfer, transferID, 1, time.Now(), "4bf92f3577b34da6a3ce929d0e0e4736",
		contract.JournalPayload{ToState: "RECEIVED"})
	msg, err := contract.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	if msg.Key != transferID || msg.EventID != e.EventID {
		t.Fatalf("message key/id = %q/%q", msg.Key, msg.EventID)
	}
	if msg.Headers["event_type"] != "TransferReceived" || msg.Headers["schema_version"] != "1" ||
		!strings.HasPrefix(msg.Headers["traceparent"], "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
		t.Fatalf("headers = %v", msg.Headers)
	}
}

// A command envelope decodes into its payload; an unknown field in the payload
// is refused (the core's DisallowUnknownFields habit).
func TestDecodeTransferRequested(t *testing.T) {
	body := `{"event_id":"e1","event_type":"TransferRequested","schema_version":1,"stream_type":"transfer",
	 "stream_id":"` + transferID + `","stream_version":1,"occurred_at":"2026-11-25T02:15:00Z","trace_id":"t",
	 "payload":{"command":"do_transfer","transfer_id":"` + transferID + `","amount":{"value":"2500000.00","currency":"IDR"},
	 "beneficiary":{"bank_code":"014","account_no":"0201245681"},"requested_at":"2026-11-25T02:15:00Z","metadata":{"remark":"x"}}}`
	e, err := contract.DecodeEnvelope([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	var p contract.TransferRequested
	if err := e.DecodePayload(&p); err != nil {
		t.Fatal(err)
	}
	if p.TransferID != transferID || p.Amount.Value != "2500000.00" || p.Beneficiary.BankCode != "014" || string(p.Metadata) != `{"remark":"x"}` {
		t.Fatalf("payload = %+v", p)
	}
	bad := strings.Replace(body, `"command":"do_transfer"`, `"command":"do_transfer","rail":"RTGS"`, 1)
	e, _ = contract.DecodeEnvelope([]byte(bad))
	if err := e.DecodePayload(&p); err == nil {
		t.Fatal("a payload with an unknown field (a rail) was accepted: the core must not route")
	}
}

func TestTransferRequestedValidate(t *testing.T) {
	good := contract.TransferRequested{Command: "do_transfer", TransferID: transferID,
		Amount: contract.Amount{Value: "10.00", Currency: "IDR"}, Beneficiary: contract.Beneficiary{BankCode: "014", AccountNo: "0201245681"},
		RequestedAt: time.Now()}
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*contract.TransferRequested){
		"wrong command":   func(p *contract.TransferRequested) { p.Command = "do_refund" },
		"bad id":          func(p *contract.TransferRequested) { p.TransferID = "x" },
		"bad amount":      func(p *contract.TransferRequested) { p.Amount.Value = "1e3" },
		"bank code":       func(p *contract.TransferRequested) { p.Beneficiary.BankCode = "14" },
		"account no":      func(p *contract.TransferRequested) { p.Beneficiary.AccountNo = "02-01" },
		"no requested_at": func(p *contract.TransferRequested) { p.RequestedAt = time.Time{} },
	} {
		p := good
		mut(&p)
		if err := p.Validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
