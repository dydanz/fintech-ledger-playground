package idempotency_test

import (
	"encoding/json"
	"strings"
	"testing"

	"auth-api/internal/idempotency"
)

func TestValidKey(t *testing.T) {
	for k, want := range map[string]bool{
		"pay-7f3a9c-2026-11-05T10:00:00Z": true,
		"a":                               true,
		strings.Repeat("a", 255):          true,
		strings.Repeat("a", 256):          false,
		"":                                false,
		"has space":                       false,
		"slash/no":                        false,
	} {
		if got := idempotency.ValidKey(k); got != want {
			t.Errorf("ValidKey(%.20q) = %v, want %v", k, got, want)
		}
	}
}

func TestHashKey(t *testing.T) {
	h := idempotency.HashKey("k1")
	if len(h) != 64 || h != idempotency.HashKey("k1") || h == idempotency.HashKey("k2") {
		t.Fatalf("HashKey = %q", h)
	}
}

type fields struct {
	Amount   int64           `json:"amount"`
	Metadata json.RawMessage `json:"metadata,omitempty"`
}

func TestFingerprintIgnoresKeyOrderAndWhitespace(t *testing.T) {
	a, err := idempotency.Fingerprint("debit", "acct", fields{Amount: 5, Metadata: json.RawMessage(`{"b":1,"a":{"y":2,"x":1}}`)})
	if err != nil {
		t.Fatal(err)
	}
	b, _ := idempotency.Fingerprint("debit", "acct", fields{Amount: 5, Metadata: json.RawMessage(`{ "a": {"x":1, "y":2}, "b": 1 }`)})
	if a != b {
		t.Fatal("same request in a different key order got a different fingerprint")
	}
	for name, other := range map[string]func() (string, error){
		"amount": func() (string, error) {
			return idempotency.Fingerprint("debit", "acct", fields{Amount: 6, Metadata: json.RawMessage(`{"a":{"x":1,"y":2},"b":1}`)})
		},
		"operation": func() (string, error) {
			return idempotency.Fingerprint("credit", "acct", fields{Amount: 5, Metadata: json.RawMessage(`{"a":{"x":1,"y":2},"b":1}`)})
		},
		"target": func() (string, error) {
			return idempotency.Fingerprint("debit", "acct2", fields{Amount: 5, Metadata: json.RawMessage(`{"a":{"x":1,"y":2},"b":1}`)})
		},
		"metadata": func() (string, error) {
			return idempotency.Fingerprint("debit", "acct", fields{Amount: 5, Metadata: json.RawMessage(`{"a":{"x":1,"y":3},"b":1}`)})
		},
	} {
		if fp, _ := other(); fp == a {
			t.Errorf("changing %s did not change the fingerprint", name)
		}
	}
}

func TestFingerprintKeepsLargeNumbersExact(t *testing.T) {
	a, _ := idempotency.Fingerprint("x", "", json.RawMessage(`{"n":9007199254740993}`))
	b, _ := idempotency.Fingerprint("x", "", json.RawMessage(`{"n":9007199254740992}`))
	if a == b {
		t.Fatal("numbers beyond float64 precision collapsed to the same fingerprint")
	}
}
