//go:build integration

package api_test

import (
	"net/http"
	"testing"
)

func TestLedgerAPIFreeze(t *testing.T) {
	h := newLedgerHarness(t)
	acct := h.openAccount()
	base := "/internal/v1/accounts/" + acct
	if w := h.do("POST", base+"/credits", payToken, "seed", `{"amount":10,"currency":"IDR"}`); w.Code != http.StatusCreated {
		t.Fatalf("seed: %d %s", w.Code, w.Body)
	}

	if w := h.do("POST", base+"/freeze", payToken, "f-pay", `{"reason":"x"}`); w.Code != http.StatusForbidden {
		t.Fatalf("payments freeze: %d %s", w.Code, w.Body)
	}
	w := h.do("POST", base+"/freeze", opsToken, "f-1", `{"reason":"velocity","rule":"topup_velocity"}`)
	if w.Code != http.StatusOK || field(t, w, "account", "status") != "FROZEN" || w.Header().Get("Location") != "" {
		t.Fatalf("freeze: %d %s %v", w.Code, w.Body, w.Header())
	}
	if w = h.do("POST", base+"/freeze", opsToken, "f-1", `{"rule":"topup_velocity","reason":"velocity"}`); w.Code != http.StatusOK || w.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %v", w.Code, w.Header())
	}
	if w = h.do("POST", base+"/debits", payToken, "d-1", `{"amount":1,"currency":"IDR"}`); w.Code != http.StatusUnprocessableEntity || field(t, w, "error", "code") != "account_frozen" {
		t.Fatalf("debit while frozen: %d %s", w.Code, w.Body)
	}

	for _, tc := range []struct {
		name, path, key, body string
		status                int
		code                  string
	}{
		{"unknown rule", base + "/freeze", "f-2", `{"reason":"x","rule":"teleport"}`, 400, "invalid_input"},
		{"missing reason", base + "/freeze", "f-3", `{}`, 400, "invalid_input"},
		{"unknown field", base + "/unfreeze", "u-0", `{"reason":"x","rule":"topup_velocity"}`, 400, "invalid_input"},
		{"unknown account", "/internal/v1/accounts/0192f3c4-0000-7000-8000-00000000dead/freeze", "f-4", `{"reason":"x"}`, 404, "account_not_found"},
		{"missing key", base + "/unfreeze", "", `{"reason":"x"}`, 400, "idempotency_key_required"},
	} {
		if w = h.do("POST", tc.path, opsToken, tc.key, tc.body); w.Code != tc.status || field(t, w, "error", "code") != tc.code {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
	}

	if w = h.do("POST", base+"/unfreeze", payToken, "u-pay", `{"reason":"x"}`); w.Code != http.StatusForbidden {
		t.Fatalf("payments unfreeze: %d %s", w.Code, w.Body)
	}
	if w = h.do("POST", base+"/unfreeze", opsToken, "u-1", `{"reason":"cleared"}`); w.Code != http.StatusOK || field(t, w, "account", "status") != "ACTIVE" {
		t.Fatalf("unfreeze: %d %s", w.Code, w.Body)
	}
	if w = h.do("POST", base+"/debits", payToken, "d-2", `{"amount":1,"currency":"IDR"}`); w.Code != http.StatusCreated {
		t.Fatalf("debit after unfreeze: %d %s", w.Code, w.Body)
	}
}
