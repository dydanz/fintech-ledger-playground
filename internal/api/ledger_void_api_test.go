//go:build integration

package api_test

import (
	"net/http"
	"testing"
)

func TestLedgerAPIVoid(t *testing.T) {
	h := newLedgerHarness(t)
	acct := h.openAccount()
	w := h.do("POST", "/internal/v1/accounts/"+acct+"/debits", payToken, "seed-d", `{"amount":1,"currency":"IDR"}`)
	if w.Code != http.StatusUnprocessableEntity { // empty ledger: a FAILED debit to void later
		t.Fatalf("seed debit: %d %s", w.Code, w.Body)
	}
	failedID := field(t, w, "error", "transaction_id").(string)
	w = h.do("POST", "/internal/v1/accounts/"+acct+"/credits", payToken, "seed-c", `{"amount":400,"currency":"IDR"}`)
	creditID := field(t, w, "transaction", "id").(string)
	path := "/internal/v1/transactions/" + creditID + "/void"

	// payments may credit and debit but not void.
	if w = h.do("POST", path, payToken, "v-pay", `{"reason":"x"}`); w.Code != http.StatusForbidden {
		t.Fatalf("payments void: %d %s", w.Code, w.Body)
	}

	w = h.do("POST", path, opsToken, "v-1", `{"reason":"fraudulent top-up"}`)
	voidID, _ := field(t, w, "transaction", "id").(string)
	if w.Code != http.StatusCreated || field(t, w, "transaction", "type") != "VOID" ||
		field(t, w, "transaction", "original_transaction_id") != creditID || field(t, w, "transaction", "balance_after") != float64(0) ||
		w.Header().Get("Location") != "/internal/v1/transactions/"+voidID {
		t.Fatalf("void: %d %s %v", w.Code, w.Body, w.Header())
	}
	if w = h.do("GET", "/internal/v1/transactions/"+creditID, opsToken, "", ""); field(t, w, "transaction", "status") != "VOIDED" {
		t.Fatalf("original: %s", w.Body)
	}
	if w = h.do("POST", path, opsToken, "v-1", `{"reason":"fraudulent top-up"}`); w.Code != http.StatusCreated || w.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %v", w.Code, w.Header())
	}

	for _, tc := range []struct {
		name, path, key, body string
		status                int
		code                  string
	}{
		{"second void", path, "v-2", `{"reason":"again"}`, 422, "not_voidable"},
		{"void of a void", "/internal/v1/transactions/" + voidID + "/void", "v-3", `{"reason":"x"}`, 422, "not_voidable"},
		{"void of a failed debit", "/internal/v1/transactions/" + failedID + "/void", "v-4", `{"reason":"x"}`, 422, "not_voidable"},
		{"unknown transaction", "/internal/v1/transactions/0192f3c4-0000-7000-8000-00000000dead/void", "v-5", `{"reason":"x"}`, 404, "transaction_not_found"},
		{"missing reason", path, "v-6", `{}`, 400, "invalid_input"},
		{"unknown field", path, "v-7", `{"reason":"x","amount":1}`, 400, "invalid_input"},
		{"missing key", path, "", `{"reason":"x"}`, 400, "idempotency_key_required"},
	} {
		if w = h.do("POST", tc.path, opsToken, tc.key, tc.body); w.Code != tc.status || field(t, w, "error", "code") != tc.code {
			t.Errorf("%s: %d %s", tc.name, w.Code, w.Body)
		}
	}
}
