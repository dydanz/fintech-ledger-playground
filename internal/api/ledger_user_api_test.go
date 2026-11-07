//go:build integration

package api_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"auth-api/internal/token"
)

// login stores an auth-api session for userID, as POST /auth/login does.
func (h *ledgerHarness) login(userID string) string {
	h.t.Helper()
	tok, err := token.Generate()
	if err != nil {
		h.t.Fatal(err)
	}
	if err := h.sess.Create(context.Background(), token.Hash(tok), userID, time.Hour); err != nil {
		h.t.Fatal(err)
	}
	return tok
}

func (h *ledgerHarness) newUser(email string) string {
	h.t.Helper()
	var id string
	if err := h.pool.QueryRow(context.Background(), `INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`, email).Scan(&id); err != nil {
		h.t.Fatal(err)
	}
	return id
}

func TestLedgerUserRoutes(t *testing.T) {
	h := newLedgerHarness(t)
	acct := h.openAccount()
	alice := h.login(h.userA)

	// Read-your-writes: the balance right after a 201 credit includes it.
	w := h.do("POST", "/internal/v1/accounts/"+acct+"/credits", payToken, "cr-u1", `{"amount":700,"currency":"IDR","reference":"ORD-U1"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("credit: %d %s", w.Code, w.Body)
	}
	txID := field(t, w, "transaction", "id").(string)
	w = h.do("GET", "/api/v1/ledger/account", alice, "", "")
	if w.Code != http.StatusOK || field(t, w, "account", "id") != acct {
		t.Fatalf("account: %d %s", w.Code, w.Body)
	}
	if l := field(t, w, "account", "ledgers").([]any)[0].(map[string]any); l["balance"] != float64(700) || l["currency"] != "IDR" {
		t.Fatalf("ledger = %v", l)
	}

	for i := range 2 {
		h.do("POST", "/internal/v1/accounts/"+acct+"/debits", payToken, "db-u"+string(rune('a'+i)), `{"amount":100,"currency":"IDR"}`)
	}
	w = h.do("GET", "/api/v1/ledger/transactions?limit=2", alice, "", "")
	if w.Code != http.StatusOK || len(field(t, w, "transactions").([]any)) != 2 {
		t.Fatalf("page 1: %d %s", w.Code, w.Body)
	}
	cursor, _ := field(t, w, "next_cursor").(string)
	w = h.do("GET", "/api/v1/ledger/transactions?limit=2&cursor="+cursor, alice, "", "")
	rows, _ := field(t, w, "transactions").([]any)
	if w.Code != http.StatusOK || len(rows) != 1 || rows[0].(map[string]any)["id"] != txID || field(t, w, "next_cursor") != nil {
		t.Fatalf("page 2: %d %s", w.Code, w.Body)
	}
	if w = h.do("GET", "/api/v1/ledger/transactions/"+txID, alice, "", ""); w.Code != http.StatusOK || field(t, w, "transaction", "amount") != float64(700) {
		t.Fatalf("own transaction: %d %s", w.Code, w.Body)
	}

	// Bob has an account of their own: alice's transaction is a 404 to him, never a 403.
	bobID := h.newUser("bob@x.io")
	if w = h.do("POST", "/internal/v1/accounts", opsToken, "open-bob", `{"user_id":"`+bobID+`"}`); w.Code != http.StatusCreated {
		t.Fatalf("open bob: %d %s", w.Code, w.Body)
	}
	bob := h.login(bobID)
	if w = h.do("GET", "/api/v1/ledger/transactions/"+txID, bob, "", ""); w.Code != http.StatusNotFound || field(t, w, "error", "code") != "transaction_not_found" {
		t.Errorf("bob reads alice's transaction: %d %s", w.Code, w.Body)
	}
	if w = h.do("GET", "/api/v1/ledger/transactions", bob, "", ""); w.Code != http.StatusOK || len(field(t, w, "transactions").([]any)) != 0 {
		t.Errorf("bob's list: %d %s", w.Code, w.Body)
	}
	// Carol is logged in but has no ledger account yet.
	carol := h.login(h.newUser("carol@x.io"))
	for path, code := range map[string]string{
		"/api/v1/ledger/transactions/" + txID: "transaction_not_found",
		"/api/v1/ledger/account":              "account_not_found",
		"/api/v1/ledger/transactions":         "account_not_found",
	} {
		if w = h.do("GET", path, carol, "", ""); w.Code != http.StatusNotFound || field(t, w, "error", "code") != code {
			t.Errorf("carol %s: %d %s", path, w.Code, w.Body)
		}
	}

	for _, q := range []string{"limit=101", "limit=0", "limit=x", "cursor=garbage!", "currency=EUR"} {
		if w = h.do("GET", "/api/v1/ledger/transactions?"+q, alice, "", ""); w.Code != http.StatusBadRequest || field(t, w, "error", "code") != "invalid_input" {
			t.Errorf("%s: %d %s", q, w.Code, w.Body)
		}
	}
}
