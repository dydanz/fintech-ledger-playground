//go:build integration

package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"

	"auth-api/internal/outbox"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/session"
	"auth-api/internal/testutil/pgtest"
)

// flakyStore fails the next InTx call(s) chosen by failNth.
type flakyStore struct {
	service.LedgerStore
	mu     sync.Mutex
	calls  int
	failAt int
	err    error
}

func (f *flakyStore) InTx(ctx context.Context, d time.Duration, fn func(repository.LedgerTx) error) error {
	f.mu.Lock()
	f.calls++
	fail := f.calls == f.failAt
	f.mu.Unlock()
	if fail {
		return f.err
	}
	return f.LedgerStore.InTx(ctx, d, fn)
}

func (f *flakyStore) failNth(n int, err error) {
	f.mu.Lock()
	f.failAt, f.err = f.calls+n, err
	f.mu.Unlock()
}

type ledgerHarness struct {
	t     *testing.T
	srv   *gin.Engine
	logs  *bytes.Buffer
	store *flakyStore
	pool  *pgxpool.Pool
	sess  *session.RedisStore
	userA string
}

func newLedgerHarness(t *testing.T) *ledgerHarness {
	t.Helper()
	pool := pgtest.New(t)
	store := &flakyStore{LedgerStore: repository.NewLedgerRepo(pool, outbox.DefaultTopic, nil)}
	srv, logs, sessions := newLedgerRouter(t, store)
	var userA string
	if err := pool.QueryRow(context.Background(), `INSERT INTO users (email, password_hash) VALUES ('a@x.io', 'x') RETURNING id`).Scan(&userA); err != nil {
		t.Fatal(err)
	}
	// Mirror ledger-api startup.
	svc, _ := service.NewLedgerService(store, service.LedgerConfig{Currencies: []string{"IDR", "USD"}, MaxAmount: 1_000_000_000_000,
		ProcessTimeout: 2 * time.Second, IdempotencyTTL: time.Hour})
	if err := svc.EnsureSystemLedgers(context.Background()); err != nil {
		t.Fatal(err)
	}
	return &ledgerHarness{t: t, srv: srv, logs: logs, store: store, pool: pool, sess: sessions, userA: userA}
}

func (h *ledgerHarness) do(method, path, tok, key, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.srv.ServeHTTP(w, req)
	return w
}

func field(t *testing.T, w *httptest.ResponseRecorder, path ...string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(w.Body.Bytes(), &v); err != nil {
		t.Fatalf("body is not JSON: %s", w.Body)
	}
	for _, p := range path {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[p]
	}
	return v
}

func (h *ledgerHarness) openAccount() string {
	h.t.Helper()
	w := h.do("POST", "/internal/v1/accounts", opsToken, "open-a", `{"user_id":"`+h.userA+`"}`)
	if w.Code != http.StatusCreated {
		h.t.Fatalf("open account: %d %s", w.Code, w.Body)
	}
	return field(h.t, w, "account", "id").(string)
}

func TestLedgerAPIEndToEnd(t *testing.T) {
	h := newLedgerHarness(t)
	acct := h.openAccount()
	base := "/internal/v1/accounts/" + acct

	// Credit, then debit: 201 with Location and the transaction.
	w := h.do("POST", base+"/credits", payToken, "cr-1", `{"amount":1000,"currency":"IDR","reference":"ORD-1","metadata":{"channel":"qris"}}`)
	if w.Code != http.StatusCreated || field(t, w, "transaction", "status") != "SETTLED" || field(t, w, "transaction", "balance_after") != float64(1000) {
		t.Fatalf("credit: %d %s", w.Code, w.Body)
	}
	txID := field(t, w, "transaction", "id").(string)
	if w.Header().Get("Location") != "/internal/v1/transactions/"+txID || w.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("headers = %v", w.Header())
	}
	if tp := w.Header().Get("traceparent"); !strings.Contains(tp, field(t, w, "transaction", "trace_id").(string)) {
		t.Fatalf("response trace %q does not match stored trace_id", tp)
	}
	first := w.Body.String()

	// Replay: same bytes, flagged.
	w = h.do("POST", base+"/credits", payToken, "cr-1", `{"currency":"IDR","amount":1000,"metadata":{"channel":"qris"},"reference":"ORD-1"}`)
	if w.Code != http.StatusCreated || w.Body.String() != first || w.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d %s %v", w.Code, w.Body, w.Header())
	}

	w = h.do("POST", base+"/debits", payToken, "db-1", `{"amount":300,"currency":"IDR"}`)
	if w.Code != http.StatusCreated || field(t, w, "transaction", "balance_after") != float64(700) {
		t.Fatalf("debit: %d %s", w.Code, w.Body)
	}

	// Business failure: 422 envelope with the transaction id.
	w = h.do("POST", base+"/debits", payToken, "db-2", `{"amount":701,"currency":"IDR"}`)
	if w.Code != http.StatusUnprocessableEntity || field(t, w, "error", "code") != "insufficient_funds" ||
		field(t, w, "error", "transaction_id") == nil || field(t, w, "error", "request_id") == "" {
		t.Fatalf("NSF: %d %s", w.Code, w.Body)
	}

	// Reads.
	w = h.do("GET", base, payToken, "", "")
	if w.Code != http.StatusOK || field(t, w, "account", "ledgers").([]any)[0].(map[string]any)["balance"] != float64(700) {
		t.Fatalf("get account: %d %s", w.Code, w.Body)
	}
	w = h.do("GET", "/internal/v1/accounts?user_id="+h.userA, payToken, "", "")
	if w.Code != http.StatusOK || field(t, w, "account", "id") != acct {
		t.Fatalf("find account: %d %s", w.Code, w.Body)
	}
	w = h.do("GET", "/internal/v1/transactions/"+txID, payToken, "", "")
	if w.Code != http.StatusOK || field(t, w, "transaction", "ledger_sequence") != float64(1) {
		t.Fatalf("get transaction: %d %s", w.Code, w.Body)
	}

	// The access log carries the client; the raw key and reference never appear.
	for _, secret := range []string{"cr-1", "db-1", "ORD-1", "qris", payToken} {
		if strings.Contains(h.logs.String(), secret) {
			t.Errorf("logs contain %q", secret)
		}
	}
}

func TestLedgerAPIErrorCodes(t *testing.T) {
	h := newLedgerHarness(t)
	acct := h.openAccount()
	base := "/internal/v1/accounts/" + acct
	if w := h.do("POST", base+"/credits", payToken, "seed", `{"amount":5,"currency":"IDR"}`); w.Code != 201 {
		t.Fatalf("seed: %d %s", w.Code, w.Body)
	}

	cases := []struct {
		name, method, path, tok, key, body string
		status                             int
		code                               string
	}{
		{"missing Idempotency-Key", "POST", base + "/credits", payToken, "", `{"amount":1,"currency":"IDR"}`, 400, "idempotency_key_required"},
		{"invalid Idempotency-Key", "POST", base + "/credits", payToken, "bad key", `{"amount":1,"currency":"IDR"}`, 400, "idempotency_key_required"},
		{"unknown field", "POST", base + "/credits", payToken, "k1", `{"amount":1,"currency":"IDR","extra":true}`, 400, "invalid_input"},
		{"trailing data", "POST", base + "/credits", payToken, "k2", `{"amount":1,"currency":"IDR"}{}`, 400, "invalid_input"},
		{"amount as string", "POST", base + "/credits", payToken, "k3", `{"amount":"1","currency":"IDR"}`, 400, "invalid_input"},
		{"fractional amount", "POST", base + "/credits", payToken, "k4", `{"amount":1.5,"currency":"IDR"}`, 400, "invalid_input"},
		{"zero amount", "POST", base + "/credits", payToken, "k5", `{"amount":0,"currency":"IDR"}`, 400, "invalid_input"},
		{"unsupported currency", "POST", base + "/credits", payToken, "k6", `{"amount":1,"currency":"EUR"}`, 400, "invalid_input"},
		{"unknown account", "POST", "/internal/v1/accounts/0192f3c4-0000-7000-8000-00000000dead/debits", payToken, "k7", `{"amount":1,"currency":"IDR"}`, 404, "account_not_found"},
		{"ledger not opened", "POST", base + "/credits", payToken, "k8", `{"amount":1,"currency":"USD"}`, 404, "ledger_not_found"},
		{"key reused", "POST", base + "/credits", payToken, "seed", `{"amount":6,"currency":"IDR"}`, 422, "idempotency_key_reused"},
		{"unknown transaction", "GET", "/internal/v1/transactions/0192f3c4-0000-7000-8000-00000000dead", payToken, "", "", 404, "transaction_not_found"},
		{"find without user_id", "GET", "/internal/v1/accounts", payToken, "", "", 400, "invalid_input"},
		{"unknown user", "POST", "/internal/v1/accounts", opsToken, "o1", `{"user_id":"0192f3c4-0000-7000-8000-00000000dead"}`, 404, "user_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := h.do(tc.method, tc.path, tc.tok, tc.key, tc.body)
			if w.Code != tc.status || field(t, w, "error", "code") != tc.code || field(t, w, "error", "request_id") == "" {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
		})
	}

	// Second ledger in a currency → 409.
	if w := h.do("POST", base+"/ledgers", opsToken, "l1", `{"currency":"USD"}`); w.Code != 201 {
		t.Fatalf("open USD: %d %s", w.Code, w.Body)
	}
	if w := h.do("POST", base+"/ledgers", opsToken, "l2", `{"currency":"USD"}`); w.Code != 409 || field(t, w, "error", "code") != "ledger_exists" {
		t.Fatalf("second USD: %d %s", w.Code, w.Body)
	}
}

func TestLedgerAPIPendingAndUnavailable(t *testing.T) {
	h := newLedgerHarness(t)
	acct := h.openAccount()
	base := "/internal/v1/accounts/" + acct

	// Tx2 fails: 202 PENDING with Location; a retry gets 409 + Retry-After.
	h.store.failNth(2, fmt.Errorf("%w: lock wait", repository.ErrTimeout))
	w := h.do("POST", base+"/credits", payToken, "slow", `{"amount":5,"currency":"IDR"}`)
	if w.Code != http.StatusAccepted || field(t, w, "transaction", "status") != "PENDING" || !strings.HasPrefix(w.Header().Get("Location"), "/internal/v1/transactions/") {
		t.Fatalf("pending: %d %s %v", w.Code, w.Body, w.Header())
	}
	w = h.do("POST", base+"/credits", payToken, "slow", `{"amount":5,"currency":"IDR"}`)
	if w.Code != http.StatusConflict || field(t, w, "error", "code") != "request_in_progress" || w.Header().Get("Retry-After") != "1" {
		t.Fatalf("retry while pending: %d %s %v", w.Code, w.Body, w.Header())
	}

	// Tx1 fails: 503 ledger_unavailable, with the error in the access log.
	h.store.failNth(1, errors.New("connection reset by peer"))
	w = h.do("POST", base+"/debits", payToken, "down", `{"amount":1,"currency":"IDR"}`)
	if w.Code != http.StatusServiceUnavailable || field(t, w, "error", "code") != "ledger_unavailable" {
		t.Fatalf("Tx1 down: %d %s", w.Code, w.Body)
	}
	if !strings.Contains(h.logs.String(), "connection reset by peer") {
		t.Fatal("unexpected error not recorded in the access log")
	}
}
