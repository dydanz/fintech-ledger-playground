package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"auth-api/internal/api"
	"auth-api/internal/config"
	"auth-api/internal/logger"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/token"
)

// emptyStore has no accounts; enough to get past ClientAuth to a 404.
type emptyStore struct{}

func (emptyStore) InTx(context.Context, time.Duration, func(repository.LedgerTx) error) error {
	return nil
}
func (emptyStore) GetAccount(context.Context, string) (repository.AccountRecord, error) {
	return repository.AccountRecord{}, repository.ErrNotFound
}
func (emptyStore) GetAccountByUserID(context.Context, string) (repository.AccountRecord, error) {
	return repository.AccountRecord{}, repository.ErrNotFound
}
func (emptyStore) ListUserLedgers(context.Context, string) ([]repository.LedgerRecord, error) {
	return nil, nil
}
func (emptyStore) GetTransaction(context.Context, string) (repository.TransactionRecord, error) {
	return repository.TransactionRecord{}, repository.ErrNotFound
}

const (
	payToken = "dev-payments-token-0123456789"
	opsToken = "dev-ops-token-0123456789"
)

func testClients() []config.ClientCredential {
	return []config.ClientCredential{
		{ID: "payments", TokenHash: token.Hash(payToken), Operations: []string{"credit", "debit", "read"}},
		{ID: "ops", TokenHash: token.Hash(opsToken), Operations: []string{"account_open", "read"}},
	}
}

func newLedgerRouter(t *testing.T, store service.LedgerStore) (*gin.Engine, *bytes.Buffer) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	svc, err := service.NewLedgerService(store, service.LedgerConfig{Currencies: []string{"IDR", "USD"}, MaxAmount: 1_000_000_000_000,
		ProcessTimeout: 2 * time.Second, IdempotencyTTL: 24 * time.Hour, OverdraftClients: []string{"ops"}})
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	r, err := api.NewLedgerRouter(api.LedgerDeps{Logger: logger.New(logs, slog.LevelDebug, "ledger-api"), Ledger: svc,
		Clients: api.NewClients(testClients()), Ready: func(context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r, logs
}

func logRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", line)
		}
		out = append(out, m)
	}
	return out
}

func findEvent(t *testing.T, buf *bytes.Buffer, event string) map[string]any {
	t.Helper()
	for _, r := range logRecords(t, buf) {
		if r["event"] == event {
			return r
		}
	}
	t.Fatalf("no %q record in:\n%s", event, buf)
	return nil
}

func TestClientAuth(t *testing.T) {
	cases := []struct {
		name, method, path, auth string
		status                   int
		code, reason             string
	}{
		{"missing token", "GET", "/internal/v1/accounts/x", "", 401, "unauthorized", "missing_token"},
		{"not bearer", "GET", "/internal/v1/accounts/x", "Basic abc", 401, "unauthorized", "missing_token"},
		{"unknown token", "GET", "/internal/v1/accounts/x", "Bearer nope", 401, "unauthorized", "invalid_token"},
		{"forbidden operation", "POST", "/internal/v1/accounts", "Bearer " + payToken, 403, "forbidden", "forbidden_operation"},
		{"oversized token", "GET", "/internal/v1/accounts/x", "Bearer " + strings.Repeat("a", 600), 401, "unauthorized", "missing_token"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, logs := newLedgerRouter(t, emptyStore{})
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(`{}`))
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.status || !strings.Contains(w.Body.String(), `"code":"`+tc.code+`"`) {
				t.Fatalf("%d %s", w.Code, w.Body)
			}
			if tc.status == 401 && !strings.Contains(w.Header().Get("WWW-Authenticate"), "ledger-api") {
				t.Errorf("WWW-Authenticate = %q", w.Header().Get("WWW-Authenticate"))
			}
			rec := findEvent(t, logs, "ledger.client_auth")
			if rec["outcome"] != "denied" || rec["reason"] != tc.reason || rec["level"] != "warn" {
				t.Errorf("record = %v", rec)
			}
			if tc.reason == "forbidden_operation" && rec["client_id"] != "payments" {
				t.Errorf("client_id = %v", rec["client_id"])
			}
			if strings.Contains(logs.String(), payToken) || strings.Contains(logs.String(), token.Hash(payToken)) {
				t.Error("log contains the token or its hash")
			}
		})
	}
}

func TestClientAuthAdmitsAndAccessLogNamesTheClient(t *testing.T) {
	r, logs := newLedgerRouter(t, emptyStore{})
	req := httptest.NewRequest("GET", "/internal/v1/accounts/0192f3c4-0000-7000-8000-000000000001", nil)
	req.Header.Set("Authorization", "Bearer "+payToken)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "account_not_found") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var access map[string]any
	for _, rec := range logRecords(t, logs) {
		if rec["message"] == "http_request" {
			access = rec
		}
	}
	if access["client_id"] != "payments" || access["route"] != "/internal/v1/accounts/:account_id" {
		t.Fatalf("access log = %v", access)
	}
	for _, rec := range logRecords(t, logs) {
		if rec["event"] == "ledger.client_auth" {
			t.Fatalf("admitted request logged a rejection: %v", rec)
		}
	}
}
