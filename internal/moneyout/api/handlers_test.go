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

	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/logger"
	moapi "fintech-ledger-playground/internal/moneyout/api"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/transfer"
	"fintech-ledger-playground/internal/token"
)

const (
	id      = "01a1d000-0000-7000-8000-000000000001"
	opsTok  = "test-ops-token"
	authHdr = "Bearer " + opsTok
)

type fakeReader struct{}

func (fakeReader) GetTransfer(_ context.Context, tid string) (repository.Transfer, []repository.Attempt, error) {
	if tid != id {
		return repository.Transfer{}, nil, repository.ErrNotFound
	}
	at := time.Date(2026, 11, 25, 2, 15, 0, 0, time.UTC)
	return repository.Transfer{ID: id, Amount: 2500000, Currency: "IDR", BankCode: "014", AccountNo: "0201245681",
			State: transfer.Dispatching, Version: 3, CoreAck: "none", CoreTransactionID: "c0de", RequestedAt: at, CreatedAt: at, UpdatedAt: at,
			Routing: json.RawMessage(`{"candidates":[]}`)},
		[]repository.Attempt{{TransferID: id, AttemptNo: 1, RequestID: "r1", BankMiddleware: "bank-middleware-a", Rail: "BIFAST", Status: "DISPATCHING", CreatedAt: at, UpdatedAt: at}}, nil
}

func newRouterLog(t *testing.T, logs *bytes.Buffer) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r, err := moapi.NewRouter(moapi.Deps{Logger: logger.New(logs, slog.LevelDebug, "money-out-middleware"),
		Transfers: fakeReader{}, Ready: func(context.Context) error { return nil },
		Clients: []config.MoneyOutClient{{ID: "ops", TokenHash: token.Hash(opsTok)}}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newRouter(t *testing.T) http.Handler { return newRouterLog(t, &bytes.Buffer{}) }

// get calls path as the ops client.
func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	code, m, _ := getWith(t, h, path, authHdr)
	return code, m
}

func getWith(t *testing.T, h http.Handler, path, authorization string) (int, map[string]any, http.Header) {
	t.Helper()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	h.ServeHTTP(w, req)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m, w.Header()
}

// P§3.6: the current state, the attempts and the routing decision.
func TestGetTransfer(t *testing.T) {
	code, m := get(t, newRouter(t), "/transfers/"+id)
	if code != 200 {
		t.Fatalf("status %d: %v", code, m)
	}
	tr := m["transfer"].(map[string]any)
	if tr["transfer_id"] != id || tr["state"] != "DISPATCHING" || tr["version"] != 3.0 {
		t.Fatalf("transfer = %v", tr)
	}
	amt := tr["amount"].(map[string]any)
	if amt["value"] != "2500000.00" || amt["currency"] != "IDR" {
		t.Fatalf("amount = %v, want the wire format", amt)
	}
	if b := tr["beneficiary"].(map[string]any); b["bank_code"] != "014" || b["account_no"] != "0201245681" {
		t.Fatalf("beneficiary = %v", b)
	}
	if tr["routing"] == nil {
		t.Fatal("no routing decision")
	}
	atts := m["attempts"].([]any)
	if len(atts) != 1 || atts[0].(map[string]any)["bank_middleware"] != "bank-middleware-a" || atts[0].(map[string]any)["rail"] != "BIFAST" {
		t.Fatalf("attempts = %v", atts)
	}
}

func TestGetUnknownTransferIs404(t *testing.T) {
	for _, p := range []string{"/transfers/01a1d000-0000-7000-8000-0000000000ff", "/transfers/not-a-uuid"} {
		code, m := get(t, newRouter(t), p)
		if code != 404 || m["error"].(map[string]any)["code"] != "transfer_not_found" {
			t.Errorf("%s: %d %v", p, code, m)
		}
	}
}

// Probes stay open: the orchestrator has no token.
func TestHealthRoutesNeedNoToken(t *testing.T) {
	for _, p := range []string{"/healthz", "/readyz"} {
		if code, _, _ := getWith(t, newRouter(t), p, ""); code != 200 {
			t.Errorf("%s: %d", p, code)
		}
	}
}

// The ops API shows beneficiary account numbers, so it needs a known client
// token, and every rejection is logged without the token.
func TestTransferRouteRequiresClientToken(t *testing.T) {
	for _, c := range []struct{ name, authz, reason string }{
		{"no header", "", "missing_token"},
		{"not bearer", "Basic b3BzOng=", "missing_token"},
		{"empty bearer", "Bearer ", "missing_token"},
		{"unknown token", "Bearer some-other-token", "invalid_token"},
		{"too long", "Bearer " + strings.Repeat("x", 513), "missing_token"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var logs bytes.Buffer
			code, m, hdr := getWith(t, newRouterLog(t, &logs), "/transfers/"+id, c.authz)
			if code != 401 || m["error"].(map[string]any)["code"] != "unauthorized" {
				t.Fatalf("status %d: %v", code, m)
			}
			if !strings.HasPrefix(hdr.Get("WWW-Authenticate"), "Bearer") {
				t.Fatalf("WWW-Authenticate = %q", hdr.Get("WWW-Authenticate"))
			}
			if _, ok := m["transfer"]; ok {
				t.Fatal("transfer served without a valid token")
			}
			out := logs.String()
			if !strings.Contains(out, `"event":"transfer.client_auth"`) || !strings.Contains(out, `"reason":"`+c.reason+`"`) {
				t.Fatalf("log = %s", out)
			}
			if strings.Contains(out, "some-other-token") || strings.Contains(out, token.Hash("some-other-token")) {
				t.Fatal("token or its hash logged")
			}
		})
	}
	var logs bytes.Buffer
	if code, _, _ := getWith(t, newRouterLog(t, &logs), "/transfers/"+id, "bearer "+opsTok); code != 200 {
		t.Fatalf("known token (lower-case scheme): %d", code)
	}
	if !strings.Contains(logs.String(), `"client_id":"ops"`) {
		t.Fatalf("access log doesn't name the client: %s", logs.String())
	}
}

// A router built with no clients would serve nobody; refuse to build it.
func TestRouterRequiresClients(t *testing.T) {
	_, err := moapi.NewRouter(moapi.Deps{Logger: logger.New(&bytes.Buffer{}, slog.LevelDebug, "m"), Transfers: fakeReader{},
		Ready: func(context.Context) error { return nil }})
	if err == nil {
		t.Fatal("router built without clients")
	}
}
