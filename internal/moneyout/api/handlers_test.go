package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/logger"
	moapi "fintech-ledger-playground/internal/moneyout/api"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

const id = "01a1d000-0000-7000-8000-000000000001"

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

func newRouter(t *testing.T) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r, err := moapi.NewRouter(moapi.Deps{Logger: logger.New(&bytes.Buffer{}, slog.LevelDebug, "money-out-middleware"),
		Transfers: fakeReader{}, Ready: func(context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func get(t *testing.T, h http.Handler, path string) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
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

func TestHealthRoutes(t *testing.T) {
	for _, p := range []string{"/healthz", "/readyz"} {
		if code, _ := get(t, newRouter(t), p); code != 200 {
			t.Errorf("%s: %d", p, code)
		}
	}
}
