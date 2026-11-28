package api_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/logger"
	moapi "fintech-ledger-playground/internal/moneyout/api"
	"fintech-ledger-playground/internal/moneyout/breaker"
	"fintech-ledger-playground/internal/token"
)

type fakeBreakers []breaker.Row

func (f fakeBreakers) List(context.Context) ([]breaker.Row, error) { return f, nil }

// GET /bank-middlewares/health: every (bank middleware, rail) the routing
// knows, with its breaker; one never tripped is CLOSED (P§3.6).
func TestBankMiddlewareHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	opened := time.Date(2026, 11, 28, 3, 0, 0, 0, time.UTC)
	r, err := moapi.NewRouter(moapi.Deps{Logger: logger.New(&bytes.Buffer{}, slog.LevelDebug, "money-out-middleware"),
		Transfers: fakeReader{}, Ready: func(context.Context) error { return nil },
		Clients:  []config.MoneyOutClient{{ID: "ops", TokenHash: token.Hash(opsTok)}},
		Breakers: fakeBreakers{{BankMiddleware: "bank-middleware-a", Rail: "BIFAST", State: breaker.Open, Consecutive: 0, Calls: 0, OpenedAt: opened, Version: 1}},
		Pairs: func() [][2]string {
			return [][2]string{{"bank-middleware-a", "BIFAST"}, {"bank-middleware-a", "RTOL"}, {"bank-middleware-d", "RTGS"}}
		}})
	if err != nil {
		t.Fatal(err)
	}
	code, body := get(t, r, "/bank-middlewares/health")
	if code != http.StatusOK {
		t.Fatalf("%d %v", code, body)
	}
	list := body["bank_middlewares"].([]any)
	if len(list) != 3 {
		t.Fatalf("%d entries: %v", len(list), list)
	}
	first := list[0].(map[string]any)
	if first["bank_middleware"] != "bank-middleware-a" || first["rail"] != "BIFAST" || first["state"] != "OPEN" || first["opened_at"] != "2026-11-28T03:00:00Z" {
		t.Fatalf("first = %v", first)
	}
	for _, e := range list[1:] {
		if e.(map[string]any)["state"] != "CLOSED" {
			t.Fatalf("an untripped breaker isn't CLOSED: %v", e)
		}
	}
	if code, _, _ := getWith(t, r, "/bank-middlewares/health", ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
}
