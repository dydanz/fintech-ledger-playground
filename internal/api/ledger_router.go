package api

import (
	"context"
	"log/slog"

	"github.com/gin-gonic/gin"

	"auth-api/internal/service"
)

type LedgerDeps struct {
	Logger         *slog.Logger
	Ledger         *service.LedgerService
	Clients        *Clients
	Sessions       SessionLookup                   // auth-api's Redis sessions, for /api/v1/ledger
	Ready          func(ctx context.Context) error // dependency check for /readyz
	TrustedProxies []string                        // nil: ignore X-Forwarded-For
}

// NewLedgerRouter serves ledger-api. /internal/v1 is for trusted services
// only: bind it to the internal network (mTLS in production, TRD §6.1).
// /api/v1/ledger is for end users, authenticated by auth-api's sessions.
func NewLedgerRouter(d LedgerDeps) (*gin.Engine, error) {
	r, err := newEngine(d.Logger, d.TrustedProxies, d.Ready)
	if err != nil {
		return nil, err
	}

	h := &LedgerHandlers{ledger: d.Ledger}
	need := d.Clients.Require

	internal := r.Group("/internal/v1")
	{
		internal.POST("/accounts", need("account_open"), h.OpenAccount)
		internal.GET("/accounts", need("read"), h.FindAccount)
		internal.GET("/accounts/:account_id", need("read"), h.GetAccount)
		internal.POST("/accounts/:account_id/ledgers", need("account_open"), h.OpenLedger)
		internal.POST("/accounts/:account_id/credits", need("credit"), h.Credit)
		internal.POST("/accounts/:account_id/debits", need("debit"), h.Debit)
		internal.GET("/transactions/:transaction_id", need("read"), h.GetTransaction)
	}

	user := r.Group("/api/v1/ledger", Auth(d.Sessions))
	{
		user.GET("/account", h.MyAccount)
		user.GET("/transactions", h.MyTransactions)
		user.GET("/transactions/:transaction_id", h.MyTransaction)
	}
	return r, nil
}
