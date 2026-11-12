package api

import (
	"context"
	"log/slog"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/service"
)

type LedgerDeps struct {
	Logger         *slog.Logger
	Ledger         *service.LedgerService
	Ops            *service.OpsReader // read-only ops routes; nil leaves them out
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
		if d.Ops != nil {
			ops := &opsHandlers{ops: d.Ops, LedgerHandlers: h}
			internal.GET("/accounts", need("read"), ops.Accounts) // ?user_id= keeps FindAccount
			internal.GET("/accounts/:account_id/events", need("read"), ops.AccountEvents)
			internal.GET("/accounts/:account_id/transactions", need("read"), ops.AccountTransactions)
			internal.GET("/transactions", need("read"), ops.Transactions)
			internal.GET("/transactions/:transaction_id/detail", need("read"), ops.TransactionDetail)
			internal.GET("/stats", need("read"), ops.Stats)
		} else {
			internal.GET("/accounts", need("read"), h.FindAccount)
		}
		internal.GET("/accounts/:account_id", need("read"), h.GetAccount)
		internal.POST("/accounts/:account_id/ledgers", need("account_open"), h.OpenLedger)
		internal.POST("/accounts/:account_id/credits", need("credit"), h.Credit)
		internal.POST("/accounts/:account_id/debits", need("debit"), h.Debit)
		internal.POST("/accounts/:account_id/freeze", need("freeze"), h.Freeze)
		internal.POST("/accounts/:account_id/unfreeze", need("unfreeze"), h.Unfreeze)
		internal.GET("/transactions/:transaction_id", need("read"), h.GetTransaction)
		internal.POST("/transactions/:transaction_id/void", need("void"), h.Void)
	}

	user := r.Group("/api/v1/ledger", Auth(d.Sessions))
	{
		user.GET("/account", h.MyAccount)
		user.GET("/transactions", h.MyTransactions)
		user.GET("/transactions/:transaction_id", h.MyTransaction)
	}
	return r, nil
}
