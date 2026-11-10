package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/service"
)

// User routes (TRD §6.2). The account always comes from the session's
// user_id; no route takes an account_id.

// GET /api/v1/ledger/account
func (h *LedgerHandlers) MyAccount(c *gin.Context) {
	v, err := h.ledger.AccountByUser(c.Request.Context(), c.GetString(ctxUserID))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"account": v})
}

// GET /api/v1/ledger/transactions?currency=&limit=&cursor=
func (h *LedgerHandlers) MyTransactions(c *gin.Context) {
	q := service.TransactionPageQuery{Currency: c.Query("currency"), Cursor: c.Query("cursor")}
	if v := c.Query("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			abortError(c, http.StatusBadRequest, "invalid_input", "limit must be a positive integer")
			return
		}
		q.Limit = n
	}
	page, err := h.ledger.UserTransactions(c.Request.Context(), c.GetString(ctxUserID), q)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

// GET /api/v1/ledger/transactions/:transaction_id
func (h *LedgerHandlers) MyTransaction(c *gin.Context) {
	v, err := h.ledger.UserTransaction(c.Request.Context(), c.GetString(ctxUserID), c.Param("transaction_id"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"transaction": v})
}
