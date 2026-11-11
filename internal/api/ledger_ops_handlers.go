package api

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/service"
)

// Read-only ops routes (spec/2026-11-10 Web Console Design D§7.2). All need
// the read operation; registered only when LedgerDeps.Ops is set.

type opsHandlers struct {
	ops *service.OpsReader
	*LedgerHandlers
}

// queryLimit parses ?limit=; 0 when absent. ok is false after a 400.
func queryLimit(c *gin.Context) (int, bool) {
	v := c.Query("limit")
	if v == "" {
		return 0, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		abortError(c, http.StatusBadRequest, "invalid_input", "limit must be a positive integer")
		return 0, false
	}
	return n, true
}

// GET /internal/v1/accounts?user_id= (one account) or ?limit=&cursor= (a page)
func (h *opsHandlers) Accounts(c *gin.Context) {
	if c.Query("user_id") != "" {
		h.FindAccount(c)
		return
	}
	limit, ok := queryLimit(c)
	if !ok {
		return
	}
	page, err := h.ops.Accounts(c.Request.Context(), limit, c.Query("cursor"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

// GET /internal/v1/transactions?limit=&cursor=&status=&type=&since=
func (h *opsHandlers) Transactions(c *gin.Context) {
	limit, ok := queryLimit(c)
	if !ok {
		return
	}
	page, err := h.ops.Transactions(c.Request.Context(), service.OpsTransactionsQuery{Limit: limit, Cursor: c.Query("cursor"),
		Status: c.Query("status"), Type: c.Query("type"), Since: c.Query("since")})
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, page)
}

// GET /internal/v1/transactions/:transaction_id/detail
func (h *opsHandlers) TransactionDetail(c *gin.Context) {
	d, err := h.ops.TransactionDetail(c.Request.Context(), c.Param("transaction_id"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, d)
}

// GET /internal/v1/accounts/:account_id/events
func (h *opsHandlers) AccountEvents(c *gin.Context) {
	ev, err := h.ops.AccountEvents(c.Request.Context(), c.Param("account_id"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"events": ev})
}

// GET /internal/v1/stats?since=
func (h *opsHandlers) Stats(c *gin.Context) {
	s, err := h.ops.Stats(c.Request.Context(), c.Query("since"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, s)
}
