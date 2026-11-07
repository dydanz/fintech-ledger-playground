package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"

	"auth-api/internal/service"
)

// LedgerHandlers serves ledger-api's internal routes (TRD §6.2).
type LedgerHandlers struct {
	ledger *service.LedgerService
}

type openAccountRequest struct {
	UserID string `json:"user_id"`
}

type openLedgerRequest struct {
	Currency       string `json:"currency"`
	OverdraftLimit int64  `json:"overdraft_limit"`
}

type moneyRequest struct {
	Amount       int64           `json:"amount"`
	Currency     string          `json:"currency"`
	Counterparty string          `json:"counterparty"`
	Reference    string          `json:"reference"`
	Metadata     json.RawMessage `json:"metadata"`
}

// ledgerRequest builds the service.Request from the authenticated client, the
// Idempotency-Key header and the request/trace ids set by RequestContext.
func ledgerRequest(c *gin.Context) service.Request {
	return service.Request{
		ClientID:       c.GetString(ctxClientID),
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
		RequestID:      c.GetString(ctxRequestID),
		TraceID:        c.GetString(ctxTraceID),
	}
}

// bindStrict decodes exactly one JSON object, rejecting unknown fields and
// trailing data (TRD §6.3).
func bindStrict(c *gin.Context, v any) bool {
	body, err := io.ReadAll(c.Request.Body)
	if err == nil {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.DisallowUnknownFields()
		if err = dec.Decode(v); err == nil && dec.More() {
			err = errors.New("trailing data after JSON object")
		}
	}
	if err != nil {
		abortError(c, http.StatusBadRequest, "invalid_input", "request body must be a single JSON object with known fields")
		return false
	}
	return true
}

// writeResponse sends a service response verbatim; it is also what a replay
// returns, byte for byte.
func writeResponse(c *gin.Context, r service.Response) {
	if r.Replayed {
		c.Header("Idempotent-Replayed", "true")
	}
	if r.Status == http.StatusCreated || r.Status == http.StatusAccepted {
		var b struct {
			Transaction struct {
				ID string `json:"id"`
			} `json:"transaction"`
		}
		if json.Unmarshal(r.Body, &b) == nil && b.Transaction.ID != "" {
			c.Header("Location", "/internal/v1/transactions/"+b.Transaction.ID)
		}
	}
	c.Data(r.Status, "application/json; charset=utf-8", r.Body)
}

// POST /internal/v1/accounts
func (h *LedgerHandlers) OpenAccount(c *gin.Context) {
	var req openAccountRequest
	if !bindStrict(c, &req) {
		return
	}
	r, err := h.ledger.OpenAccount(c.Request.Context(), service.OpenAccountCommand{Request: ledgerRequest(c), UserID: req.UserID})
	h.respond(c, r, err)
}

// POST /internal/v1/accounts/:account_id/ledgers
func (h *LedgerHandlers) OpenLedger(c *gin.Context) {
	var req openLedgerRequest
	if !bindStrict(c, &req) {
		return
	}
	r, err := h.ledger.OpenLedger(c.Request.Context(), service.OpenLedgerCommand{Request: ledgerRequest(c),
		AccountID: c.Param("account_id"), Currency: req.Currency, OverdraftLimit: req.OverdraftLimit})
	h.respond(c, r, err)
}

// POST /internal/v1/accounts/:account_id/credits
func (h *LedgerHandlers) Credit(c *gin.Context) { h.money(c, h.ledger.Credit) }

// POST /internal/v1/accounts/:account_id/debits
func (h *LedgerHandlers) Debit(c *gin.Context) { h.money(c, h.ledger.Debit) }

func (h *LedgerHandlers) money(c *gin.Context, op func(context.Context, service.MoneyCommand) (service.Response, error)) {
	var req moneyRequest
	if !bindStrict(c, &req) {
		return
	}
	r, err := op(c.Request.Context(), service.MoneyCommand{Request: ledgerRequest(c), AccountID: c.Param("account_id"),
		Amount: req.Amount, Currency: req.Currency, Counterparty: req.Counterparty, Reference: req.Reference, Metadata: req.Metadata})
	h.respond(c, r, err)
}

// GET /internal/v1/accounts/:account_id
func (h *LedgerHandlers) GetAccount(c *gin.Context) {
	v, err := h.ledger.Account(c.Request.Context(), c.Param("account_id"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"account": v})
}

// GET /internal/v1/accounts?user_id=
func (h *LedgerHandlers) FindAccount(c *gin.Context) {
	userID := c.Query("user_id")
	if userID == "" {
		abortError(c, http.StatusBadRequest, "invalid_input", "user_id query parameter is required")
		return
	}
	v, err := h.ledger.AccountByUser(c.Request.Context(), userID)
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"account": v})
}

// GET /internal/v1/transactions/:transaction_id
func (h *LedgerHandlers) GetTransaction(c *gin.Context) {
	v, err := h.ledger.Transaction(c.Request.Context(), c.Param("transaction_id"))
	if err != nil {
		h.fail(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"transaction": v})
}

func (h *LedgerHandlers) respond(c *gin.Context, r service.Response, err error) {
	if err != nil {
		h.fail(c, err)
		return
	}
	writeResponse(c, r)
}

// fail maps service errors to the TRD §6.5 codes. The service has already
// logged the event; unexpected errors go to the access log via c.Error.
func (h *LedgerHandlers) fail(c *gin.Context, err error) {
	var ve *service.ValidationError
	switch {
	case errors.Is(err, service.ErrIdempotencyKeyInvalid):
		abortError(c, http.StatusBadRequest, "idempotency_key_required", service.ErrIdempotencyKeyInvalid.Msg)
	case errors.As(err, &ve):
		abortError(c, http.StatusBadRequest, "invalid_input", ve.Msg)
	case errors.Is(err, service.ErrAccountNotFound):
		abortError(c, http.StatusNotFound, "account_not_found", "account not found")
	case errors.Is(err, service.ErrUserNotFound):
		abortError(c, http.StatusNotFound, "user_not_found", "user not found")
	case errors.Is(err, service.ErrTransactionNotFound):
		abortError(c, http.StatusNotFound, "transaction_not_found", "transaction not found")
	case errors.Is(err, service.ErrLedgerExists):
		abortError(c, http.StatusConflict, "ledger_exists", "account already has a ledger for this currency")
	case errors.Is(err, service.ErrRequestInProgress):
		c.Header("Retry-After", "1")
		abortError(c, http.StatusConflict, "request_in_progress", "a request with this Idempotency-Key is still being processed")
	case errors.Is(err, service.ErrIdempotencyKeyReused):
		abortError(c, http.StatusUnprocessableEntity, "idempotency_key_reused", "Idempotency-Key was already used with a different request")
	default:
		_ = c.Error(err)
		abortError(c, http.StatusServiceUnavailable, "ledger_unavailable", "ledger temporarily unavailable")
	}
}
