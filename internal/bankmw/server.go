// Package bankmw is the runtime every bank middleware shares (spec/money-out
// TRD §3.5, §3.6, §4): the HTTP server that accepts only MOM-signed requests,
// the Bank interface each bank package implements, the Kafka emitter for
// asynchronous statuses, and masking for logs. It holds no bank concept.
package bankmw

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	coreapi "fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/signing"
)

// Bank is one bank's adapter: it maps the canonical request onto the bank's
// API and the bank's answers onto the canonical status (P§2.4).
type Bank interface {
	Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult
	InquireStatus(ctx context.Context, transferID, requestID string) contract.BankResult
}

// The MOM-facing routes (T§3.5).
const (
	PathTransfers = "/internal/v1/transfers"
	PathStatus    = "/internal/v1/transfers/status"
)

const maxBody = 64 << 10

type Deps struct {
	Name           string // e.g. bank-middleware-a, on every log line and event
	Logger         *slog.Logger
	Bank           Bank
	Verifier       *signing.Verifier
	Now            func() time.Time
	Ready          func(ctx context.Context) error
	TrustedProxies []string
	Routes         func(*gin.Engine) // the bank's own inbound routes (S:C webhooks), not MOM-signed
}

func NewRouter(d Deps) (*gin.Engine, error) {
	switch {
	case d.Name == "":
		return nil, errors.New("bankmw: no name")
	case d.Bank == nil:
		return nil, errors.New("bankmw: no bank")
	case d.Verifier == nil:
		return nil, errors.New("bankmw: no verifier")
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Ready == nil {
		d.Ready = func(context.Context) error { return nil }
	}
	r, err := coreapi.NewBaseEngine(d.Logger, d.TrustedProxies, d.Ready)
	if err != nil {
		return nil, err
	}
	s := &server{d: d}
	r.POST(PathTransfers, s.verified, s.transfer)
	r.POST(PathStatus, s.verified, s.status)
	if d.Routes != nil {
		d.Routes(r)
	}
	return r, nil
}

type server struct{ d Deps }

const ctxBody = "bankmw.body"

// verified admits only a request MOM signed, inside the window, with a
// nonce not seen before (T§3.5). Rejections never reach the bank and log
// bank.request_verify without the nonce or signature.
func (s *server) verified(c *gin.Context) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, maxBody+1))
	if err != nil || len(body) > maxBody {
		coreapi.AbortError(c, http.StatusRequestEntityTooLarge, "invalid_input", "request body too large")
		return
	}
	if err := s.d.Verifier.Verify(c.Request, body, s.d.Now()); err != nil {
		reason := "bad_signature"
		switch {
		case errors.Is(err, signing.ErrStale):
			reason = "stale"
		case errors.Is(err, signing.ErrReplay):
			reason = "replay"
		}
		s.reject(c, reason)
		return
	}
	c.Set(ctxBody, body)
	c.Next()
}

func (s *server) reject(c *gin.Context, reason string) {
	ctx := c.Request.Context()
	logger.FromContext(ctx).WarnContext(ctx, "request from MOM rejected", "event", "bank.request_verify", "outcome", "denied",
		"reason", reason, "bank_middleware", s.d.Name, "path", c.FullPath())
	coreapi.AbortError(c, http.StatusUnauthorized, "invalid_signature", "request signature rejected")
}

func decode(body []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

func (s *server) transfer(c *gin.Context) {
	var req contract.TransferRequest
	if err := decode(c.MustGet(ctxBody).([]byte), &req); err != nil {
		coreapi.AbortError(c, http.StatusBadRequest, "invalid_input", "not a transfer request")
		return
	}
	if err := req.Validate(); err != nil {
		coreapi.AbortError(c, http.StatusBadRequest, "invalid_input", strings.TrimPrefix(err.Error(), contract.ErrInvalidRequest.Error()+": "))
		return
	}
	// The body's own issued_at and nonce must be the signed ones (T§3.5).
	signedAt, _ := time.Parse(time.RFC3339, c.GetHeader(signing.HeaderTimestamp))
	if req.Nonce != c.GetHeader(signing.HeaderNonce) || !req.IssuedAt.Equal(signedAt) {
		s.reject(c, "body_mismatch")
		return
	}
	ctx := c.Request.Context()
	start := time.Now()
	res, ok := s.checked(ctx, "bank.transfer", s.d.Bank.Transfer(ctx, req), req.TransferID, req.RequestID)
	if !ok {
		c.JSON(http.StatusOK, res)
		return
	}
	logger.FromContext(ctx).InfoContext(ctx, "transfer handled", "event", "bank.transfer", "outcome", "success",
		"bank_middleware", s.d.Name, "transfer_id", req.TransferID, "request_id", req.RequestID, "attempt_no", req.AttemptNo,
		"rail", req.Rail, "amount", req.Amount.Value, "currency", req.Amount.Currency, "account", Mask(req.Beneficiary.AccountNo),
		"status", string(res.Status), "result_reason", string(res.Reason), "reached_bank", res.ReachedBank,
		"bank_raw_code", res.BankRawCode, "duration_ms", time.Since(start).Milliseconds())
	c.JSON(http.StatusOK, res)
}

func (s *server) status(c *gin.Context) {
	var q contract.StatusInquiry
	if err := decode(c.MustGet(ctxBody).([]byte), &q); err != nil || q.Validate() != nil {
		coreapi.AbortError(c, http.StatusBadRequest, "invalid_input", "not a status inquiry")
		return
	}
	ctx := c.Request.Context()
	start := time.Now()
	res, ok := s.checked(ctx, "bank.inquiry", s.d.Bank.InquireStatus(ctx, q.TransferID, q.RequestID), q.TransferID, q.RequestID)
	if !ok {
		c.JSON(http.StatusOK, res)
		return
	}
	logger.FromContext(ctx).InfoContext(ctx, "status inquiry handled", "event", "bank.inquiry", "outcome", "success",
		"bank_middleware", s.d.Name, "transfer_id", q.TransferID, "request_id", q.RequestID, "status", string(res.Status),
		"result_reason", string(res.Reason), "bank_raw_code", res.BankRawCode, "duration_ms", time.Since(start).Milliseconds())
	c.JSON(http.StatusOK, res)
}

// checked never passes on a result the bank package got wrong as final: it
// becomes UNKNOWN, which MOM resolves by inquiry (P§2.4).
// It logs event's failure itself and reports false then.
func (s *server) checked(ctx context.Context, event string, res contract.BankResult, transferID, requestID string) (contract.BankResult, bool) {
	if err := res.Validate(); err != nil {
		logger.FromContext(ctx).ErrorContext(ctx, "bank package returned an invalid result", "event", event,
			"outcome", "failure", "reason", "invalid_result", "bank_middleware", s.d.Name, "transfer_id", transferID,
			"request_id", requestID, "error", err)
		return contract.BankResult{Status: contract.StatusUnknown, ReachedBank: res.ReachedBank, BankReference: res.BankReference,
			BankRawCode: res.BankRawCode}, false
	}
	return res, true
}

// Mask keeps only an account number's last 4 digits (T§9).
func Mask(accountNo string) string {
	if len(accountNo) <= 4 {
		return strings.Repeat("*", len(accountNo))
	}
	return strings.Repeat("*", len(accountNo)-4) + accountNo[len(accountNo)-4:]
}
