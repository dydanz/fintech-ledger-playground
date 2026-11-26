// Package stubb simulates Bank-Stub-B, the corporate host-to-host bank
// (bank-api-stubs S:B): OAuth Basic token, then X-BCA-* HMAC-SHA256-signed
// balance, beneficiary inquiry, single transfer and status. Bulk is out of
// scope (P§1.3). The error body and codes are the T§11.2 (A) assumptions.
package stubb

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/moneyout/contract"
)

var Steps = banksim.Steps{All: []string{"token", "balance", "inquiry", "transfer", "status"}, Pending: []string{"transfer"}}

type Config struct {
	ClientID, ClientSecret string // OAuth Basic
	APIKey, APISecret      string // X-BCA-Key and the HMAC-SHA256 key
	CorporateID            string
	TokenTTL               time.Duration
	Now                    func() time.Time
}

// Error codes (A): the contract shows only success bodies.
const (
	codeUnauthorized = "ESB-14-001"
	codeSignature    = "ESB-14-002"
	codeHeader       = "ESB-14-003"
	codeField        = "ESB-14-004"
	codeNotFound     = "ESB-82-404"
	codeDuplicate    = "ESB-82-409"
	codeInProgress   = "ESB-99-504"
)

const timestampLayout = "2006-01-02T15:04:05.000Z07:00"

type transfer struct {
	body       transferReq
	settlement string
	polls      int
	failure    string // FailureReason once rejected; "" settles
}

type bank struct {
	cfg Config
	sim *banksim.Sim

	mu        sync.Mutex
	tokens    map[string]time.Time
	transfers map[string]*transfer // by TransactionID
	seq       int
}

func Register(r *gin.Engine, sim *banksim.Sim, cfg Config) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	b := &bank{cfg: cfg, sim: sim, tokens: map[string]time.Time{}, transfers: map[string]*transfer{}}
	sim.OnReset(func() {
		b.mu.Lock()
		b.tokens, b.transfers = map[string]time.Time{}, map[string]*transfer{}
		b.mu.Unlock()
	})
	r.POST("/api/oauth/token", b.token)
	r.GET("/banking/v3/corporates/:corp/accounts/:acct", b.signed(b.balance))
	r.POST("/banking/v3/corporates/transfers/inquiry", b.signed(b.inquiry))
	r.POST("/banking/corporates/transfers", b.signed(b.transfer))
	r.POST("/banking/v3/corporates/transfers/status", b.signed(b.status))
}

func fail(c *gin.Context, status int, code, english string) {
	c.AbortWithStatusJSON(status, gin.H{"ErrorCode": code, "ErrorMessage": gin.H{"Indonesian": english, "English": english}})
}

func (b *bank) token(c *gin.Context) {
	id, secret, ok := c.Request.BasicAuth()
	if !ok || !hmac.Equal([]byte(id), []byte(b.cfg.ClientID)) || !hmac.Equal([]byte(secret), []byte(b.cfg.ClientSecret)) {
		fail(c, http.StatusUnauthorized, codeUnauthorized, "Unauthorized")
		return
	}
	if c.PostForm("grant_type") != "client_credentials" {
		fail(c, http.StatusBadRequest, codeField, "grant_type must be client_credentials")
		return
	}
	b.sim.Count("token")
	f := b.sim.Take("token", "")
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, orDefault(f.Code, codeUnauthorized), "Simulated")
		return
	}
	raw := make([]byte, 30)
	_, _ = rand.Read(raw)
	tok := base64.RawURLEncoding.EncodeToString(raw)
	b.mu.Lock()
	b.tokens[tok] = b.cfg.Now().Add(b.cfg.TokenTTL)
	b.mu.Unlock()
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"access_token": tok, "token_type": "bearer", "expires_in": int(b.cfg.TokenTTL.Seconds()),
		"scope": "resource.WRITE resource.READ"})
}

// Signature is S:B's X-BCA-Signature (T§12.1 #2, assumed BCA's):
// Base64(HMAC-SHA256(apiSecret, METHOD:RelativeUrl:AccessToken:
// lowercase(hex(SHA-256(minified body))):Timestamp)).
func Signature(secret, method, relativeURL, token string, minBody []byte, ts string) string {
	sum := sha256.Sum256(minBody)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(method + ":" + relativeURL + ":" + token + ":" + hex.EncodeToString(sum[:]) + ":" + ts))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (b *bank) signed(h func(*gin.Context, []byte)) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, 64<<10))
		minBody, merr := banksim.Minify(body)
		if err != nil || merr != nil {
			fail(c, http.StatusBadRequest, codeField, "Invalid JSON")
			return
		}
		ts := c.GetHeader("X-BCA-Timestamp")
		if _, err := time.Parse(timestampLayout, ts); err != nil || c.GetHeader("X-BCA-Signature") == "" {
			fail(c, http.StatusBadRequest, codeHeader, "Invalid X-BCA-Timestamp or missing X-BCA-Signature")
			return
		}
		tok, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		b.mu.Lock()
		exp, known := b.tokens[tok]
		b.mu.Unlock()
		if !ok || !known || !b.cfg.Now().Before(exp) {
			fail(c, http.StatusUnauthorized, codeUnauthorized, "Invalid or expired token")
			return
		}
		rel := c.Request.URL.Path
		if q := c.Request.URL.RawQuery; q != "" {
			rel += "?" + q
		}
		want := Signature(b.cfg.APISecret, c.Request.Method, rel, tok, minBody, ts)
		if c.GetHeader("X-BCA-Key") != b.cfg.APIKey || !hmac.Equal([]byte(want), []byte(c.GetHeader("X-BCA-Signature"))) {
			fail(c, http.StatusUnauthorized, codeSignature, "Invalid signature")
			return
		}
		h(c, minBody)
	}
}

// statusFault answers an injected status fault and reports whether it did.
func statusFault(c *gin.Context, f *banksim.Fault) bool {
	if f == nil || f.Action != banksim.ActionStatus {
		return false
	}
	fail(c, f.HTTPStatus, orDefault(f.Code, fmt.Sprintf("ESB-99-%03d", f.HTTPStatus)), "Simulated")
	return true
}

func (b *bank) balance(c *gin.Context, _ []byte) {
	b.sim.Count("balance")
	f := b.sim.Take("balance", "")
	if statusFault(c, f) || !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"AccountDetailDataSuccess": []gin.H{{"AccountNumber": c.Param("acct"), "Currency": "IDR",
		"Balance": "150000000.00", "AvailableBalance": "148500000.00", "FloatAmount": "1500000.00", "HoldAmount": "0.00"}},
		"AccountDetailDataFailed": []any{}})
}

func (b *bank) inquiry(c *gin.Context, body []byte) {
	var req struct{ CorpCode, InquiryRef, TargetBank, DestAcctNo string }
	if json.Unmarshal(body, &req) != nil || req.DestAcctNo == "" || req.TargetBank == "" {
		fail(c, http.StatusBadRequest, codeField, "DestAcctNo and TargetBank are required")
		return
	}
	b.sim.Count("inquiry")
	f := b.sim.Take("inquiry", req.DestAcctNo)
	status := "ACTIVE"
	if f != nil && f.Action == banksim.ActionStatus {
		if f.HTTPStatus != http.StatusOK {
			statusFault(c, f)
			return
		}
		status = orDefault(f.Code, "ACTIVE") // e.g. CLOSED, DORMANT
	}
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"InquiryRef": req.InquiryRef, "TargetBank": req.TargetBank, "DestAcctNo": req.DestAcctNo,
		"AccountHolderName": "SIMULASI " + last4(req.DestAcctNo), "AccountStatus": status, "CurrencyCode": "IDR"})
}

type transferReq struct {
	CorporateID, SourceAccountNumber, TransactionID, TransactionDate, ReferenceID string
	CurrencyCode, Amount, BeneficiaryAccountNumber, Remark1, Remark2              string
}

func (b *bank) transfer(c *gin.Context, body []byte) {
	var req transferReq
	if json.Unmarshal(body, &req) != nil || req.TransactionID == "" || req.BeneficiaryAccountNumber == "" ||
		req.CorporateID != b.cfg.CorporateID {
		fail(c, http.StatusBadRequest, codeField, "Invalid transfer request")
		return
	}
	if _, err := contract.ParseMoney(req.Amount, req.CurrencyCode); err != nil {
		fail(c, http.StatusBadRequest, codeField, "Invalid Amount")
		return
	}
	b.sim.Count("transfer")
	b.mu.Lock()
	if t, ok := b.transfers[req.TransactionID]; ok {
		b.mu.Unlock()
		if t.body != req {
			fail(c, http.StatusConflict, codeDuplicate, "Duplicate TransactionID")
			return
		}
		b.answerTransfer(c, t)
		return
	}
	b.mu.Unlock()
	f := b.sim.Take("transfer", req.BeneficiaryAccountNumber)
	if statusFault(c, f) {
		return
	}
	t := &transfer{body: req}
	if f != nil {
		switch f.Action {
		case banksim.ActionPendingThenSuccess:
			t.polls = f.Polls
		case banksim.ActionPendingThenFail:
			t.polls, t.failure = f.Polls, orDefault(f.Code, "REJECTED_BY_BANK")
		}
	}
	b.mu.Lock()
	b.seq++
	t.settlement = fmt.Sprintf("STLB%s%05d", b.cfg.Now().Format("20060102"), b.seq)
	b.transfers[req.TransactionID] = t
	b.mu.Unlock()
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	b.answerTransfer(c, t)
}

// answerTransfer: a pending transfer answers 504 "in progress" (A); the
// status route resolves it.
func (b *bank) answerTransfer(c *gin.Context, t *transfer) {
	b.mu.Lock()
	pending := t.polls != 0
	b.mu.Unlock()
	if pending {
		fail(c, http.StatusGatewayTimeout, codeInProgress, "Transaction in progress")
		return
	}
	c.JSON(http.StatusOK, gin.H{"TransactionID": t.body.TransactionID, "TransactionDate": t.body.TransactionDate,
		"ReferenceID": t.body.ReferenceID, "Status": "Success"})
}

func (b *bank) status(c *gin.Context, body []byte) {
	var req struct{ CorporateID, OriginalTransactionID, OriginalReferenceID, TransactionDate string }
	if json.Unmarshal(body, &req) != nil || req.OriginalTransactionID == "" {
		fail(c, http.StatusBadRequest, codeField, "OriginalTransactionID is required")
		return
	}
	b.sim.Count("status")
	b.mu.Lock()
	t, ok := b.transfers[req.OriginalTransactionID]
	account := ""
	if ok {
		account = t.body.BeneficiaryAccountNumber
	}
	b.mu.Unlock()
	f := b.sim.Take("status", account)
	if statusFault(c, f) || !banksim.Hold(c.Request.Context(), f) {
		return
	}
	if !ok {
		fail(c, http.StatusNotFound, codeNotFound, "Transaction not found")
		return
	}
	out := gin.H{"OriginalTransactionID": req.OriginalTransactionID, "BankSettlementRef": t.settlement, "FailureReason": nil}
	b.mu.Lock()
	switch {
	case t.polls > 0:
		t.polls--
		out["SettlementState"] = "PENDING"
	case t.polls < 0:
		out["SettlementState"] = "PENDING"
	case t.failure != "":
		out["SettlementState"], out["FailureReason"] = "REJECTED", t.failure
	default:
		out["SettlementState"], out["SettledTimestamp"] = "SETTLED", b.cfg.Now().Format(timestampLayout)
	}
	b.mu.Unlock()
	c.JSON(http.StatusOK, out)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func last4(s string) string {
	if len(s) <= 4 {
		return s
	}
	return s[len(s)-4:]
}
