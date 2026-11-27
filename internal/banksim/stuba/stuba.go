// Package stuba simulates Bank-Stub-A, the BI-SNAP bank (bank-api-stubs
// S:A): RSA B2B token, then HMAC-SHA512-signed account inquiry, interbank
// transfer and status inquiry. Failure codes are the T§11.1 (A) rows; a
// test injects them as faults.
package stuba

import (
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// Steps are S:A's endpoints, by fault step name.
var Steps = banksim.Steps{All: []string{"token", "inquiry", "transfer", "status"}, Pending: []string{"transfer"}}

// Config is the bank's view of its one partner (the aggregator).
type Config struct {
	ClientKey        string         // X-CLIENT-KEY
	PartnerPublicKey *rsa.PublicKey // verifies the token request's SHA256withRSA
	ClientSecret     string         // HMAC-SHA512 key for transactional calls
	PartnerID        string         // X-PARTNER-ID
	TokenTTL         time.Duration
	Now              func() time.Time
}

// SNAP service codes.
const (
	svcToken    = "73"
	svcInquiry  = "16"
	svcTransfer = "18"
	svcStatus   = "36"
)

type transfer struct {
	body      transferReq
	reference string
	polls     int    // pending replies left; -1 forever
	final     string // latestTransactionStatus once no longer pending
}

type bank struct {
	cfg Config
	sim *banksim.Sim

	mu        sync.Mutex
	tokens    map[string]time.Time
	transfers map[string]*transfer // by partnerReferenceNo
	seq       int
}

// Register adds S:A's routes to r.
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
	r.POST("/v1.0/access-token/b2b", b.token)
	r.POST("/interbank/snap/v1.0/account-inquiry-external", b.signed(svcInquiry, b.inquiry))
	r.POST("/interbank/snap/v1.0/transfer-interbank", b.signed(svcTransfer, b.transfer))
	r.POST("/v1.0/transfer/status", b.signed(svcStatus, b.status))
}

func fail(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"responseCode": code, "responseMessage": msg})
}

// faultCode is the injected code, or the SNAP code for the HTTP status.
func faultCode(f *banksim.Fault, svc string) string {
	if f.Code != "" {
		return f.Code
	}
	return strconv.Itoa(f.HTTPStatus) + svc + "00"
}

func (b *bank) token(c *gin.Context) {
	ts := c.GetHeader("X-TIMESTAMP")
	sig, err := base64.StdEncoding.DecodeString(c.GetHeader("X-SIGNATURE"))
	sum := sha256.Sum256([]byte(c.GetHeader("X-CLIENT-KEY") + "|" + ts))
	if err != nil || c.GetHeader("X-CLIENT-KEY") != b.cfg.ClientKey || ts == "" ||
		rsa.VerifyPKCS1v15(b.cfg.PartnerPublicKey, crypto.SHA256, sum[:], sig) != nil {
		fail(c, http.StatusUnauthorized, "401"+svcToken+"00", "Unauthorized. Signature")
		return
	}
	var req struct {
		GrantType string `json:"grantType"`
	}
	if json.NewDecoder(c.Request.Body).Decode(&req) != nil || req.GrantType != "client_credentials" {
		fail(c, http.StatusBadRequest, "400"+svcToken+"02", "Invalid Mandatory Field grantType")
		return
	}
	b.sim.Count("token")
	f := b.sim.Take("token", "")
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, faultCode(f, svcToken), "Simulated")
		return
	}
	raw := make([]byte, 24)
	_, _ = rand.Read(raw)
	tok := "stubA." + hex.EncodeToString(raw)
	b.mu.Lock()
	b.tokens[tok] = b.cfg.Now().Add(b.cfg.TokenTTL)
	b.mu.Unlock()
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"responseCode": "2007300", "responseMessage": "Successful", "accessToken": tok,
		"tokenType": "Bearer", "expiresIn": strconv.Itoa(int(b.cfg.TokenTTL.Seconds()))})
}

// Signature is S:A's X-SIGNATURE: Base64(HMAC-SHA512(secret, stringToSign)),
// where stringToSign is the SNAP symmetric string
// METHOD:path:token:lowercase(hex(SHA-256(minified body))):timestamp.
func Signature(secret, method, path, token string, minBody []byte, ts string) string {
	sum := sha256.Sum256(minBody)
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write([]byte(method + ":" + path + ":" + token + ":" + hex.EncodeToString(sum[:]) + ":" + ts))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

// signed checks the seven SNAP headers, the token and the signature before h
// sees the minified body.
func (b *bank) signed(svc string, h func(*gin.Context, []byte)) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, 64<<10))
		minBody, merr := banksim.Minify(body)
		if err != nil || merr != nil {
			fail(c, http.StatusBadRequest, "400"+svc+"01", "Invalid Field Format body")
			return
		}
		ts := c.GetHeader("X-TIMESTAMP")
		for _, hd := range []string{"Authorization", "X-TIMESTAMP", "X-SIGNATURE", "X-PARTNER-ID", "CHANNEL-ID", "X-EXTERNAL-ID"} {
			if c.GetHeader(hd) == "" {
				fail(c, http.StatusBadRequest, "400"+svc+"02", "Invalid Mandatory Field "+hd)
				return
			}
		}
		tok, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
		b.mu.Lock()
		exp, known := b.tokens[tok]
		b.mu.Unlock()
		if !ok || !known || !b.cfg.Now().Before(exp) {
			fail(c, http.StatusUnauthorized, "401"+svc+"01", "Invalid Token (B2B)")
			return
		}
		want := Signature(b.cfg.ClientSecret, c.Request.Method, c.Request.URL.Path, tok, minBody, ts)
		if c.GetHeader("X-PARTNER-ID") != b.cfg.PartnerID || !hmac.Equal([]byte(want), []byte(c.GetHeader("X-SIGNATURE"))) {
			fail(c, http.StatusUnauthorized, "401"+svc+"00", "Unauthorized. Signature")
			return
		}
		h(c, minBody)
	}
}

func (b *bank) inquiry(c *gin.Context, body []byte) {
	var req struct {
		BankCode       string          `json:"beneficiaryAccountCode"`
		AccountNo      string          `json:"beneficiaryAccountNo"`
		AdditionalInfo json.RawMessage `json:"additionalInfo"`
	}
	if json.Unmarshal(body, &req) != nil || req.AccountNo == "" || req.BankCode == "" {
		fail(c, http.StatusBadRequest, "400"+svcInquiry+"02", "Invalid Mandatory Field beneficiaryAccountNo")
		return
	}
	b.sim.Count("inquiry")
	f := b.sim.Take("inquiry", req.AccountNo)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, faultCode(f, svcInquiry), "Simulated")
		return
	}
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"responseCode": "2001600", "responseMessage": "Successful",
		"referenceNo": fmt.Sprintf("%020d", time.Now().UnixNano()%1e15), "beneficiaryAccountName": "PT SIMULASI " + last4(req.AccountNo),
		"beneficiaryAccountNo": req.AccountNo, "beneficiaryBankCode": req.BankCode, "currency": "IDR", "additionalInfo": req.AdditionalInfo})
}

type transferReq struct {
	PartnerReferenceNo   string          `json:"partnerReferenceNo"`
	Amount               contract.Amount `json:"amount"`
	BeneficiaryAccountNo string          `json:"beneficiaryAccountNo"`
	BeneficiaryBankCode  string          `json:"beneficiaryBankCode"`
	SourceAccountNo      string          `json:"sourceAccountNo"`
	AdditionalInfo       json.RawMessage `json:"additionalInfo"`
}

func (b *bank) transfer(c *gin.Context, body []byte) {
	var req transferReq
	if json.Unmarshal(body, &req) != nil {
		fail(c, http.StatusBadRequest, "400"+svcTransfer+"01", "Invalid Field Format amount")
		return
	}
	if _, err := req.Amount.Money(); err != nil {
		fail(c, http.StatusBadRequest, "400"+svcTransfer+"01", "Invalid Field Format amount")
		return
	}
	if req.PartnerReferenceNo == "" || req.BeneficiaryAccountNo == "" || req.BeneficiaryBankCode == "" {
		fail(c, http.StatusBadRequest, "400"+svcTransfer+"02", "Invalid Mandatory Field")
		return
	}
	b.sim.Count("transfer")
	b.mu.Lock()
	if t, ok := b.transfers[req.PartnerReferenceNo]; ok {
		b.mu.Unlock()
		if t.body.Amount != req.Amount || t.body.BeneficiaryAccountNo != req.BeneficiaryAccountNo {
			fail(c, http.StatusConflict, "409"+svcTransfer+"00", "Conflict: partnerReferenceNo already used")
			return
		}
		b.answerTransfer(c, t)
		return
	}
	b.mu.Unlock()
	f := b.sim.Take("transfer", req.BeneficiaryAccountNo)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, faultCode(f, svcTransfer), "Simulated")
		return
	}
	t := &transfer{body: req, final: "00"}
	if f != nil {
		switch f.Action {
		case banksim.ActionPendingThenSuccess:
			t.polls = f.Polls
		case banksim.ActionPendingThenFail:
			t.polls, t.final = f.Polls, "06"
		}
	}
	b.mu.Lock()
	b.seq++
	t.reference = strconv.Itoa(9981000 + b.seq)
	b.transfers[req.PartnerReferenceNo] = t
	b.mu.Unlock()
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	b.answerTransfer(c, t)
}

func (b *bank) answerTransfer(c *gin.Context, t *transfer) {
	b.mu.Lock()
	pending := t.polls != 0
	b.mu.Unlock()
	status, code, msg := http.StatusOK, "2001800", "Successful"
	if pending {
		status, code, msg = http.StatusAccepted, "2021800", "Request In Progress"
	}
	c.JSON(status, gin.H{"responseCode": code, "responseMessage": msg, "referenceNo": t.reference,
		"partnerReferenceNo": t.body.PartnerReferenceNo, "amount": t.body.Amount, "beneficiaryAccountNo": t.body.BeneficiaryAccountNo,
		"beneficiaryBankCode": t.body.BeneficiaryBankCode, "sourceAccountNo": t.body.SourceAccountNo, "additionalInfo": t.body.AdditionalInfo})
}

var statusDesc = map[string]string{"00": "Success", "03": "Pending", "06": "Failed", "07": "Not Found"}

func (b *bank) status(c *gin.Context, body []byte) {
	var req struct {
		Ref         string `json:"originalPartnerReferenceNo"`
		ServiceCode string `json:"serviceCode"`
	}
	if json.Unmarshal(body, &req) != nil || req.Ref == "" {
		fail(c, http.StatusBadRequest, "400"+svcStatus+"02", "Invalid Mandatory Field originalPartnerReferenceNo")
		return
	}
	b.sim.Count("status")
	b.mu.Lock()
	t, ok := b.transfers[req.Ref]
	account := ""
	if ok {
		account = t.body.BeneficiaryAccountNo
	}
	b.mu.Unlock()
	f := b.sim.Take("status", account)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, faultCode(f, svcStatus), "Simulated")
		return
	}
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	out := gin.H{"responseCode": "2003600", "responseMessage": "Successful", "originalPartnerReferenceNo": req.Ref, "serviceCode": svcTransfer}
	st := "07"
	if ok {
		b.mu.Lock()
		switch {
		case t.polls > 0:
			t.polls--
			st = "03"
		case t.polls < 0:
			st = "03"
		default:
			st = t.final
		}
		b.mu.Unlock()
		out["originalReferenceNo"], out["amount"] = t.reference, t.body.Amount
	}
	out["latestTransactionStatus"], out["transactionStatusDesc"] = st, statusDesc[st]
	c.JSON(http.StatusOK, out)
}

func last4(s string) string {
	if len(s) <= 4 {
		return s
	}
	return s[len(s)-4:]
}
