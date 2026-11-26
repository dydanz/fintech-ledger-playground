// Package stubd simulates Bank-Stub-D, the multi-rail cash-management bank
// (bank-api-stubs S:D): kebab-case meta/data envelopes signed with
// HMAC-SHA512 over the minified body (T§12.1 #3), account validation,
// funding and rail check, single remittance and its status (T§12.1 #4:
// by pg-order-id). Bulk and OTP approval are out of scope (P§1.3). Codes
// beyond the contract's are T§11.4 (A) assumptions.
package stubd

import (
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
)

var Steps = banksim.Steps{All: []string{"acct-validate", "funding-check", "remit-single", "remit-status"}, Pending: []string{"remit-single"}}

type Config struct {
	ClientKey   string // X-Client-Key
	Secret      string // HMAC-SHA512 key
	FundingAcct string
	Liquidity   int64 // available-liquidity
	Now         func() time.Time
}

var (
	rails = []string{"BIFAST", "SKNBI", "RTGS"}
	fees  = map[string]int64{"BIFAST": 2500, "SKNBI": 2900, "RTGS": 30000}
	texts = map[string]string{"14": "ACCOUNT_NOT_FOUND"}
)

type remit struct {
	data    remitData
	railRef string
	polls   int
	final   string // tx-state once out of clearing
}

type remitData struct {
	OrderID     string `json:"pg-order-id"`
	FundingAcct string `json:"funding-acct-no"`
	DestBank    string `json:"dest-bank-id"`
	DestAcct    string `json:"dest-acct-no"`
	DestName    string `json:"dest-acct-name"`
	Rail        string `json:"rail-mode"`
	Amount      int64  `json:"-"`
	Currency    string `json:"currency-iso"`
	Memo        string `json:"payment-memo"`
}

type bank struct {
	cfg Config
	sim *banksim.Sim

	mu     sync.Mutex
	remits map[string]*remit // by pg-order-id
	seq    int
}

func Register(r *gin.Engine, sim *banksim.Sim, cfg Config) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	b := &bank{cfg: cfg, sim: sim, remits: map[string]*remit{}}
	sim.OnReset(func() {
		b.mu.Lock()
		b.remits = map[string]*remit{}
		b.mu.Unlock()
	})
	g := r.Group("/api/v2/disbursement")
	g.POST("/acct-validate", b.signed(b.acctValidate))
	g.POST("/funding-check", b.signed(b.fundingCheck))
	g.POST("/remit-single", b.signed(b.remitSingle))
	g.POST("/remit-status", b.signed(b.remitStatus))
}

// Signature is S:D's X-Signature-512: Base64(HMAC-SHA512(secret, minified body)).
func Signature(secret string, minBody []byte) string {
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(minBody)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func reply(c *gin.Context, status int, code, text string, data any) {
	out := gin.H{"meta": gin.H{"status-code": code, "status-text": text}}
	if data != nil {
		out["data"] = data
	}
	c.AbortWithStatusJSON(status, out)
}

func formatError(c *gin.Context) { reply(c, http.StatusBadRequest, "30", "FORMAT_ERROR", nil) }

func (b *bank) signed(h func(*gin.Context, json.RawMessage)) gin.HandlerFunc {
	return func(c *gin.Context) {
		body, err := io.ReadAll(io.LimitReader(c.Request.Body, 64<<10))
		minBody, merr := banksim.Minify(body)
		if err != nil || merr != nil {
			formatError(c)
			return
		}
		if c.GetHeader("X-Client-Key") != b.cfg.ClientKey ||
			!hmac.Equal([]byte(c.GetHeader("X-Signature-512")), []byte(Signature(b.cfg.Secret, minBody))) {
			reply(c, http.StatusUnauthorized, "41", "INVALID_SIGNATURE", nil)
			return
		}
		var env struct {
			Meta struct {
				TraceID string `json:"client-trace-id"`
				SentAt  string `json:"sent-at"`
			} `json:"meta"`
			Data json.RawMessage `json:"data"`
		}
		if json.Unmarshal(minBody, &env) != nil || env.Meta.TraceID == "" || env.Meta.SentAt == "" || len(env.Data) == 0 {
			formatError(c)
			return
		}
		h(c, env.Data)
	}
}

// statusFault answers a non-200 status fault; a 200 one is the handler's.
func statusFault(c *gin.Context, f *banksim.Fault) bool {
	if f == nil || f.Action != banksim.ActionStatus || f.HTTPStatus == http.StatusOK {
		return false
	}
	reply(c, f.HTTPStatus, orDefault(f.Code, "99"), "SIMULATED", nil)
	return true
}

func (b *bank) acctValidate(c *gin.Context, raw json.RawMessage) {
	var d struct {
		Bank string `json:"dest-bank-id"`
		Acct string `json:"dest-acct-no"`
		Rail string `json:"rail-mode"`
	}
	if json.Unmarshal(raw, &d) != nil || d.Bank == "" || d.Acct == "" {
		formatError(c)
		return
	}
	b.sim.Count("acct-validate")
	f := b.sim.Take("acct-validate", d.Acct)
	if statusFault(c, f) || !banksim.Hold(c.Request.Context(), f) {
		return
	}
	if f != nil && f.Action == banksim.ActionStatus {
		reply(c, http.StatusOK, f.Code, orDefault(texts[f.Code], "SIMULATED"), gin.H{"dest-bank-id": d.Bank, "dest-acct-no": d.Acct})
		return
	}
	reply(c, http.StatusOK, "00", "ACCOUNT_FOUND", gin.H{"dest-bank-id": d.Bank, "dest-acct-no": d.Acct,
		"registered-owner": "CV SIMULASI " + last4(d.Acct), "bifast-proxy-eligible": true})
}

var amountPattern = regexp.MustCompile(`^[1-9][0-9]{0,15}$`)

func amount(raw json.RawMessage) (int64, bool) {
	if !amountPattern.Match(raw) {
		return 0, false
	}
	v, err := strconv.ParseInt(string(raw), 10, 64)
	return v, err == nil
}

func (b *bank) fundingCheck(c *gin.Context, raw json.RawMessage) {
	var d struct {
		Funding string          `json:"funding-acct-no"`
		Rail    string          `json:"requested-rail"`
		Amount  json.RawMessage `json:"tx-amount"`
	}
	_ = json.Unmarshal(raw, &d)
	amt, ok := amount(d.Amount)
	if !ok || !slices.Contains(rails, d.Rail) || d.Funding != b.cfg.FundingAcct {
		formatError(c)
		return
	}
	b.sim.Count("funding-check")
	f := b.sim.Take("funding-check", "")
	if statusFault(c, f) || !banksim.Hold(c.Request.Context(), f) {
		return
	}
	code, text, liquidity, open := "00", "RAIL_AVAILABLE", b.cfg.Liquidity, true
	switch {
	case f != nil && f.Code == "WINDOW_CLOSED":
		code, text, open = "05", "RAIL_WINDOW_CLOSED", false
	case f != nil && f.Code == "LOW_LIQUIDITY":
		liquidity = 0
	}
	if liquidity < amt {
		code, text = "51", "INSUFFICIENT_LIQUIDITY"
	}
	reply(c, http.StatusOK, code, text, gin.H{"funding-acct-no": d.Funding, "available-liquidity": liquidity,
		"requested-rail": d.Rail, "rail-window-open": open, "estimated-fee": fees[d.Rail]})
}

func (b *bank) remitSingle(c *gin.Context, raw json.RawMessage) {
	var d struct {
		remitData
		Amount json.RawMessage `json:"tx-amount"`
	}
	_ = json.Unmarshal(raw, &d)
	data := d.remitData
	var ok bool
	data.Amount, ok = amount(d.Amount)
	if !ok || data.OrderID == "" || data.DestAcct == "" || data.DestBank == "" || data.Currency != "IDR" ||
		!slices.Contains(rails, data.Rail) || data.FundingAcct != b.cfg.FundingAcct {
		formatError(c)
		return
	}
	b.sim.Count("remit-single")
	b.mu.Lock()
	if r, exists := b.remits[data.OrderID]; exists {
		b.mu.Unlock()
		if r.data != data {
			reply(c, http.StatusConflict, "94", "DUPLICATE_ORDER", nil)
			return
		}
		submitted(c, r)
		return
	}
	b.mu.Unlock()
	f := b.sim.Take("remit-single", data.DestAcct)
	if statusFault(c, f) {
		return
	}
	r := &remit{data: data, polls: 1, final: "SETTLED"}
	if f != nil {
		switch f.Action {
		case banksim.ActionPendingThenSuccess:
			r.polls = f.Polls
		case banksim.ActionPendingThenFail:
			r.polls, r.final = f.Polls, "REJECTED"
			if f.Code == "RETURNED" {
				r.final = "RETURNED"
			}
		}
	}
	b.mu.Lock()
	b.seq++
	r.railRef = fmt.Sprintf("%s-%s-%04d", data.Rail, b.cfg.Now().Format("20060102"), 4400+b.seq)
	b.remits[data.OrderID] = r
	b.mu.Unlock()
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	submitted(c, r)
}

func submitted(c *gin.Context, r *remit) {
	reply(c, http.StatusOK, "00", "SUBMITTED", gin.H{"pg-order-id": r.data.OrderID, "bank-rail-ref": r.railRef,
		"rail-mode": r.data.Rail, "tx-state": "IN_CLEARING"})
}

func (b *bank) remitStatus(c *gin.Context, raw json.RawMessage) {
	var d struct {
		OrderID string `json:"pg-order-id"`
	}
	if json.Unmarshal(raw, &d) != nil || d.OrderID == "" {
		formatError(c) // batch-ticket (bulk) is out of scope
		return
	}
	b.sim.Count("remit-status")
	b.mu.Lock()
	r, ok := b.remits[d.OrderID]
	account := ""
	if ok {
		account = r.data.DestAcct
	}
	b.mu.Unlock()
	f := b.sim.Take("remit-status", account)
	if statusFault(c, f) || !banksim.Hold(c.Request.Context(), f) {
		return
	}
	if !ok {
		reply(c, http.StatusNotFound, "14", "ORDER_NOT_FOUND", nil)
		return
	}
	b.mu.Lock()
	state := "IN_CLEARING"
	switch {
	case r.polls > 0:
		r.polls--
	case r.polls == 0:
		state = r.final
	}
	b.mu.Unlock()
	reply(c, http.StatusOK, "00", "QUERY_OK", gin.H{"pg-order-id": d.OrderID, "rail-mode": r.data.Rail,
		"bank-rail-ref": r.railRef, "tx-state": state})
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
