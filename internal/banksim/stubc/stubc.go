// Package stubc simulates Bank-Stub-C, the async multi-ACK bank
// (bank-api-stubs S:C): initiate, an ACK-1 webhook with an AAA challenge,
// aaa-verify, an ACK-2 webhook issuing a single-use execution token,
// execute, and a status poll. Webhooks go to the callback_base_url the
// caller gave. final_state values beyond SETTLED are T§11.3 (A)
// assumptions: AWAITING_ACK_1, AWAITING_AAA_VERIFY,
// VALIDATING_ACCOUNT_FOR_ACK_2, AWAITING_EXECUTE, PROCESSING, FAILED,
// EXPIRED.
package stubc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
)

var Steps = banksim.Steps{
	All:      []string{"initiate", "ack-1", "aaa-verify", "ack-2", "execute", "status"},
	Pending:  []string{"execute"},
	Webhooks: []string{"ack-1", "ack-2"},
}

// CanonicalFieldsOrder is what ACK-1 asks to be hashed, in order.
var CanonicalFieldsOrder = []string{"pg_trx_ref", "dest_bank_code", "bank_acct_no", "remit_amt", "aaa_challenge_nonce"}

type Config struct {
	APIKey       string        // the aggregator's bearer key
	AAASecret    string        // the aggregator's AAA HMAC secret
	Fee          int64         // fee_amt, debited on top of remit_amt (T§12.1 #7)
	AckTTL       time.Duration // ACK-1's expires_at
	ExecTTL      time.Duration // ACK-2's execution_deadline
	WebhookDelay time.Duration // before each webhook
	Now          func() time.Time
	Client       *http.Client
}

const (
	stAwaitAck1   = "AWAITING_ACK_1"
	stAwaitAAA    = "AWAITING_AAA_VERIFY"
	stValidating  = "VALIDATING_ACCOUNT_FOR_ACK_2"
	stAwaitExec   = "AWAITING_EXECUTE"
	stProcessing  = "PROCESSING"
	stSettled     = "SETTLED"
	stFailed      = "FAILED"
	stExpired     = "EXPIRED"
	timeLayoutWIB = "2006-01-02T15:04:05Z07:00"
)

type initiateReq struct {
	PgTrxRef        string `json:"pg_trx_ref"`
	SourceVaultID   string `json:"source_vault_id"`
	DestBankCode    string `json:"dest_bank_code"`
	BankAcctNo      string `json:"bank_acct_no"`
	RemitAmt        int64  `json:"-"`
	Ccy             string `json:"ccy"`
	CallbackBaseURL string `json:"callback_base_url"`
}

type session struct {
	req              initiateReq
	id, nonce        string
	state            string
	ack1Done, aaaOK  bool
	ack2Done         bool
	ackExpires       time.Time
	token            string
	tokenUsed        bool
	deadline         time.Time
	settlement       string
	polls            int
	failAfterPending bool
}

type bank struct {
	cfg Config
	sim *banksim.Sim

	mu       sync.Mutex
	sessions map[string]*session // by pg_trx_ref
	byID     map[string]*session // by bank_session_id
	seq      int
}

func Register(r *gin.Engine, sim *banksim.Sim, cfg Config) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 5 * time.Second}
	}
	b := &bank{cfg: cfg, sim: sim, sessions: map[string]*session{}, byID: map[string]*session{}}
	sim.OnReset(func() {
		b.mu.Lock()
		b.sessions, b.byID = map[string]*session{}, map[string]*session{}
		b.mu.Unlock()
	})
	g := r.Group("/v2/remit/async", b.auth)
	g.POST("/initiate", b.initiate)
	g.POST("/aaa-verify", b.aaaVerify)
	g.POST("/execute", b.execute)
	g.GET("/status/:ref", b.status)
}

// PayloadHash is the AAA digest: hex(SHA-256(values in order, joined by "|")).
func PayloadHash(values map[string]string, order []string) string {
	parts := make([]string, len(order))
	for i, f := range order {
		parts[i] = values[f]
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

// AAASignature is Base64(HMAC-SHA256(secret, hex digest)).
func AAASignature(secret, digest string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(digest))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}

func (b *bank) auth(c *gin.Context) {
	tok, ok := strings.CutPrefix(c.GetHeader("Authorization"), "Bearer ")
	if !ok || !hmac.Equal([]byte(tok), []byte(b.cfg.APIKey)) {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "UNAUTHORIZED"})
	}
}

func fail(c *gin.Context, status int, code string) {
	c.AbortWithStatusJSON(status, gin.H{"error": code})
}

var amountPattern = regexp.MustCompile(`^[1-9][0-9]{0,15}$`)

func (b *bank) initiate(c *gin.Context) {
	var raw struct {
		initiateReq
		RemitAmt json.RawMessage `json:"remit_amt"`
	}
	if err := json.NewDecoder(c.Request.Body).Decode(&raw); err != nil || !amountPattern.Match(raw.RemitAmt) {
		fail(c, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	req := raw.initiateReq
	req.RemitAmt, _ = strconv.ParseInt(string(raw.RemitAmt), 10, 64)
	if req.PgTrxRef == "" || req.DestBankCode == "" || req.BankAcctNo == "" || req.Ccy != "IDR" || req.CallbackBaseURL == "" {
		fail(c, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	b.sim.Count("initiate")
	b.mu.Lock()
	if s, ok := b.sessions[req.PgTrxRef]; ok {
		same := s.req == req
		out := gin.H{"ack_stage": "INIT_ACCEPTED", "bank_session_id": s.id, "pg_trx_ref": req.PgTrxRef, "workflow_state": b.state(s)}
		b.mu.Unlock()
		if !same {
			fail(c, http.StatusConflict, "DUPLICATE_PG_TRX_REF")
			return
		}
		c.JSON(http.StatusAccepted, out)
		return
	}
	b.mu.Unlock()
	f := b.sim.Take("initiate", req.BankAcctNo)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, orDefault(f.Code, "SIMULATED"))
		return
	}
	b.mu.Lock()
	b.seq++
	s := &session{req: req, id: fmt.Sprintf("BSESS-%07d", 9910000+b.seq), nonce: randHex(8), state: stAwaitAck1}
	b.sessions[req.PgTrxRef], b.byID[s.id] = s, s
	b.mu.Unlock()
	go b.sendAck1(s)
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"ack_stage": "INIT_ACCEPTED", "bank_session_id": s.id, "pg_trx_ref": req.PgTrxRef,
		"workflow_state": stAwaitAck1})
}

func (b *bank) sendAck1(s *session) {
	f := b.sim.Take("ack-1", s.req.BankAcctNo)
	b.pause(f)
	b.mu.Lock()
	now := b.cfg.Now()
	s.state, s.ackExpires = stAwaitAAA, now.Add(b.cfg.AckTTL)
	body := gin.H{"ack_code": "OK ACK-1", "bank_session_id": s.id, "pg_trx_ref": s.req.PgTrxRef, "aaa_challenge_nonce": s.nonce,
		"required_hash_alg": "SHA-256", "canonical_fields_order": CanonicalFieldsOrder, "expires_at": s.ackExpires.Format(timeLayoutWIB)}
	b.mu.Unlock()
	if f != nil && f.Action == banksim.ActionDropWebhook {
		return
	}
	if b.post(s.req.CallbackBaseURL+"/ack-1", "ACK-1", body) {
		b.mu.Lock()
		s.ack1Done = true
		b.mu.Unlock()
	}
}

func (b *bank) pause(f *banksim.Fault) {
	d := b.cfg.WebhookDelay
	if f != nil && f.Action == banksim.ActionDelay {
		d += time.Duration(f.DelayMS) * time.Millisecond
	}
	if d > 0 {
		time.Sleep(d)
	}
}

func (b *bank) post(url, event string, body any) bool {
	raw, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-BankC-Event", event)
	resp, err := b.cfg.Client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode/100 == 2
}

func (b *bank) aaaVerify(c *gin.Context) {
	var req struct {
		SessionID string `json:"bank_session_id"`
		PgTrxRef  string `json:"pg_trx_ref"`
		Nonce     string `json:"aaa_challenge_nonce"`
		Hash      string `json:"req_payload_hash"`
		Signature string `json:"aaa_hmac_signature"`
	}
	if json.NewDecoder(c.Request.Body).Decode(&req) != nil {
		fail(c, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	b.sim.Count("aaa-verify")
	b.mu.Lock()
	s, ok := b.byID[req.SessionID]
	if !ok || s.req.PgTrxRef != req.PgTrxRef {
		b.mu.Unlock()
		fail(c, http.StatusNotFound, "SESSION_NOT_FOUND")
		return
	}
	state := b.state(s)
	b.mu.Unlock()
	if state != stAwaitAAA {
		fail(c, http.StatusConflict, "INVALID_STAGE_"+state)
		return
	}
	f := b.sim.Take("aaa-verify", s.req.BankAcctNo)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, orDefault(f.Code, "SIMULATED"))
		return
	}
	want := PayloadHash(map[string]string{"pg_trx_ref": s.req.PgTrxRef, "dest_bank_code": s.req.DestBankCode,
		"bank_acct_no": s.req.BankAcctNo, "remit_amt": strconv.FormatInt(s.req.RemitAmt, 10), "aaa_challenge_nonce": s.nonce},
		CanonicalFieldsOrder)
	if !hmac.Equal([]byte(req.Nonce), []byte(s.nonce)) || !hmac.Equal([]byte(req.Hash), []byte(want)) ||
		!hmac.Equal([]byte(req.Signature), []byte(AAASignature(b.cfg.AAASecret, want))) {
		c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"aaa_status": "AAA_FAILED", "bank_session_id": s.id, "pg_trx_ref": s.req.PgTrxRef})
		return
	}
	b.mu.Lock()
	s.aaaOK, s.state = true, stValidating
	b.mu.Unlock()
	go b.sendAck2(s)
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"aaa_status": "AAA_PASSED", "bank_session_id": s.id, "pg_trx_ref": s.req.PgTrxRef,
		"workflow_state": stValidating})
}

// ack2Refusals are the codes a status fault at ack-2 takes (T§11.3 (A)).
var ack2Refusals = map[string]gin.H{
	"ACCT_INVALID":    {"request_valid": true, "acct_valid": false, "acct_status": "NOT_FOUND"},
	"ACCT_CLOSED":     {"request_valid": true, "acct_valid": false, "acct_status": "CLOSED"},
	"REQUEST_INVALID": {"request_valid": false, "acct_valid": true, "acct_status": "OPEN_ACTIVE"},
}

func (b *bank) sendAck2(s *session) {
	f := b.sim.Take("ack-2", s.req.BankAcctNo)
	b.pause(f)
	b.mu.Lock()
	body := gin.H{"ack_code": "OK ACK-2", "bank_session_id": s.id, "pg_trx_ref": s.req.PgTrxRef, "bank_acct_no": s.req.BankAcctNo,
		"remit_amt": s.req.RemitAmt, "fee_amt": b.cfg.Fee}
	if refusal, refused := ack2Refusals[faultCode(f)]; refused {
		for k, v := range refusal {
			body[k] = v
		}
		s.state = stFailed
	} else {
		s.token, s.deadline, s.state = "EXEC-TOK-"+strings.ToUpper(randHex(6)), b.cfg.Now().Add(b.cfg.ExecTTL), stAwaitExec
		body["request_valid"], body["acct_valid"], body["acct_status"] = true, true, "OPEN_ACTIVE"
		body["resolved_acct_name"] = "SIMULASI " + last4(s.req.BankAcctNo)
		body["execution_token"], body["execution_deadline"] = s.token, s.deadline.Format(timeLayoutWIB)
	}
	b.mu.Unlock()
	if f != nil && f.Action == banksim.ActionDropWebhook {
		return
	}
	if b.post(s.req.CallbackBaseURL+"/ack-2", "ACK-2", body) {
		b.mu.Lock()
		s.ack2Done = true
		b.mu.Unlock()
	}
}

func faultCode(f *banksim.Fault) string {
	if f == nil || f.Action != banksim.ActionStatus {
		return ""
	}
	return f.Code
}

func (b *bank) execute(c *gin.Context) {
	var req struct {
		SessionID string `json:"bank_session_id"`
		PgTrxRef  string `json:"pg_trx_ref"`
		Token     string `json:"execution_token"`
		Action    string `json:"execution_action"`
	}
	if json.NewDecoder(c.Request.Body).Decode(&req) != nil || req.Action != "EXECUTE" {
		fail(c, http.StatusBadRequest, "INVALID_REQUEST")
		return
	}
	b.sim.Count("execute")
	b.mu.Lock()
	s, ok := b.byID[req.SessionID]
	if !ok || s.req.PgTrxRef != req.PgTrxRef {
		b.mu.Unlock()
		fail(c, http.StatusNotFound, "SESSION_NOT_FOUND")
		return
	}
	switch {
	case s.token == "" || !hmac.Equal([]byte(req.Token), []byte(s.token)):
		b.mu.Unlock()
		fail(c, http.StatusUnauthorized, "INVALID_EXECUTION_TOKEN")
		return
	case s.tokenUsed:
		b.mu.Unlock()
		fail(c, http.StatusConflict, "TOKEN_ALREADY_USED")
		return
	case b.state(s) == stExpired:
		b.mu.Unlock()
		fail(c, http.StatusGone, "EXECUTION_EXPIRED")
		return
	}
	b.mu.Unlock()
	f := b.sim.Take("execute", s.req.BankAcctNo)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, orDefault(f.Code, "SIMULATED")) // not processed: the token stays unused
		return
	}
	b.mu.Lock()
	s.tokenUsed = true
	b.seq++
	s.settlement = fmt.Sprintf("CORE-C-%s-%06d", b.cfg.Now().Format("20060102"), 8800+b.seq)
	s.state = stSettled
	if f != nil && (f.Action == banksim.ActionPendingThenSuccess || f.Action == banksim.ActionPendingThenFail) {
		s.state, s.polls, s.failAfterPending = stProcessing, f.Polls, f.Action == banksim.ActionPendingThenFail
	}
	out := gin.H{"pg_trx_ref": s.req.PgTrxRef, "bank_session_id": s.id, "final_state": s.state}
	if s.state == stSettled {
		out["core_settlement_id"], out["debited_amt"], out["settled_at"] = s.settlement, s.req.RemitAmt+b.cfg.Fee, b.cfg.Now().Format(timeLayoutWIB)
	}
	b.mu.Unlock()
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	if out["final_state"] == stProcessing {
		c.JSON(http.StatusAccepted, out)
		return
	}
	c.JSON(http.StatusOK, out)
}

func (b *bank) status(c *gin.Context) {
	b.sim.Count("status")
	ref := c.Param("ref")
	b.mu.Lock()
	s, ok := b.sessions[ref]
	account := ""
	if ok {
		account = s.req.BankAcctNo
	}
	b.mu.Unlock()
	f := b.sim.Take("status", account)
	if f != nil && f.Action == banksim.ActionStatus {
		fail(c, f.HTTPStatus, orDefault(f.Code, "SIMULATED"))
		return
	}
	if !banksim.Hold(c.Request.Context(), f) {
		return
	}
	if !ok {
		fail(c, http.StatusNotFound, "NOT_FOUND")
		return
	}
	b.mu.Lock()
	if s.state == stProcessing {
		switch {
		case s.polls > 0:
			s.polls--
		case s.polls == 0 && s.failAfterPending:
			s.state = stFailed
		case s.polls == 0:
			s.state = stSettled
		}
	}
	out := gin.H{"pg_trx_ref": ref, "bank_session_id": s.id, "ack_1_completed": s.ack1Done, "aaa_verified": s.aaaOK,
		"ack_2_completed": s.ack2Done, "final_state": b.state(s)}
	if s.state == stSettled {
		out["core_settlement_id"] = s.settlement
	}
	b.mu.Unlock()
	c.JSON(http.StatusOK, out)
}

// state is s's state with lapsed deadlines applied. b.mu must be held.
func (b *bank) state(s *session) string {
	now := b.cfg.Now()
	switch {
	case s.state == stAwaitAAA && !s.ackExpires.IsZero() && now.After(s.ackExpires),
		s.state == stAwaitExec && !s.tokenUsed && now.After(s.deadline):
		s.state = stExpired
	}
	return s.state
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
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
