// Package stubc is bank-middleware-c's adapter for Bank-Stub-C, the async
// multi-ACK bank (bank-api-stubs S:C). The whole handshake runs inside the
// BMW (T§11.3): initiate (ACCEPTED), the ACK-1 webhook (PROCESSING), the AAA
// hash and HMAC to aaa-verify, the ACK-2 webhook with the single-use
// execution token, and execute. A status poll runs alongside, as the
// fallback for a dropped webhook. MOM sees ACCEPTED → PROCESSING → terminal
// on money-out.bank-events.v1. See README.md.
package stubc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

type Config struct {
	BaseURL         string
	APIKey          string // the bearer key
	AAASecret       string // the AAA HMAC-SHA256 key
	SourceVault     string // source_vault_id (T§12.1 #9)
	CallbackBaseURL string // where the bank sends ACK-1 and ACK-2: this BMW's WebhookPath
	PollInterval    time.Duration
	PollFor         time.Duration // a session gives up after this; MOM's inquiry resolves the attempt
	Now             func() time.Time
}

// WebhookPath is where the BMW listens for the bank's ACK-1 and ACK-2.
const WebhookPath = "/callbacks/bank-c"

// Rails: S:C has no rail field; BIFAST is the T§11.4 draft (A).
var Rails = []string{contract.RailBIFAST}

// hashFields are the fields the BMW can put in the AAA digest.
var hashFields = []string{"pg_trx_ref", "dest_bank_code", "bank_acct_no", "remit_amt", "aaa_challenge_nonce"}

const (
	reportTries = 3
	// failedPolls: while a webhook is still expected, a polled FAILED counts
	// only on this many polls in a row, so a refusing ACK-2 in flight gives
	// the precise reason.
	failedPolls = 3
	readyWait   = 5 * time.Second // a webhook waits this long for its initiate answer
	maxWebhook  = 64 << 10
)

var wib = time.FixedZone("WIB", 7*60*60)

var (
	failed  = bankmw.Failed
	unknown = bankmw.Unknown
)

// Reporter publishes an asynchronous status: bankmw.Emitter.
type Reporter interface {
	Report(ctx context.Context, transferID, requestID string, res contract.BankResult) error
}

type Bank struct {
	cfg    Config
	client *http.Client
	rep    Reporter

	ctx    context.Context // the sessions'; canceled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	sessions map[string]*session // by pg_trx_ref (= request_id)
	closed   bool
}

func New(cfg Config, client *http.Client, rep Reporter, log *slog.Logger) (*Bank, error) {
	switch {
	case cfg.BaseURL == "" || cfg.APIKey == "" || cfg.AAASecret == "" || cfg.SourceVault == "" || cfg.CallbackBaseURL == "":
		return nil, errors.New("stubc: base URL, API key, AAA secret, source vault and callback base URL are required")
	case rep == nil:
		return nil, errors.New("stubc: a reporter is required: bank C settles asynchronously (KAFKA_BROKERS)")
	case cfg.PollInterval <= 0 || cfg.PollFor <= 0:
		return nil, errors.New("stubc: poll interval and poll duration must be positive")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	ctx, cancel := context.WithCancel(logger.WithContext(context.Background(), log))
	return &Bank{cfg: cfg, client: client, rep: rep, ctx: ctx, cancel: cancel, sessions: map[string]*session{}}, nil
}

// Close stops every session and waits for them. A stopped session never
// executes; MOM's status inquiry resolves its attempt (T§3.6).
func (b *Bank) Close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cancel()
	b.wg.Wait()
}

type ack1 struct {
	nonce string
	order []string
}

type ack2 struct {
	requestValid, acctValid bool
	acctStatus, token       string
	deadline                time.Time
}

// session is one attempt's handshake, in this process only.
type session struct {
	req    contract.TransferRequest
	amount int64
	ready  chan struct{} // closed once initiate has answered
	bankID string        // bank_session_id; "" if initiate failed

	mu               sync.Mutex
	gotAck1, gotAck2 bool
	ack1             chan ack1
	ack2             chan ack2
}

func (s *session) ref() string { return s.req.RequestID }

// Transfer starts the handshake: initiate, then the session runs on its own.
func (b *Bank) Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult {
	if !slices.Contains(Rails, req.Rail) {
		return failed(contract.ReasonRailUnavailable, true, false, "")
	}
	money, err := req.Amount.Money()
	if err != nil || money.Currency != "IDR" {
		return failed(contract.ReasonOther, false, false, "")
	}
	b.mu.Lock()
	if b.closed { // no session could run: nothing is initiated
		b.mu.Unlock()
		return failed(contract.ReasonRailUnavailable, true, false, "")
	}
	if s, ok := b.sessions[req.RequestID]; ok { // the same attempt again
		b.mu.Unlock()
		select {
		case <-s.ready:
		case <-ctx.Done():
			return unknown("")
		}
		if s.bankID == "" {
			return unknown("")
		}
		return accepted(s.bankID)
	}
	// The session exists before initiate: the bank may send ACK-1 before
	// its 202 reaches us.
	s := &session{req: req, amount: money.Amount, ready: make(chan struct{}), ack1: make(chan ack1, 1), ack2: make(chan ack2, 1)}
	b.sessions[req.RequestID] = s
	b.wg.Add(1)
	b.mu.Unlock()

	a := b.call(ctx, "initiate", http.MethodPost, "/v2/remit/async/initiate", req, map[string]any{
		"pg_trx_ref": req.RequestID, "source_vault_id": b.cfg.SourceVault, "dest_bank_code": req.Beneficiary.BankCode,
		"bank_acct_no": req.Beneficiary.AccountNo, "remit_amt": money.Amount, "ccy": money.Currency,
		"callback_base_url": b.cfg.CallbackBaseURL})
	var out struct {
		Stage     string `json:"ack_stage"`
		SessionID string `json:"bank_session_id"`
	}
	if a.Status != http.StatusAccepted || json.Unmarshal(a.Body, &out) != nil || out.Stage != "INIT_ACCEPTED" || out.SessionID == "" {
		// No session here: a late ACK-1 is refused, so this attempt can
		// never be executed. The bank expires it.
		b.drop(s)
		close(s.ready)
		b.wg.Done()
		switch {
		case a.NotReached:
			return failed(contract.ReasonRailUnavailable, true, false, "")
		case a.Status == http.StatusUnauthorized:
			return failed(contract.ReasonAuthError, true, false, a.Code)
		}
		return unknown(a.Code) // a timeout, 5xx, duplicate or anything unmapped
	}
	s.bankID = out.SessionID
	close(s.ready)
	go func() {
		defer b.wg.Done()
		defer b.drop(s)
		b.run(s)
	}()
	return accepted(out.SessionID)
}

func accepted(bankID string) contract.BankResult {
	return contract.BankResult{Status: contract.StatusAccepted, ReachedBank: true, BankReference: bankID, BankRawCode: "INIT_ACCEPTED"}
}

func (b *Bank) drop(s *session) {
	b.mu.Lock()
	if b.sessions[s.ref()] == s {
		delete(b.sessions, s.ref())
	}
	b.mu.Unlock()
}

// run is one session's worker. It sends aaa-verify on ACK-1 and execute on
// ACK-2 (at most once), and polls status every PollInterval alongside, so a
// dropped webhook still ends the session: the bank reports EXPIRED once its
// deadline passes without an execute, which is FAILED OTHER. It reports
// PROCESSING once and the terminal status once, and logs one bank.session.
func (b *Bank) run(s *session) {
	ctx := b.ctx
	start := time.Now()
	deadline := start.Add(b.cfg.PollFor)
	tick := time.NewTicker(b.cfg.PollInterval)
	defer tick.Stop()
	const (
		awaitAck1 = iota
		awaitAck2
		awaitFinal // execute was sent
	)
	phase, polls, failedInARow := awaitAck1, 0, 0
	processing, unknownSent := false, false
	end := func(outcome, reason string, status contract.Status) {
		attrs := []any{"event", "bank.session", "outcome", outcome, "transfer_id", s.req.TransferID, "request_id", s.ref(),
			"polls", polls, "executed", phase == awaitFinal, "duration_ms", time.Since(start).Milliseconds()}
		log := logger.FromContext(ctx)
		if outcome == "success" {
			log.InfoContext(ctx, "bank C session ended in a terminal state", append(attrs, "status", string(status))...)
			return
		}
		log.WarnContext(ctx, "bank C session stopped; MOM's inquiry resolves the attempt", append(attrs, "reason", reason)...)
	}
	report := func(res contract.BankResult) bool { return b.rep.Report(ctx, s.req.TransferID, s.ref(), res) == nil }
	inProgress := func(res contract.BankResult) {
		if !processing {
			processing = report(res)
		}
	}
	// finish reports the terminal status, retrying at the poll interval.
	finish := func(res contract.BankResult) {
		for try := 1; ; try++ {
			if report(res) {
				end("success", "", res.Status)
				return
			}
			if try == reportTries {
				end("failure", "report_failed", "")
				return
			}
			select {
			case <-ctx.Done():
				end("failure", "shutdown", "")
				return
			case <-tick.C:
			}
		}
	}
	for {
		var a1 <-chan ack1
		var a2 <-chan ack2
		switch phase {
		case awaitAck1:
			a1 = s.ack1
		case awaitAck2:
			a2 = s.ack2
		}
		select {
		case <-ctx.Done():
			end("failure", "shutdown", "")
			return
		case a := <-a1:
			inProgress(contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true, BankReference: s.bankID, BankRawCode: "OK ACK-1"})
			if b.verify(ctx, s, a) {
				finish(failed(contract.ReasonAuthError, true, true, "AAA_FAILED")) // no token can be issued: nothing can move
				return
			}
			phase, failedInARow = awaitAck2, 0 // on an ambiguous answer too: ACK-2 or the poll decides
		case a := <-a2:
			switch {
			case !a.requestValid:
				finish(failed(contract.ReasonBankRejected, true, true, "REQUEST_INVALID"))
				return
			case !a.acctValid:
				reason := contract.ReasonInvalidAccount
				if strings.Contains(strings.ToUpper(a.acctStatus), "CLOSED") {
					reason = contract.ReasonAccountClosed
				}
				finish(failed(reason, false, true, a.acctStatus))
				return
			case !b.cfg.Now().Before(a.deadline):
				finish(failed(contract.ReasonOther, true, true, "DEADLINE_PASSED")) // execute was never sent
				return
			}
			phase = awaitFinal // before the call: execute is sent at most once
			res := b.execute(ctx, s, a.token)
			switch {
			case res.Status == contract.StatusSucceeded || res.Status == contract.StatusFailed:
				finish(res)
				return
			case res.Status == contract.StatusProcessing:
				inProgress(res)
			case !unknownSent:
				unknownSent = report(res) // the poll decides
			}
		case <-tick.C:
			if time.Now().After(deadline) {
				end("failure", "deadline", "")
				return
			}
			polls++
			res := b.InquireStatus(ctx, s.req.TransferID, s.ref())
			switch {
			case res.Status == contract.StatusProcessing:
				failedInARow = 0
				inProgress(res)
			case res.Status == contract.StatusSucceeded:
				finish(res)
				return
			case res.Status == contract.StatusFailed && res.ReachedBank: // "not found" is never final here
				if failedInARow++; phase == awaitFinal || res.BankRawCode == "EXPIRED" || failedInARow >= failedPolls {
					finish(res)
					return
				}
			default:
				failedInARow = 0
			}
		}
	}
}

// verify sends aaa-verify for ACK-1's challenge. It reports true only when
// the bank definitely refused the AAA (AAA_FAILED).
func (b *Bank) verify(ctx context.Context, s *session, a ack1) bool {
	digest, err := PayloadHash(map[string]string{"pg_trx_ref": s.ref(), "dest_bank_code": s.req.Beneficiary.BankCode,
		"bank_acct_no": s.req.Beneficiary.AccountNo, "remit_amt": strconv.FormatInt(s.amount, 10), "aaa_challenge_nonce": a.nonce}, a.order)
	if err != nil {
		return true // checked when the webhook was accepted
	}
	r := b.call(ctx, "aaa-verify", http.MethodPost, "/v2/remit/async/aaa-verify", s.req, map[string]any{
		"bank_session_id": s.bankID, "pg_trx_ref": s.ref(), "aaa_challenge_nonce": a.nonce, "req_payload_hash": digest,
		"aaa_hmac_signature": AAASignature(b.cfg.AAASecret, digest)})
	var out struct {
		Status string `json:"aaa_status"`
	}
	_ = json.Unmarshal(r.Body, &out)
	return r.Status == http.StatusUnauthorized && out.Status == "AAA_FAILED"
}

// execute sends the single-use token. It is never retried.
func (b *Bank) execute(ctx context.Context, s *session, token string) contract.BankResult {
	r := b.call(ctx, "execute", http.MethodPost, "/v2/remit/async/execute", s.req, map[string]any{
		"bank_session_id": s.bankID, "pg_trx_ref": s.ref(), "execution_token": token, "execution_action": "EXECUTE"})
	var out statusAnswer
	switch {
	case r.NotReached: // provably not sent, and this session never sends it again
		return failed(contract.ReasonRailUnavailable, true, false, "")
	case r.Status == http.StatusGone:
		return failed(contract.ReasonOther, true, true, r.Code) // EXECUTION_EXPIRED: never executed
	case (r.Status == http.StatusOK || r.Status == http.StatusAccepted) && json.Unmarshal(r.Body, &out) == nil:
		return out.result()
	}
	return unknown(r.Code) // a timeout, 5xx, a used or refused token: the poll decides
}

type statusAnswer struct {
	State      string `json:"final_state"`
	Settlement string `json:"core_settlement_id"`
	SessionID  string `json:"bank_session_id"`
}

// result maps final_state (T§11.3; values beyond SETTLED are (A)).
func (a statusAnswer) result() contract.BankResult {
	st := a.State
	switch st {
	case "SETTLED":
		return contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: a.Settlement, BankRawCode: st}
	case "PROCESSING", "AWAITING_ACK_1", "AWAITING_AAA_VERIFY", "VALIDATING_ACCOUNT_FOR_ACK_2", "AWAITING_EXECUTE":
		return contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true, BankReference: a.SessionID, BankRawCode: st}
	case "FAILED":
		return failed(contract.ReasonBankRejected, true, true, st)
	case "EXPIRED": // the deadline passed without an execute: nothing moved
		return failed(contract.ReasonOther, true, true, st)
	}
	return unknown(st)
}

// InquireStatus is the status poll, by pg_trx_ref (= request_id). It needs
// no session, so it works after a restart.
func (b *Bank) InquireStatus(ctx context.Context, transferID, requestID string) contract.BankResult {
	r := b.call(ctx, "status", http.MethodGet, "/v2/remit/async/status/"+url.PathEscape(requestID),
		contract.TransferRequest{TransferID: transferID, RequestID: requestID}, nil)
	if r.Status == http.StatusNotFound {
		return failed(contract.ReasonOther, true, false, r.Code) // the bank has no such transfer
	}
	var out statusAnswer
	if r.Status != http.StatusOK || json.Unmarshal(r.Body, &out) != nil {
		return unknown(r.Code)
	}
	return out.result()
}

func (b *Bank) call(ctx context.Context, step, method, path string, req contract.TransferRequest, body any) bankmw.Response {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return bankmw.Response{NotReached: true, Err: err}
		}
		rd = bytes.NewReader(raw)
	}
	hr, err := http.NewRequestWithContext(ctx, method, b.cfg.BaseURL+path, rd)
	if err != nil {
		return bankmw.Response{NotReached: true, Err: err}
	}
	hr.Header.Set("Authorization", "Bearer "+b.cfg.APIKey)
	hr.Header.Set("X-PG-Request-Time", b.cfg.Now().In(wib).Format(time.RFC3339))
	if body != nil {
		hr.Header.Set("Content-Type", "application/json")
	}
	return bankmw.Do(ctx, b.client, step, hr, req, errorCode)
}

func errorCode(body []byte) string {
	var e struct {
		Error string `json:"error"`
		AAA   string `json:"aaa_status"`
	}
	_ = json.Unmarshal(body, &e)
	if e.Error == "" {
		return e.AAA
	}
	return e.Error
}

// Routes mounts the webhook listener. It isn't MOM-signed: S:C defines no
// webhook signature (Q8), so a webhook is accepted only for a live session
// in this process, matching pg_trx_ref and bank_session_id, in its stage,
// before expires_at, and (ACK-2) for the account and amount initiated.
func (b *Bank) Routes(r *gin.Engine) {
	g := r.Group(WebhookPath)
	g.POST("/ack-1", b.webhook("ack-1", "ACK-1"))
	g.POST("/ack-2", b.webhook("ack-2", "ACK-2"))
}

type webhookBody struct {
	SessionID    string   `json:"bank_session_id"`
	PgTrxRef     string   `json:"pg_trx_ref"`
	Nonce        string   `json:"aaa_challenge_nonce"`
	HashAlg      string   `json:"required_hash_alg"`
	Order        []string `json:"canonical_fields_order"`
	ExpiresAt    string   `json:"expires_at"`
	RequestValid *bool    `json:"request_valid"`
	AcctValid    *bool    `json:"acct_valid"`
	AcctStatus   string   `json:"acct_status"`
	AcctNo       string   `json:"bank_acct_no"`
	RemitAmt     *int64   `json:"remit_amt"`
	Token        string   `json:"execution_token"`
	Deadline     string   `json:"execution_deadline"`
}

func (b *Bank) webhook(stage, event string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		log := logger.FromContext(ctx)
		var s *session
		attrs := func() []any {
			a := []any{"event", "bank.webhook", "stage", stage}
			if s != nil {
				a = append(a, "transfer_id", s.req.TransferID, "request_id", s.ref())
			}
			return a
		}
		deny := func(status int, reason string) {
			log.WarnContext(ctx, "bank C webhook refused", append(attrs(), "outcome", "denied", "reason", reason)...)
			c.AbortWithStatusJSON(status, gin.H{"received": false, "error": reason})
		}
		raw, err := io.ReadAll(io.LimitReader(c.Request.Body, maxWebhook+1))
		var w webhookBody
		if err != nil || len(raw) > maxWebhook || c.GetHeader("X-BankC-Event") != event || json.Unmarshal(raw, &w) != nil {
			deny(http.StatusBadRequest, "invalid_input")
			return
		}
		b.mu.Lock()
		s = b.sessions[w.PgTrxRef]
		b.mu.Unlock()
		if s != nil {
			select {
			case <-s.ready:
			case <-ctx.Done():
			case <-time.After(readyWait):
			}
		}
		if s == nil || !isClosed(s.ready) || s.bankID == "" || !hmac.Equal([]byte(w.SessionID), []byte(s.bankID)) {
			s = nil
			deny(http.StatusNotFound, "unknown_session")
			return
		}
		now := b.cfg.Now()
		var accept func() // delivers to the session, under s.mu
		next := "SUBMIT_AAA_VERIFY"
		switch stage {
		case "ack-1":
			expires, err := time.Parse(time.RFC3339, w.ExpiresAt)
			if _, herr := PayloadHash(map[string]string{}, w.Order); err != nil || w.HashAlg != "SHA-256" || w.Nonce == "" || herr != nil {
				deny(http.StatusBadRequest, "invalid_input")
				return
			}
			if !now.Before(expires) {
				deny(http.StatusGone, "expired")
				return
			}
			a := ack1{nonce: w.Nonce, order: w.Order}
			accept = func() {
				if s.gotAck1 {
					return
				}
				s.gotAck1 = true
				s.ack1 <- a
			}
		case "ack-2":
			if w.RequestValid == nil || w.AcctValid == nil {
				deny(http.StatusBadRequest, "invalid_input")
				return
			}
			a := ack2{requestValid: *w.RequestValid, acctValid: *w.AcctValid, acctStatus: w.AcctStatus, token: w.Token}
			next = "NONE"
			if a.requestValid && a.acctValid {
				var err error
				if a.deadline, err = time.Parse(time.RFC3339, w.Deadline); err != nil || a.token == "" {
					deny(http.StatusBadRequest, "invalid_input")
					return
				}
				if w.AcctNo != s.req.Beneficiary.AccountNo || w.RemitAmt == nil || *w.RemitAmt != s.amount {
					deny(http.StatusBadRequest, "mismatch") // never execute for another account or amount
					return
				}
				next = "DISPATCH_EXECUTE"
			}
			s.mu.Lock()
			early := !s.gotAck1
			s.mu.Unlock()
			if early {
				deny(http.StatusConflict, "wrong_stage")
				return
			}
			accept = func() {
				if s.gotAck2 {
					return
				}
				s.gotAck2 = true
				s.ack2 <- a
			}
		}
		s.mu.Lock()
		dup := (stage == "ack-1" && s.gotAck1) || (stage == "ack-2" && s.gotAck2)
		accept()
		s.mu.Unlock()
		log.InfoContext(ctx, "bank C webhook received", append(attrs(), "outcome", "success", "duplicate", dup)...)
		c.JSON(http.StatusOK, gin.H{"received": true, "pg_trx_ref": w.PgTrxRef, "next_action": next})
	}
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// PayloadHash is the AAA digest (T§11.3): hex(SHA-256 of the values in
// ACK-1's canonical_fields_order, joined by "|"). A field the BMW doesn't
// know is refused: it would hash a value the bank didn't intend.
func PayloadHash(values map[string]string, order []string) (string, error) {
	if len(order) == 0 {
		return "", errors.New("stubc: empty canonical_fields_order")
	}
	parts := make([]string, len(order))
	for i, f := range order {
		if !slices.Contains(hashFields, f) {
			return "", fmt.Errorf("stubc: unknown canonical field %q", f)
		}
		parts[i] = values[f]
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:]), nil
}

// AAASignature is Base64(HMAC-SHA256(aaa secret, hex digest)).
func AAASignature(secret, digest string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(digest))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
