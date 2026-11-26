package stubc_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/banksim/stubc"
	"fintech-ledger-playground/internal/logger"
)

const (
	apiKey    = "stubC_pg_api_key_9912"
	aaaSecret = "secretC"
	acct      = "4110029384"
)

type hook struct {
	event string
	body  map[string]any
}

type env struct {
	t     *testing.T
	h     http.Handler
	hooks chan hook
	now   time.Time
	mu    sync.Mutex
	pg    *httptest.Server
}

func newEnv(t *testing.T) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	e := &env{t: t, hooks: make(chan hook, 8), now: time.Now()}
	e.pg = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var m map[string]any
		_ = json.Unmarshal(b, &m)
		e.hooks <- hook{event: r.Header.Get("X-BankC-Event") + " " + r.URL.Path, body: m}
		_, _ = w.Write([]byte(`{"received":true}`))
	}))
	t.Cleanup(e.pg.Close)
	sim := banksim.New(stubc.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "bank-simulator"), sim)
	if err != nil {
		t.Fatal(err)
	}
	stubc.Register(r, sim, stubc.Config{APIKey: apiKey, AAASecret: aaaSecret, Fee: 2500, AckTTL: 2 * time.Minute,
		ExecTTL: 3 * time.Minute, Now: e.clock})
	e.h = r
	return e
}

func (e *env) clock() time.Time { e.mu.Lock(); defer e.mu.Unlock(); return e.now }
func (e *env) advance(d time.Duration) {
	e.mu.Lock()
	e.now = e.now.Add(d)
	e.mu.Unlock()
}

func (e *env) call(method, path, body, bearer string) (int, map[string]any) {
	e.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+bearer)
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

func (e *env) fault(body string) {
	e.t.Helper()
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, httptest.NewRequest("POST", "/_sim/faults", strings.NewReader(body)))
	if w.Code != 201 {
		e.t.Fatalf("fault: %d %s", w.Code, w.Body)
	}
}

func (e *env) wait(want string) map[string]any {
	e.t.Helper()
	select {
	case h := <-e.hooks:
		if h.event != want {
			e.t.Fatalf("webhook %q, want %q", h.event, want)
		}
		return h.body
	case <-time.After(3 * time.Second):
		e.t.Fatalf("no %s webhook", want)
	}
	return nil
}

func (e *env) noHook(d time.Duration) {
	e.t.Helper()
	select {
	case h := <-e.hooks:
		e.t.Fatalf("unexpected webhook %s", h.event)
	case <-time.After(d):
	}
}

func initiateBody(ref, callback string) string {
	return `{"pg_trx_ref":"` + ref + `","source_vault_id":"VAULT-PG-881","dest_bank_code":"013","bank_acct_no":"` + acct +
		`","remit_amt":1500000,"ccy":"IDR","callback_base_url":"` + callback + `"}`
}

// aaa answers ACK-1 as the contract specifies: SHA-256 over the values in
// canonical_fields_order joined by "|", and HMAC-SHA256 over the hex digest.
func aaa(ack1 map[string]any, secret string) string {
	values := map[string]string{"pg_trx_ref": ack1["pg_trx_ref"].(string), "dest_bank_code": "013", "bank_acct_no": acct,
		"remit_amt": "1500000", "aaa_challenge_nonce": ack1["aaa_challenge_nonce"].(string)}
	var parts []string
	for _, f := range ack1["canonical_fields_order"].([]any) {
		parts = append(parts, values[f.(string)])
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	digest := hex.EncodeToString(sum[:])
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(digest))
	return `{"bank_session_id":"` + ack1["bank_session_id"].(string) + `","pg_trx_ref":"` + ack1["pg_trx_ref"].(string) +
		`","aaa_challenge_nonce":"` + ack1["aaa_challenge_nonce"].(string) + `","req_payload_hash":"` + digest +
		`","aaa_hmac_signature":"` + base64.StdEncoding.EncodeToString(mac.Sum(nil)) + `"}`
}

func executeBody(ack2 map[string]any) string {
	return `{"bank_session_id":"` + ack2["bank_session_id"].(string) + `","pg_trx_ref":"` + ack2["pg_trx_ref"].(string) +
		`","execution_token":"` + ack2["execution_token"].(string) + `","execution_action":"EXECUTE"}`
}

// handshake runs initiate → ACK-1 → aaa-verify → ACK-2 and returns ACK-2.
func (e *env) handshake(ref string) map[string]any {
	e.t.Helper()
	code, m := e.call("POST", "/v2/remit/async/initiate", initiateBody(ref, e.pg.URL+"/callbacks/bank-c"), apiKey)
	if code != 202 || m["ack_stage"] != "INIT_ACCEPTED" || m["workflow_state"] != "AWAITING_ACK_1" || m["bank_session_id"] == "" {
		e.t.Fatalf("initiate: %d %v", code, m)
	}
	ack1 := e.wait("ACK-1 /callbacks/bank-c/ack-1")
	if ack1["ack_code"] != "OK ACK-1" || ack1["bank_session_id"] != m["bank_session_id"] || ack1["required_hash_alg"] != "SHA-256" {
		e.t.Fatalf("ack-1 = %v", ack1)
	}
	code, v := e.call("POST", "/v2/remit/async/aaa-verify", aaa(ack1, aaaSecret), apiKey)
	if code != 202 || v["aaa_status"] != "AAA_PASSED" || v["workflow_state"] != "VALIDATING_ACCOUNT_FOR_ACK_2" {
		e.t.Fatalf("aaa-verify: %d %v", code, v)
	}
	return e.wait("ACK-2 /callbacks/bank-c/ack-2")
}

func (e *env) status(ref string) map[string]any {
	e.t.Helper()
	_, m := e.call("GET", "/v2/remit/async/status/"+ref, "", apiKey)
	return m
}

func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	ack2 := e.handshake("TRX-C-1")
	if ack2["request_valid"] != true || ack2["acct_valid"] != true || ack2["execution_token"] == "" || ack2["fee_amt"] != 2500.0 {
		t.Fatalf("ack-2 = %v", ack2)
	}
	code, m := e.call("POST", "/v2/remit/async/execute", executeBody(ack2), apiKey)
	if code != 200 || m["final_state"] != "SETTLED" || m["debited_amt"] != 1502500.0 || m["core_settlement_id"] == "" {
		t.Fatalf("execute: %d %v", code, m)
	}
	// The bank marks a webhook delivered once our 200 reaches it.
	var s map[string]any
	for i := 0; i < 50; i++ {
		if s = e.status("TRX-C-1"); s["ack_2_completed"] == true {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if s["final_state"] != "SETTLED" || s["ack_1_completed"] != true || s["aaa_verified"] != true || s["ack_2_completed"] != true ||
		s["core_settlement_id"] != m["core_settlement_id"] {
		t.Fatalf("status = %v", s)
	}
}

func TestBearerKeyRequired(t *testing.T) {
	e := newEnv(t)
	if code, _ := e.call("POST", "/v2/remit/async/initiate", initiateBody("TRX-C-2", e.pg.URL), "wrong"); code != 401 {
		t.Fatalf("wrong key: %d", code)
	}
	if code, _ := e.call("GET", "/v2/remit/async/status/TRX-C-2", "", ""); code != 401 {
		t.Fatalf("no key: %d", code)
	}
	e.noHook(100 * time.Millisecond)
}

func TestAAAVerifyRejections(t *testing.T) {
	e := newEnv(t)
	e.call("POST", "/v2/remit/async/initiate", initiateBody("TRX-C-3", e.pg.URL+"/callbacks/bank-c"), apiKey)
	ack1 := e.wait("ACK-1 /callbacks/bank-c/ack-1")
	if code, m := e.call("POST", "/v2/remit/async/aaa-verify", aaa(ack1, "other-secret"), apiKey); code != 401 || m["aaa_status"] != "AAA_FAILED" {
		t.Fatalf("wrong secret: %d %v", code, m)
	}
	bad := strings.Replace(aaa(ack1, aaaSecret), `"req_payload_hash":"`, `"req_payload_hash":"0`, 1)
	if code, m := e.call("POST", "/v2/remit/async/aaa-verify", bad, apiKey); code != 401 || m["aaa_status"] != "AAA_FAILED" {
		t.Fatalf("wrong hash: %d %v", code, m)
	}
	unknown := strings.Replace(aaa(ack1, aaaSecret), ack1["bank_session_id"].(string), "BSESS-0", 1)
	if code, _ := e.call("POST", "/v2/remit/async/aaa-verify", unknown, apiKey); code != 404 {
		t.Fatalf("unknown session: %d", code)
	}
	e.noHook(100 * time.Millisecond)
	if s := e.status("TRX-C-3"); s["aaa_verified"] != false || s["final_state"] != "AWAITING_AAA_VERIFY" {
		t.Fatalf("status = %v", s)
	}
}

// The token is single-use: a second execute moves no money.
func TestExecuteOnce(t *testing.T) {
	e := newEnv(t)
	ack2 := e.handshake("TRX-C-4")
	_, first := e.call("POST", "/v2/remit/async/execute", executeBody(ack2), apiKey)
	if code, m := e.call("POST", "/v2/remit/async/execute", executeBody(ack2), apiKey); code != 409 || m["error"] != "TOKEN_ALREADY_USED" {
		t.Fatalf("second execute: %d %v", code, m)
	}
	if s := e.status("TRX-C-4"); s["core_settlement_id"] != first["core_settlement_id"] {
		t.Fatalf("status = %v", s)
	}
	wrong := strings.Replace(executeBody(ack2), ack2["execution_token"].(string), "EXEC-TOK-FORGED", 1)
	if code, _ := e.call("POST", "/v2/remit/async/execute", wrong, apiKey); code != 401 {
		t.Fatalf("forged token: %d", code)
	}
}

func TestExecuteAfterDeadline(t *testing.T) {
	e := newEnv(t)
	ack2 := e.handshake("TRX-C-5")
	e.advance(3*time.Minute + time.Second)
	if code, m := e.call("POST", "/v2/remit/async/execute", executeBody(ack2), apiKey); code != 410 || m["error"] != "EXECUTION_EXPIRED" {
		t.Fatalf("late execute: %d %v", code, m)
	}
	if s := e.status("TRX-C-5"); s["final_state"] != "EXPIRED" {
		t.Fatalf("status = %v", s)
	}
}

// P§5 #9's setup: ACK-2 never arrives, and the poll shows where the session is.
func TestDropAck2(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"ack-2"},"action":"drop_webhook"}`)
	e.call("POST", "/v2/remit/async/initiate", initiateBody("TRX-C-6", e.pg.URL+"/callbacks/bank-c"), apiKey)
	ack1 := e.wait("ACK-1 /callbacks/bank-c/ack-1")
	e.call("POST", "/v2/remit/async/aaa-verify", aaa(ack1, aaaSecret), apiKey)
	e.noHook(200 * time.Millisecond)
	s := e.status("TRX-C-6")
	if s["aaa_verified"] != true || s["ack_2_completed"] != false || s["final_state"] != "AWAITING_EXECUTE" {
		t.Fatalf("status = %v", s)
	}
}

func TestDropAck1(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"ack-1","account_no":"` + acct + `"},"action":"drop_webhook"}`)
	e.call("POST", "/v2/remit/async/initiate", initiateBody("TRX-C-7", e.pg.URL+"/callbacks/bank-c"), apiKey)
	e.noHook(200 * time.Millisecond)
	if s := e.status("TRX-C-7"); s["ack_1_completed"] != false || s["final_state"] != "AWAITING_AAA_VERIFY" {
		t.Fatalf("status = %v", s)
	}
	e.advance(2*time.Minute + time.Second)
	if s := e.status("TRX-C-7"); s["final_state"] != "EXPIRED" {
		t.Fatalf("after the ACK-1 window: %v", s)
	}
}

// T§11.3 (A) rows: ACK-2 refuses the account or the request.
func TestAck2Refusals(t *testing.T) {
	for code, want := range map[string]map[string]any{
		"ACCT_CLOSED":     {"acct_valid": false, "acct_status": "CLOSED", "request_valid": true},
		"ACCT_INVALID":    {"acct_valid": false, "acct_status": "NOT_FOUND", "request_valid": true},
		"REQUEST_INVALID": {"acct_valid": true, "request_valid": false},
	} {
		t.Run(code, func(t *testing.T) {
			e := newEnv(t)
			e.fault(`{"match":{"step":"ack-2"},"action":"status","code":"` + code + `"}`)
			ack2 := e.handshake("TRX-C-8")
			for k, v := range want {
				if ack2[k] != v {
					t.Fatalf("ack-2 %s = %v, want %v (%v)", k, ack2[k], v, ack2)
				}
			}
			if _, ok := ack2["execution_token"]; ok {
				t.Fatal("refusal issued an execution token")
			}
			if s := e.status("TRX-C-8"); s["final_state"] != "FAILED" {
				t.Fatalf("status = %v", s)
			}
		})
	}
}

func TestPendingExecuteThenSuccess(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":1}`)
	ack2 := e.handshake("TRX-C-9")
	if code, m := e.call("POST", "/v2/remit/async/execute", executeBody(ack2), apiKey); code != 202 || m["final_state"] != "PROCESSING" {
		t.Fatalf("execute: %d %v", code, m)
	}
	for _, want := range []string{"PROCESSING", "SETTLED"} {
		if s := e.status("TRX-C-9"); s["final_state"] != want {
			t.Fatalf("status = %v, want %s", s, want)
		}
	}
}

func TestPendingExecuteThenFail(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_fail"}`)
	ack2 := e.handshake("TRX-C-10")
	e.call("POST", "/v2/remit/async/execute", executeBody(ack2), apiKey)
	for _, want := range []string{"PROCESSING", "FAILED"} {
		if s := e.status("TRX-C-10"); s["final_state"] != want {
			t.Fatalf("status = %v, want %s", s, want)
		}
	}
}

func TestExecuteTimeoutStillSettles(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"execute"},"action":"timeout","delay_ms":5000}`)
	ack2 := e.handshake("TRX-C-11")
	req := httptest.NewRequest("POST", "/v2/remit/async/execute", strings.NewReader(executeBody(ack2)))
	req.Header.Set("Authorization", "Bearer "+apiKey)
	ctx, cancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer cancel()
	e.h.ServeHTTP(httptest.NewRecorder(), req.WithContext(ctx))
	if s := e.status("TRX-C-11"); s["final_state"] != "SETTLED" {
		t.Fatalf("status = %v", s)
	}
}

func TestInitiateIdempotentAndValidated(t *testing.T) {
	e := newEnv(t)
	_, first := e.call("POST", "/v2/remit/async/initiate", initiateBody("TRX-C-12", e.pg.URL+"/callbacks/bank-c"), apiKey)
	e.wait("ACK-1 /callbacks/bank-c/ack-1")
	code, again := e.call("POST", "/v2/remit/async/initiate", initiateBody("TRX-C-12", e.pg.URL+"/callbacks/bank-c"), apiKey)
	if code != 202 || again["bank_session_id"] != first["bank_session_id"] {
		t.Fatalf("repeat: %d %v", code, again)
	}
	e.noHook(100 * time.Millisecond)
	if code, _ := e.call("POST", "/v2/remit/async/initiate", strings.Replace(initiateBody("TRX-C-12", e.pg.URL), "1500000", "1", 1), apiKey); code != 409 {
		t.Fatalf("same ref, other amount: %d", code)
	}
	for _, bad := range []string{`"remit_amt":1500000.5`, `"remit_amt":-1`, `"remit_amt":"1500000"`} {
		body := strings.Replace(initiateBody("TRX-C-13", e.pg.URL), `"remit_amt":1500000`, bad, 1)
		if code, _ := e.call("POST", "/v2/remit/async/initiate", body, apiKey); code != 400 {
			t.Errorf("%s: %d", bad, code)
		}
	}
	if code, _ := e.call("GET", "/v2/remit/async/status/TRX-C-404", "", apiKey); code != 404 {
		t.Fatalf("unknown ref: %d", code)
	}
}

// Known answer from the contract's example inputs and openssl:
// printf '%s' 'TRX-C-20261125-7701|013|4110029384|1500000|n9f8a7c6e5d4b3a2' | shasum -a 256
func TestAAAKnownAnswer(t *testing.T) {
	digest := stubc.PayloadHash(map[string]string{"pg_trx_ref": "TRX-C-20261125-7701", "dest_bank_code": "013",
		"bank_acct_no": "4110029384", "remit_amt": "1500000", "aaa_challenge_nonce": "n9f8a7c6e5d4b3a2"}, stubc.CanonicalFieldsOrder)
	if digest != "81f7a7b8d6da846c1a78fcb1c3bc5aecd10cf4f35770c215b885574629bef3ba" {
		t.Fatalf("digest = %s", digest)
	}
	if got := stubc.AAASignature("secretC", digest); got != "aOsjxzLV/Z0MsZYwv+oUUA1J62YwktLb7Y2E2ysN3+w=" {
		t.Fatalf("hmac = %s", got)
	}
}
