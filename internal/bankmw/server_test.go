package bankmw_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/signing"
)

type fakeBank struct {
	mu        sync.Mutex
	transfers []contract.TransferRequest
	inquiries []contract.StatusInquiry
	result    contract.BankResult
}

func (b *fakeBank) Transfer(_ context.Context, r contract.TransferRequest) contract.BankResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.transfers = append(b.transfers, r)
	return b.result
}

func (b *fakeBank) InquireStatus(_ context.Context, transferID, requestID string) contract.BankResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inquiries = append(b.inquiries, contract.StatusInquiry{TransferID: transferID, RequestID: requestID})
	return b.result
}

func (b *fakeBank) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.transfers) + len(b.inquiries)
}

type env struct {
	t      *testing.T
	h      http.Handler
	bank   *fakeBank
	signer *signing.Signer
	logs   *bytes.Buffer
	now    time.Time
}

func key(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func newEnv(t *testing.T, routes func(*gin.Engine)) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pub, priv := key(t)
	e := &env{t: t, bank: &fakeBank{result: contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true,
		BankReference: "9981234", BankRawCode: "2001800"}}, signer: signing.NewSigner("mom-1", priv), logs: &bytes.Buffer{},
		now: time.Now().UTC().Truncate(time.Second)}
	r, err := bankmw.NewRouter(bankmw.Deps{Name: "bank-middleware-a", Logger: logger.New(e.logs, slog.LevelDebug, "bank-middleware-a"),
		Bank: e.bank, Verifier: signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000),
		Now: func() time.Time { return e.now }, Ready: func(context.Context) error { return nil }, Routes: routes})
	if err != nil {
		t.Fatal(err)
	}
	e.h = r
	return e
}

func request(nonce string, at time.Time) contract.TransferRequest {
	return contract.TransferRequest{RequestID: "01a1d000-0000-7000-8000-0000000000aa", TransferID: "01a1d000-0000-7000-8000-000000000001",
		AttemptNo: 1, Rail: contract.RailBIFAST, Amount: contract.Amount{Value: "2500000.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: "014", AccountNo: "0201245681"}, IssuedAt: at, Nonce: nonce}
}

// signed builds the request MOM sends: the body carries the nonce and
// issued_at it is signed with.
func (e *env) signed(path string, body []byte, at time.Time, nonce string, s *signing.Signer) *http.Request {
	e.t.Helper()
	h, err := s.SignNonce(http.MethodPost, path, body, at, nonce)
	if err != nil {
		e.t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k := range h {
		req.Header.Set(k, h.Get(k))
	}
	return req
}

func (e *env) transferReq(mut func(*contract.TransferRequest)) (*http.Request, []byte) {
	e.t.Helper()
	nonce, _ := signing.NewNonce()
	r := request(nonce, e.now)
	if mut != nil {
		mut(&r)
	}
	body, _ := json.Marshal(r)
	return e.signed("/internal/v1/transfers", body, e.now, nonce, e.signer), body
}

func (e *env) serve(req *http.Request) (int, map[string]any) {
	e.t.Helper()
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

func errCode(m map[string]any) any {
	if er, ok := m["error"].(map[string]any); ok {
		return er["code"]
	}
	return nil
}

func (e *env) wantRejected(req *http.Request, reason string) {
	e.t.Helper()
	code, m := e.serve(req)
	if code != 401 || errCode(m) != "invalid_signature" {
		e.t.Fatalf("status %d %v, want 401 invalid_signature", code, m)
	}
	if !strings.Contains(e.logs.String(), `"event":"bank.request_verify"`) || !strings.Contains(e.logs.String(), `"reason":"`+reason+`"`) {
		e.t.Fatalf("log = %s, want bank.request_verify %s", e.logs, reason)
	}
}

func TestSignedTransferReachesTheBank(t *testing.T) {
	e := newEnv(t, nil)
	req, _ := e.transferReq(nil)
	code, m := e.serve(req)
	if code != 200 || m["status"] != "SUCCEEDED" || m["bank_reference"] != "9981234" || m["reached_bank"] != true {
		t.Fatalf("status %d %v", code, m)
	}
	if len(e.bank.transfers) != 1 || e.bank.transfers[0].Amount.Value != "2500000.00" || e.bank.transfers[0].Rail != "BIFAST" {
		t.Fatalf("bank saw %+v", e.bank.transfers)
	}
	logs := e.logs.String()
	if !strings.Contains(logs, `"event":"bank.transfer"`) || !strings.Contains(logs, `"account":"******5681"`) ||
		!strings.Contains(logs, `"status":"SUCCEEDED"`) {
		t.Fatalf("log = %s", logs)
	}
	if strings.Contains(logs, "0201245681") {
		t.Fatal("full account number logged")
	}
	if strings.Contains(logs, req.Header.Get(signing.HeaderNonce)) || strings.Contains(logs, req.Header.Get(signing.HeaderSignature)) {
		t.Fatal("nonce or signature logged")
	}
}

// P§5 #8: the body changed after signing.
func TestTamperedBodyIsRejected(t *testing.T) {
	e := newEnv(t, nil)
	req, body := e.transferReq(nil)
	tampered := httptest.NewRequest(http.MethodPost, "/internal/v1/transfers",
		bytes.NewReader(bytes.Replace(body, []byte("2500000.00"), []byte("9500000.00"), 1)))
	tampered.Header = req.Header
	e.wantRejected(tampered, "bad_signature")
	if e.bank.calls() != 0 {
		t.Fatal("the bank was called")
	}
}

// P§5 #8: the same signed request twice.
func TestReplayIsRejected(t *testing.T) {
	e := newEnv(t, nil)
	req, body := e.transferReq(nil)
	again := httptest.NewRequest(http.MethodPost, "/internal/v1/transfers", bytes.NewReader(body))
	again.Header = req.Header.Clone()
	if code, _ := e.serve(req); code != 200 {
		t.Fatalf("first: %d", code)
	}
	e.wantRejected(again, "replay")
	if len(e.bank.transfers) != 1 {
		t.Fatalf("bank called %d times", len(e.bank.transfers))
	}
}

// P§5 #8: signed with a key this BMW doesn't trust, under a known and an
// unknown kid.
func TestOtherKeyIsRejected(t *testing.T) {
	_, other := key(t)
	for _, kid := range []string{"mom-1", "mom-9"} {
		e := newEnv(t, nil)
		nonce, _ := signing.NewNonce()
		body, _ := json.Marshal(request(nonce, e.now))
		e.wantRejected(e.signed("/internal/v1/transfers", body, e.now, nonce, signing.NewSigner(kid, other)), "bad_signature")
		if e.bank.calls() != 0 {
			t.Fatalf("kid %s: the bank was called", kid)
		}
	}
}

func TestStaleRequestIsRejected(t *testing.T) {
	e := newEnv(t, nil)
	nonce, _ := signing.NewNonce()
	at := e.now.Add(-5*time.Minute - time.Second)
	body, _ := json.Marshal(request(nonce, at))
	e.wantRejected(e.signed("/internal/v1/transfers", body, at, nonce, e.signer), "stale")
}

func TestUnsignedRequestIsRejected(t *testing.T) {
	e := newEnv(t, nil)
	body, _ := json.Marshal(request("n", e.now))
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/transfers", bytes.NewReader(body))
	e.wantRejected(req, "bad_signature")
}

// T§3.5: the body's own issued_at and nonce must be the signed ones.
func TestBodyMustCarryTheSignedNonceAndTime(t *testing.T) {
	for name, mut := range map[string]func(*contract.TransferRequest){
		"nonce":     func(r *contract.TransferRequest) { r.Nonce = "other-nonce" },
		"issued_at": func(r *contract.TransferRequest) { r.IssuedAt = r.IssuedAt.Add(-time.Minute) },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, nil)
			req, _ := e.transferReq(mut)
			e.wantRejected(req, "body_mismatch")
			if e.bank.calls() != 0 {
				t.Fatal("the bank was called")
			}
		})
	}
}

func TestInvalidRequestIsRefused(t *testing.T) {
	for name, mut := range map[string]func(*contract.TransferRequest){
		"rail":   func(r *contract.TransferRequest) { r.Rail = "SWIFT" },
		"amount": func(r *contract.TransferRequest) { r.Amount.Value = "2500000.50" },
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, nil)
			req, _ := e.transferReq(mut)
			if code, m := e.serve(req); code != 400 || errCode(m) != "invalid_input" {
				t.Fatalf("status %d %v", code, m)
			}
			if e.bank.calls() != 0 {
				t.Fatal("the bank was called")
			}
		})
	}
}

// No bank concept crosses into the request: unknown fields are refused.
func TestUnknownFieldIsRefused(t *testing.T) {
	e := newEnv(t, nil)
	nonce, _ := signing.NewNonce()
	body, _ := json.Marshal(request(nonce, e.now))
	body = bytes.Replace(body, []byte(`{`), []byte(`{"sourceAccountNo":"000101002345501",`), 1)
	if code, m := e.serve(e.signed("/internal/v1/transfers", body, e.now, nonce, e.signer)); code != 400 || errCode(m) != "invalid_input" {
		t.Fatalf("status %d %v", code, m)
	}
}

// A result the bank package got wrong is never passed on as final.
func TestInvalidBankResultBecomesUnknown(t *testing.T) {
	e := newEnv(t, nil)
	e.bank.result = contract.BankResult{Status: contract.StatusFailed, ReachedBank: true} // no reason
	req, _ := e.transferReq(nil)
	code, m := e.serve(req)
	if code != 200 || m["status"] != "UNKNOWN" || m["reached_bank"] != true {
		t.Fatalf("status %d %v", code, m)
	}
	if !strings.Contains(e.logs.String(), `"reason":"invalid_result"`) {
		t.Fatalf("log = %s", e.logs)
	}
	if n := strings.Count(e.logs.String(), `"event":"bank.transfer"`); n != 1 {
		t.Fatalf("bank.transfer logged %d times, want once", n)
	}
}

func TestStatusInquiry(t *testing.T) {
	e := newEnv(t, nil)
	nonce, _ := signing.NewNonce()
	body := []byte(`{"transfer_id":"01a1d000-0000-7000-8000-000000000001","request_id":"01a1d000-0000-7000-8000-0000000000aa"}`)
	code, m := e.serve(e.signed("/internal/v1/transfers/status", body, e.now, nonce, e.signer))
	if code != 200 || m["status"] != "SUCCEEDED" || len(e.bank.inquiries) != 1 || e.bank.inquiries[0].RequestID != "01a1d000-0000-7000-8000-0000000000aa" {
		t.Fatalf("status %d %v %+v", code, m, e.bank.inquiries)
	}
	// Signed for the transfer route, replayed at the status route.
	req, b := e.transferReq(nil)
	moved := httptest.NewRequest(http.MethodPost, "/internal/v1/transfers/status", bytes.NewReader(b))
	moved.Header = req.Header
	e.wantRejected(moved, "bad_signature")
	if code, m := e.serve(e.signed("/internal/v1/transfers/status", []byte(`{"transfer_id":"x"}`), e.now, "n2", e.signer)); code != 400 {
		t.Fatalf("invalid inquiry: %d %v", code, m)
	}
}

// The bank's own webhooks (S:C) are the only other routes, and aren't MOM-signed.
func TestBankRoutesAndProbes(t *testing.T) {
	e := newEnv(t, func(r *gin.Engine) {
		r.POST("/callbacks/bank-c/ack-1", func(c *gin.Context) { c.JSON(200, gin.H{"received": true}) })
	})
	if code, m := e.serve(httptest.NewRequest(http.MethodPost, "/callbacks/bank-c/ack-1", nil)); code != 200 || m["received"] != true {
		t.Fatalf("webhook: %d %v", code, m)
	}
	for _, p := range []string{"/healthz", "/readyz"} {
		if code, _ := e.serve(httptest.NewRequest(http.MethodGet, p, nil)); code != 200 {
			t.Fatalf("%s: %d", p, code)
		}
	}
}

func TestRouterNeedsItsParts(t *testing.T) {
	pub, _ := key(t)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"k": pub}, time.Minute, 10)
	for name, d := range map[string]bankmw.Deps{
		"no bank":     {Name: "b", Logger: slog.Default(), Verifier: v},
		"no verifier": {Name: "b", Logger: slog.Default(), Bank: &fakeBank{}},
		"no name":     {Logger: slog.Default(), Bank: &fakeBank{}, Verifier: v},
	} {
		if _, err := bankmw.NewRouter(d); err == nil {
			t.Errorf("%s: built", name)
		}
	}
}
