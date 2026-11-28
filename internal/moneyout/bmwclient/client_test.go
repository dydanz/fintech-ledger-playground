package bmwclient_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/bmwclient"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/signing"
)

// bank answers whatever the test sets; it records what it was asked.
type bank struct {
	mu        sync.Mutex
	result    contract.BankResult
	delay     time.Duration
	transfers []contract.TransferRequest
	inquiries [][2]string
}

func (b *bank) Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult {
	b.mu.Lock()
	b.transfers = append(b.transfers, req)
	res, d := b.result, b.delay
	b.mu.Unlock()
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
	return res
}

func (b *bank) InquireStatus(_ context.Context, transferID, requestID string) contract.BankResult {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.inquiries = append(b.inquiries, [2]string{transferID, requestID})
	return b.result
}

type env struct {
	bank   *bank
	srv    *httptest.Server
	client *bmwclient.Client
	ctx    context.Context
}

// newEnv runs a real bank middleware (bankmw's server and verifier) in
// front of a scripted bank, with MOM's key, or another when foreign.
func newEnv(t *testing.T, foreign bool, timeout time.Duration) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	b := &bank{result: contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: "R1"}}
	r, err := bankmw.NewRouter(bankmw.Deps{Name: "bmw-a", Logger: logger.New(&bytes.Buffer{}, slog.LevelDebug, "bmw-a"), Bank: b,
		Verifier: signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 100)})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	key := priv
	if foreign {
		key = other
	}
	c := bmwclient.New(signing.NewSigner("mom-1", key), map[string]string{"bmw-a": srv.URL, "bmw-down": "http://127.0.0.1:1"},
		&http.Client{Timeout: timeout})
	return &env{bank: b, srv: srv, client: c, ctx: context.Background()}
}

func request() contract.TransferRequest {
	return contract.TransferRequest{RequestID: "01a210a5-5380-7000-8000-000000000001", TransferID: "01a1d000-0000-7000-8000-000000000001",
		AttemptNo: 1, Rail: "BIFAST", Amount: contract.Amount{Value: "750965.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: "014", AccountNo: "0201245681"}}
}

// A signed request reaches the bank with its own nonce and issued_at in the
// body (T§3.5), and the bank's result comes back unchanged.
func TestTransferSignedAndAnswered(t *testing.T) {
	e := newEnv(t, false, 5*time.Second)
	o := e.client.Transfer(e.ctx, "bmw-a", request())
	if o.Result.Status != contract.StatusSucceeded || o.Result.BankReference != "R1" || o.Reason != "ok" || o.HTTPStatus != 200 {
		t.Fatalf("outcome = %+v", o)
	}
	if len(e.bank.transfers) != 1 || e.bank.transfers[0].Nonce == "" || e.bank.transfers[0].IssuedAt.IsZero() ||
		e.bank.transfers[0].RequestID != request().RequestID {
		t.Fatalf("bank saw %+v", e.bank.transfers)
	}
	// Each call is a fresh nonce: a second one isn't a replay.
	if o := e.client.Transfer(e.ctx, "bmw-a", request()); o.Reason != "ok" {
		t.Fatalf("second call = %+v", o)
	}
}

func TestInquire(t *testing.T) {
	e := newEnv(t, false, 5*time.Second)
	e.bank.result = contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true}
	o := e.client.Inquire(e.ctx, "bmw-a", request().TransferID, request().RequestID)
	if o.Result.Status != contract.StatusProcessing || o.Reason != "ok" || len(e.bank.inquiries) != 1 || e.bank.inquiries[0][1] != request().RequestID {
		t.Fatalf("outcome = %+v, bank saw %v", o, e.bank.inquiries)
	}
}

// What MOM concludes when the bank middleware itself answers badly or not
// at all (T§6.3). Only a provable non-delivery may be retried on the node.
func TestTransportOutcomes(t *testing.T) {
	// Down: never sent, retry on the same node.
	e := newEnv(t, false, 5*time.Second)
	o := e.client.Transfer(e.ctx, "bmw-down", request())
	want(t, o, contract.StatusFailed, contract.ReasonRailUnavailable, true, false, false, "connection_refused")

	// No endpoint configured: never sent; a config gap, not worth retrying here.
	o = e.client.Transfer(e.ctx, "bmw-unknown", request())
	want(t, o, contract.StatusFailed, contract.ReasonRailUnavailable, true, false, true, "no_endpoint")

	// Timeout: the middleware may have called the bank. UNKNOWN.
	slow := newEnv(t, false, 100*time.Millisecond)
	slow.bank.delay = time.Second
	want(t, slow.client.Transfer(slow.ctx, "bmw-a", request()), contract.StatusUnknown, "", false, true, false, "timeout")

	// Signed with another key: refused before the bank. A bug, not a
	// transient: never retried on the same node, may go elsewhere.
	f := newEnv(t, true, 5*time.Second)
	want(t, f.client.Transfer(f.ctx, "bmw-a", request()), contract.StatusFailed, contract.ReasonAuthError, true, false, true, "invalid_signature")
	if len(f.bank.transfers) != 0 {
		t.Fatal("a foreign signature reached the bank")
	}

	// A result that fails validation: UNKNOWN, never trusted as final.
	bad := newEnv(t, false, 5*time.Second)
	bad.bank.result = contract.BankResult{Status: "DONE"}
	want(t, bad.client.Transfer(bad.ctx, "bmw-a", request()), contract.StatusUnknown, "", false, true, false, "")
}

func TestServerErrorsAreUnknown(t *testing.T) {
	for _, code := range []int{500, 502, 503} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		c := bmwclient.New(signing.NewSigner("mom-1", mustKey(t)), map[string]string{"x": srv.URL}, http.DefaultClient)
		o := c.Transfer(context.Background(), "x", request())
		srv.Close()
		want(t, o, contract.StatusUnknown, "", false, true, false, "http_5xx")
	}
	// 400 invalid_input: refused before the bank, a bug; may go elsewhere.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"error":{"code":"invalid_input","message":"x"}}`))
	}))
	defer srv.Close()
	c := bmwclient.New(signing.NewSigner("mom-1", mustKey(t)), map[string]string{"x": srv.URL}, http.DefaultClient)
	want(t, c.Transfer(context.Background(), "x", request()), contract.StatusFailed, contract.ReasonOther, true, false, true, "invalid_input")
}

// A connection refused from a custom transport counts as never sent.
func TestDialRefused(t *testing.T) {
	c := bmwclient.New(signing.NewSigner("mom-1", mustKey(t)), map[string]string{"x": "http://bmw.invalid"},
		&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "bmw.invalid", IsNotFound: true}}
		})})
	want(t, c.Transfer(context.Background(), "x", request()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, false, "connection_refused")
}

func mustKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, k, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func want(t *testing.T, o bmwclient.Outcome, status contract.Status, reason contract.Reason, retry, reached, noSameNode bool, why string) {
	t.Helper()
	r := o.Result
	if r.Status != status || r.Reason != reason || r.RetryElsewhere != retry || r.ReachedBank != reached || o.NoSameNode != noSameNode ||
		(why != "" && o.Reason != why) {
		t.Fatalf("outcome = %+v, want %s %s retry=%v reached=%v noSameNode=%v reason=%s", o, status, reason, retry, reached, noSameNode, why)
	}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(o.Reason, " ") {
		t.Fatalf("reason %q is not a fixed value", o.Reason)
	}
}

// A connection lost after the request went out (not a dial failure) may
// have reached the bank: UNKNOWN, never "not sent".
func TestReadErrorIsUnknown(t *testing.T) {
	c := bmwclient.New(signing.NewSigner("mom-1", mustKey(t)), map[string]string{"x": "http://bmw"},
		&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
		})})
	want(t, c.Transfer(context.Background(), "x", request()), contract.StatusUnknown, "", false, true, false, "timeout")
}

// A 200 whose body isn't a valid result is never trusted as final.
func TestInvalidResultBodyIsUnknown(t *testing.T) {
	for _, body := range []string{`{"status":"DONE"}`, `{"status":"FAILED"}`, `not json`} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(body)) }))
		c := bmwclient.New(signing.NewSigner("mom-1", mustKey(t)), map[string]string{"x": srv.URL}, http.DefaultClient)
		o := c.Transfer(context.Background(), "x", request())
		srv.Close()
		want(t, o, contract.StatusUnknown, "", false, true, false, "invalid_response")
	}
}
