package stuba_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/bankmw/stuba"
	"fintech-ledger-playground/internal/banksim"
	sima "fintech-ledger-playground/internal/banksim/stuba"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

const (
	clientKey = "962489e9-de5d-4eb7-92a4-b07d44d64bf5"
	secret    = "dev-secret-a"
	partner   = "PG-AGG-001"
	acct      = "0201245681"
	tid       = "01a1d000-0000-7000-8000-000000000001"
)

var key = func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}()

type env struct {
	t    *testing.T
	sim  *httptest.Server
	bank *stuba.Bank
	logs *bytes.Buffer
	ctx  context.Context
	n    int
}

func newEnv(t *testing.T, tokenTTL, clientTimeout time.Duration) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	s := banksim.New(sima.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "sim"), s)
	if err != nil {
		t.Fatal(err)
	}
	sima.Register(r, s, sima.Config{ClientKey: clientKey, PartnerPublicKey: &key.PublicKey, ClientSecret: secret, PartnerID: partner, TokenTTL: tokenTTL})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return newEnvOn(t, srv, clientTimeout)
}

func newEnvOn(t *testing.T, srv *httptest.Server, clientTimeout time.Duration) *env {
	t.Helper()
	b, err := stuba.New(stuba.Config{BaseURL: srv.URL, ClientKey: clientKey, PrivateKey: key, ClientSecret: secret, PartnerID: partner,
		ChannelID: "95221", SourceAccount: "000101002345501", DeviceID: "bank-middleware-a"}, &http.Client{Timeout: clientTimeout})
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	return &env{t: t, sim: srv, bank: b, logs: logs, ctx: logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "bank-middleware-a"))}
}

// request is attempt n's request; request_id is the bank's partnerReferenceNo.
func (e *env) request(rail string) contract.TransferRequest {
	e.n++
	return contract.TransferRequest{RequestID: "01a1d000-0000-7000-8000-0000000000" + string(rune('a'+e.n/10)) + string(rune('0'+e.n%10)),
		TransferID: tid, AttemptNo: 1, Rail: rail, Amount: contract.Amount{Value: "2500000.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: "014", AccountNo: acct}, IssuedAt: time.Now(), Nonce: "n"}
}

func (e *env) fault(body string) {
	e.t.Helper()
	resp, err := http.Post(e.sim.URL+"/_sim/faults", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode != 201 {
		e.t.Fatalf("fault: %v %v", resp, err)
	}
	resp.Body.Close()
}

func (e *env) calls() map[string]int {
	e.t.Helper()
	resp, err := http.Get(e.sim.URL + "/_sim/calls")
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]int
	_ = json.NewDecoder(resp.Body).Decode(&m)
	return m
}

func want(t *testing.T, got contract.BankResult, status contract.Status, reason contract.Reason, retry, reached bool, raw string) {
	t.Helper()
	if got.Status != status || got.Reason != reason || got.RetryElsewhere != retry || got.ReachedBank != reached || (raw != "" && got.BankRawCode != raw) {
		t.Fatalf("result = %+v, want %s %s retry=%v reached=%v raw=%s", got, status, reason, retry, reached, raw)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
}

// T§11.1: inquiry 2001600 → continue; transfer 2001800 → SUCCEEDED.
func TestHappyPath(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	got := e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST))
	want(t, got, contract.StatusSucceeded, "", false, true, "2001800")
	if got.BankReference == "" {
		t.Fatal("no bank_reference")
	}
	if c := e.calls(); c["token"] != 1 || c["inquiry"] != 1 || c["transfer"] != 1 {
		t.Fatalf("calls = %v", c)
	}
}

// T§11.1 (A): inquiry 4041611 → FAILED INVALID_ACCOUNT, and no transfer.
func TestInvalidAccount(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.fault(`{"match":{"step":"inquiry","account_no":"` + acct + `"},"action":"status","http_status":404,"code":"4041611"}`)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusFailed, contract.ReasonInvalidAccount, false, true, "4041611")
	if c := e.calls(); c["transfer"] != 0 {
		t.Fatalf("transfer called: %v", c)
	}
}

// T§11.1: the Pending answers are UNKNOWN, never FAILED.
func TestPendingAnswersAreUnknown(t *testing.T) {
	for name, fault := range map[string]string{
		"202 2021800": `{"match":{"step":"transfer"},"action":"pending_then_success"}`,
		"403 4031816": `{"match":{"step":"transfer"},"action":"status","http_status":403,"code":"4031816"}`,
		"500":         `{"match":{"step":"transfer"},"action":"status","http_status":500}`,
		"504":         `{"match":{"step":"transfer"},"action":"status","http_status":504}`,
		"unmapped":    `{"match":{"step":"transfer"},"action":"status","http_status":409,"code":"4091800"}`,
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, 15*time.Minute, 5*time.Second)
			e.fault(fault)
			want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusUnknown, "", false, true, "")
		})
	}
}

// P§5 #4's bank side: a timeout is UNKNOWN, and the inquiry finds the money moved.
func TestTimeoutIsUnknownThenInquirySucceeds(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 300*time.Millisecond)
	e.fault(`{"match":{"step":"transfer"},"action":"timeout","delay_ms":3000}`)
	req := e.request(contract.RailBIFAST)
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusUnknown, "", false, true, "")
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusSucceeded, "", false, true, "00")
}

// T§11.1 (A): limit and source funds are ours or this bank's: retry elsewhere.
func TestBusinessRefusals(t *testing.T) {
	for code, reason := range map[string]contract.Reason{"4031802": contract.ReasonLimitExceeded, "4031814": contract.ReasonInsufficientSourceFunds} {
		t.Run(code, func(t *testing.T) {
			e := newEnv(t, 15*time.Minute, 5*time.Second)
			e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":403,"code":"` + code + `"}`)
			want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusFailed, reason, true, true, code)
		})
	}
}

// T§11.1: 401 at the transfer → refresh the token and retry once.
func TestUnauthorizedRefreshesTokenOnce(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":401,"code":"4011800"}`)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusSucceeded, "", false, true, "2001800")
	if c := e.calls(); c["token"] != 2 || c["transfer"] != 2 {
		t.Fatalf("calls = %v, want a second token and one retry", c)
	}
}

// …and if it repeats, AUTH_ERROR, which never reached the bank.
func TestRepeatedUnauthorizedIsAuthError(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":401,"code":"4011800","times":2}`)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusFailed, contract.ReasonAuthError, true, false, "4011800")
	if c := e.calls(); c["transfer"] != 2 {
		t.Fatalf("calls = %v, want exactly one retry", c)
	}
}

func TestTokenRefusedIsAuthError(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.fault(`{"match":{"step":"token"},"action":"status","http_status":401,"code":"4017300","times":5}`)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusFailed, contract.ReasonAuthError, true, false, "4017300")
}

// The token is cached until expiresIn, then fetched again.
func TestTokenCachedUntilExpiry(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST))
	e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST))
	if c := e.calls(); c["token"] != 1 {
		t.Fatalf("token calls = %d, want 1 (cached)", c["token"])
	}
	// expiresIn 2s is inside the 30s refresh margin: every call (2 transfers
	// × inquiry and transfer) fetches a fresh token rather than risk expiry.
	short := newEnv(t, 2*time.Second, 5*time.Second)
	short.bank.Transfer(short.ctx, short.request(contract.RailBIFAST))
	short.bank.Transfer(short.ctx, short.request(contract.RailBIFAST))
	if c := short.calls(); c["token"] != 4 {
		t.Fatalf("token calls = %d, want 4 (refreshed near expiry)", c["token"])
	}
}

// T§11.1 status rows.
func TestInquireStatusMapping(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_fail","polls":1}`)
	req := e.request(contract.RailBIFAST)
	e.bank.Transfer(e.ctx, req)
	want(t, e.bank.InquireStatus(e.ctx, tid, req.RequestID), contract.StatusProcessing, "", false, true, "03")
	want(t, e.bank.InquireStatus(e.ctx, tid, req.RequestID), contract.StatusFailed, contract.ReasonBankRejected, true, true, "06")
	// 07: the bank has no such transfer; it can't have moved money.
	want(t, e.bank.InquireStatus(e.ctx, tid, "01a1d000-0000-7000-8000-00000000ffff"), contract.StatusFailed, contract.ReasonOther, true, false, "07")
	e.fault(`{"match":{"step":"status"},"action":"status","http_status":503}`)
	want(t, e.bank.InquireStatus(e.ctx, tid, req.RequestID), contract.StatusUnknown, "", false, true, "")
}

// Connection refused: provably never reached the bank, so MOM may retry here.
func TestConnectionRefusedNeverReachedBank(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	srv.Close()
	e := newEnvOn(t, srv, time.Second)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
}

// A rail this bank doesn't offer is a routing error: refused before any call.
func TestUnsupportedRail(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailRTGS)), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	if c := e.calls(); len(c) != 0 {
		t.Fatalf("bank called: %v", c)
	}
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailRTOL)), contract.StatusSucceeded, "", false, true, "2001800")
}

// One bank.call per API call, masked, with no token, secret or signature.
func TestBankCallLogs(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST))
	out := e.logs.String()
	for _, step := range []string{"token", "inquiry", "transfer"} {
		if !strings.Contains(out, `"step":"`+step+`"`) {
			t.Errorf("no bank.call for %s: %s", step, out)
		}
	}
	if strings.Count(out, `"event":"bank.call"`) != 3 || !strings.Contains(out, `"account":"******5681"`) {
		t.Fatalf("log = %s", out)
	}
	for _, leak := range []string{acct, secret, "stubA.", "Bearer", "X-SIGNATURE"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log contains %q: %s", leak, out)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := stuba.New(stuba.Config{BaseURL: "http://x"}, http.DefaultClient); err == nil {
		t.Fatal("built without credentials")
	}
}

// refuseTransfer fails the transfer call at dial, as a refused connection
// does, and passes every other call through.
type refuseTransfer struct{ next http.RoundTripper }

func (r refuseTransfer) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, "/transfer-interbank") {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return r.next.RoundTrip(req)
}

// The token and inquiry went through, then the transfer couldn't connect:
// provably not sent, so FAILED and not reached, which lets MOM retry here.
func TestTransferConnectionRefusedNeverReachedBank(t *testing.T) {
	e := newEnv(t, 15*time.Minute, 5*time.Second)
	b, err := stuba.New(stuba.Config{BaseURL: e.sim.URL, ClientKey: clientKey, PrivateKey: key, ClientSecret: secret, PartnerID: partner,
		ChannelID: "95221", SourceAccount: "000101002345501"}, &http.Client{Transport: refuseTransfer{http.DefaultTransport}, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	want(t, b.Transfer(e.ctx, e.request(contract.RailBIFAST)), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	if c := e.calls(); c["inquiry"] != 1 || c["transfer"] != 0 {
		t.Fatalf("calls = %v", c)
	}
	if !strings.Contains(e.logs.String(), `"reason":"connection_refused"`) {
		t.Fatalf("log = %s", e.logs.String())
	}
}
