package stubb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/bankmw/stubb"
	"fintech-ledger-playground/internal/banksim"
	simb "fintech-ledger-playground/internal/banksim/stubb"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

const (
	clientID, clientSecret = "bank-middleware-b", "oauth-secret"
	apiKey, apiSecret      = "a16c5bb4-49d1-4a12-9194-db3df367d893", "dev-api-secret-b"
	corp                   = "CORPAGG01"
	acct                   = "0201245681"
	tid                    = "01a1d000-0000-7000-8000-000000000001"
)

type env struct {
	t    *testing.T
	sim  *httptest.Server
	bank *stubb.Bank
	logs *bytes.Buffer
	ctx  context.Context
	n    int
}

func newEnv(t *testing.T, tokenTTL, timeout time.Duration, transport http.RoundTripper) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	s := banksim.New(simb.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "sim"), s)
	if err != nil {
		t.Fatal(err)
	}
	simb.Register(r, s, simb.Config{ClientID: clientID, ClientSecret: clientSecret, APIKey: apiKey, APISecret: apiSecret,
		CorporateID: corp, TokenTTL: tokenTTL})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	b, err := stubb.New(stubb.Config{BaseURL: srv.URL, ClientID: clientID, ClientSecret: clientSecret, APIKey: apiKey, APISecret: apiSecret,
		CorporateID: corp, SourceAccount: "0201245680"}, &http.Client{Timeout: timeout, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	return &env{t: t, sim: srv, bank: b, logs: logs, ctx: logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "bank-middleware-b"))}
}

// request: request_ids are UUIDv7 (MOM's), whose timestamp dates the transfer.
func (e *env) request(rail, bankCode string) contract.TransferRequest {
	e.n++
	return contract.TransferRequest{RequestID: "01a210a5-5380-7000-8000-0000000000" + string(rune('a'+e.n/10)) + string(rune('0'+e.n%10)),
		TransferID: tid, AttemptNo: 1, Rail: rail, Amount: contract.Amount{Value: "750965.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: bankCode, AccountNo: acct}, IssuedAt: time.Now(), Nonce: "n"}
}

func (e *env) intrabank() contract.TransferRequest { return e.request(contract.RailIntrabank, "014") }

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

func std(t *testing.T) *env { return newEnv(t, time.Hour, 5*time.Second, nil) }

// T§11.2: inquiry ACTIVE → transfer Success → SUCCEEDED, and status SETTLED.
func TestHappyPath(t *testing.T) {
	e := std(t)
	req := e.intrabank()
	got := e.bank.Transfer(e.ctx, req)
	want(t, got, contract.StatusSucceeded, "", false, true, "Success")
	if got.BankReference == "" {
		t.Fatal("no bank_reference")
	}
	st := e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID)
	want(t, st, contract.StatusSucceeded, "", false, true, "SETTLED")
	if !strings.HasPrefix(st.BankReference, "STLB") {
		t.Fatalf("status reference = %q, want BankSettlementRef", st.BankReference)
	}
	if c := e.calls(); c["token"] != 1 || c["inquiry"] != 1 || c["transfer"] != 1 || c["status"] != 1 || c["balance"] != 0 {
		t.Fatalf("calls = %v", c)
	}
}

// T§12.1 #8: S:B's single transfer has no beneficiary bank code, so only an
// intrabank transfer to 014 can be addressed. Anything else is refused
// before any call: sending it would credit that number at bank B.
func TestOnlyIntrabankTo014(t *testing.T) {
	e := std(t)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailRTOL, "008")), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailIntrabank, "008")), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailBIFAST, "014")), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	if c := e.calls(); len(c) != 0 {
		t.Fatalf("bank called: %v", c)
	}
}

// T§11.2 (A): CLOSED and DORMANT are the beneficiary's problem.
func TestClosedOrDormantAccount(t *testing.T) {
	for _, st := range []string{"CLOSED", "DORMANT"} {
		t.Run(st, func(t *testing.T) {
			e := std(t)
			e.fault(`{"match":{"step":"inquiry","account_no":"` + acct + `"},"action":"status","code":"` + st + `"}`)
			want(t, e.bank.Transfer(e.ctx, e.intrabank()), contract.StatusFailed, contract.ReasonAccountClosed, false, true, st)
			if c := e.calls(); c["transfer"] != 0 {
				t.Fatalf("transfer called: %v", c)
			}
		})
	}
}

// T§11.2 (A): transfer errors map per ErrorCode; an unmapped code is UNKNOWN.
func TestTransferErrorCodes(t *testing.T) {
	for code, w := range map[string]struct {
		status contract.Status
		reason contract.Reason
		retry  bool
	}{
		"ESB-82-001": {contract.StatusFailed, contract.ReasonInvalidAccount, false},
		"ESB-82-002": {contract.StatusFailed, contract.ReasonAccountClosed, false},
		"ESB-14-021": {contract.StatusFailed, contract.ReasonInsufficientSourceFunds, true},
		"ESB-14-022": {contract.StatusFailed, contract.ReasonLimitExceeded, true},
		"ESB-99-009": {contract.StatusFailed, contract.ReasonRailUnavailable, true},
		"ESB-77-777": {contract.StatusUnknown, "", false},
	} {
		t.Run(code, func(t *testing.T) {
			e := std(t)
			e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":400,"code":"` + code + `"}`)
			want(t, e.bank.Transfer(e.ctx, e.intrabank()), w.status, w.reason, w.retry, true, code)
		})
	}
}

// T§11.2: timeout or 5xx at the transfer → UNKNOWN; the inquiry then decides.
func TestTimeoutOr5xxIsUnknown(t *testing.T) {
	for name, fault := range map[string]string{
		"500":                     `{"match":{"step":"transfer"},"action":"status","http_status":500}`,
		"504 in progress":         `{"match":{"step":"transfer"},"action":"pending_then_success"}`,
		"409 duplicate":           `{"match":{"step":"transfer"},"action":"status","http_status":409,"code":"ESB-82-409"}`,
		"timeout (still settles)": `{"match":{"step":"transfer"},"action":"timeout","delay_ms":3000}`,
	} {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t, time.Hour, 300*time.Millisecond, nil)
			e.fault(fault)
			want(t, e.bank.Transfer(e.ctx, e.intrabank()), contract.StatusUnknown, "", false, true, "")
		})
	}
}

func TestInquireStatusMapping(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_fail","polls":1,"code":"ACCOUNT_DORMANT"}`)
	req := e.intrabank()
	e.bank.Transfer(e.ctx, req)
	want(t, e.bank.InquireStatus(e.ctx, tid, req.RequestID), contract.StatusProcessing, "", false, true, "PENDING")
	want(t, e.bank.InquireStatus(e.ctx, tid, req.RequestID), contract.StatusFailed, contract.ReasonAccountClosed, false, true, "ACCOUNT_DORMANT")

	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_fail","code":"SOMETHING_ELSE"}`)
	other := e.intrabank()
	e.bank.Transfer(e.ctx, other)
	e.bank.InquireStatus(e.ctx, tid, other.RequestID) // PENDING
	want(t, e.bank.InquireStatus(e.ctx, tid, other.RequestID), contract.StatusFailed, contract.ReasonBankRejected, false, true, "SOMETHING_ELSE")

	// Not found: the bank has no such transfer, so it can't have moved money.
	want(t, e.bank.InquireStatus(e.ctx, tid, "01a210a5-5380-7000-8000-00000000ffff"), contract.StatusFailed, contract.ReasonOther, true, false, "ESB-82-404")
	e.fault(`{"match":{"step":"status"},"action":"status","http_status":503}`)
	want(t, e.bank.InquireStatus(e.ctx, tid, req.RequestID), contract.StatusUnknown, "", false, true, "")
}

// A 401 at a call refreshes the token and retries once; if it repeats, AUTH_ERROR.
func TestUnauthorized(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":401,"code":"ESB-14-001"}`)
	want(t, e.bank.Transfer(e.ctx, e.intrabank()), contract.StatusSucceeded, "", false, true, "Success")
	if c := e.calls(); c["token"] != 2 || c["transfer"] != 2 {
		t.Fatalf("calls = %v", c)
	}
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":401,"code":"ESB-14-001","times":2}`)
	want(t, e.bank.Transfer(e.ctx, e.intrabank()), contract.StatusFailed, contract.ReasonAuthError, true, false, "ESB-14-001")
	b := std(t)
	b.fault(`{"match":{"step":"token"},"action":"status","http_status":401,"times":3}`)
	want(t, b.bank.Transfer(b.ctx, b.intrabank()), contract.StatusFailed, contract.ReasonAuthError, true, false, "")
}

func TestTokenCached(t *testing.T) {
	e := std(t)
	e.bank.Transfer(e.ctx, e.intrabank())
	e.bank.Transfer(e.ctx, e.intrabank())
	if c := e.calls(); c["token"] != 1 {
		t.Fatalf("token calls = %d", c["token"])
	}
}

// T§12.1 (A): TransactionID is 8 characters, derived from request_id, so the
// stateless BMW can ask about it later; TransactionDate comes from the
// UUIDv7 request_id, so a status asked after midnight names the right day.
func TestTransactionIdentity(t *testing.T) {
	id1, id2 := stubb.TransactionID("01a210a5-5380-7000-8000-0000000000a1"), stubb.TransactionID("01a210a5-5380-7000-8000-0000000000a2")
	if len(id1) != 8 || id1 == id2 || id1 != stubb.TransactionID("01a210a5-5380-7000-8000-0000000000a1") {
		t.Fatalf("ids %q %q", id1, id2)
	}
	for _, c := range id1 {
		if !strings.ContainsRune("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", c) {
			t.Fatalf("id %q has %q", id1, c)
		}
	}
	// 0x01a210a55380 ms = 2026-11-25T10:00+07:00; a v4 id falls back to now.
	if d := stubb.TransactionDate("01a210a5-5380-7000-8000-000000000001", time.Time{}); d != "2026-11-25" {
		t.Fatalf("v7 date = %s", d)
	}
	now := time.Date(2026, 11, 25, 18, 0, 0, 0, time.UTC) // 01:00 WIB on the 26th
	if d := stubb.TransactionDate("6f1c1c9e-1b1a-4c2b-9a77-000000000001", now); d != "2026-11-26" {
		t.Fatalf("v4 date = %s", d)
	}
}

type refuse struct{ suffix string }

func (r refuse) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, r.suffix) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return http.DefaultTransport.RoundTrip(req)
}

// Refused at dial: provably not sent, so MOM may retry here.
func TestConnectionRefused(t *testing.T) {
	e := newEnv(t, time.Hour, 5*time.Second, refuse{"/banking/corporates/transfers"})
	want(t, e.bank.Transfer(e.ctx, e.intrabank()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	t2 := newEnv(t, time.Hour, 5*time.Second, refuse{"/api/oauth/token"})
	want(t, t2.bank.Transfer(t2.ctx, t2.intrabank()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
}

func TestLogsMaskedAndSecretFree(t *testing.T) {
	e := std(t)
	e.bank.Transfer(e.ctx, e.intrabank())
	out := e.logs.String()
	if strings.Count(out, `"event":"bank.call"`) != 3 || !strings.Contains(out, `"account":"******5681"`) {
		t.Fatalf("log = %s", out)
	}
	for _, leak := range []string{acct, apiSecret, clientSecret, "Bearer", "Basic", "X-BCA-Signature"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log contains %q", leak)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := stubb.New(stubb.Config{BaseURL: "http://x"}, http.DefaultClient); err == nil {
		t.Fatal("built without credentials")
	}
}

// A 5xx is ambiguous even when it carries a code that would be definite on a
// 4xx: the bank may have acted, so never FAILED (no failover).
func TestServerErrorWithMappedCodeIsUnknown(t *testing.T) {
	for _, code := range []string{"ESB-99-009", "ESB-14-021", "ESB-82-001"} {
		e := std(t)
		e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":503,"code":"` + code + `"}`)
		want(t, e.bank.Transfer(e.ctx, e.intrabank()), contract.StatusUnknown, "", false, true, code)
	}
}

// The 8 characters use the whole base36 alphabet; 20,000 request_ids give no
// collision (base 10 would make one likely: 10^8 values).
func TestTransactionIDSpread(t *testing.T) {
	seen := map[string]bool{}
	letters := false
	for i := 0; i < 20000; i++ {
		id := stubb.TransactionID(fmt.Sprintf("01a210a5-5380-7000-8000-%012d", i))
		if seen[id] {
			t.Fatalf("collision at %d: %s", i, id)
		}
		seen[id] = true
		letters = letters || strings.ContainsAny(id, "ABCDEFGHIJKLMNOPQRSTUVWXYZ")
	}
	if !letters {
		t.Fatal("no letters: not base36")
	}
}
