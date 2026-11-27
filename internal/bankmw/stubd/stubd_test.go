package stubd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/bankmw/stubd"
	"fintech-ledger-playground/internal/banksim"
	simd "fintech-ledger-playground/internal/banksim/stubd"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

const (
	clientKey, secret = "ent_client_d_004", "dev-bank-d-secret"
	funding           = "8800112233"
	acct              = "700192837400"
	tid               = "01a1d000-0000-7000-8000-000000000001"
)

// report is one status the poller published.
type report struct {
	transferID, requestID string
	res                   contract.BankResult
}

// reporter records reports; fail makes every Report fail (a lost publish).
type reporter struct {
	mu   sync.Mutex
	got  []report
	fail bool
	ch   chan report
}

func (r *reporter) Report(_ context.Context, transferID, requestID string, res contract.BankResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("kafka down")
	}
	rep := report{transferID, requestID, res}
	r.got = append(r.got, rep)
	r.ch <- rep
	return nil
}

func (r *reporter) all() []report {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]report(nil), r.got...)
}

type env struct {
	t    *testing.T
	sim  *httptest.Server
	bank *stubd.Bank
	rep  *reporter
	logs *syncBuffer
	ctx  context.Context
	n    int
}

// syncBuffer is a log buffer the poller goroutines and the test share.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

type opts struct {
	timeout   time.Duration
	pollFor   time.Duration
	transport http.RoundTripper
	baseURL   string
	clientKey string
}

func newEnv(t *testing.T, o opts) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	if o.timeout == 0 {
		o.timeout = 5 * time.Second
	}
	if o.pollFor == 0 {
		o.pollFor = 5 * time.Second
	}
	if o.clientKey == "" {
		o.clientKey = clientKey
	}
	s := banksim.New(simd.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "sim"), s)
	if err != nil {
		t.Fatal(err)
	}
	simd.Register(r, s, simd.Config{ClientKey: clientKey, Secret: secret, FundingAcct: funding, Liquidity: 950000000})
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	if o.baseURL == "" {
		o.baseURL = srv.URL
	}
	logs := &syncBuffer{}
	log := logger.New(logs, slog.LevelDebug, "bank-middleware-d")
	rep := &reporter{ch: make(chan report, 16)}
	b, err := stubd.New(stubd.Config{BaseURL: o.baseURL, ClientKey: o.clientKey, Secret: secret, FundingAccount: funding,
		PollInterval: 10 * time.Millisecond, PollFor: o.pollFor}, &http.Client{Timeout: o.timeout, Transport: o.transport}, rep, log)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return &env{t: t, sim: srv, bank: b, rep: rep, logs: logs, ctx: logger.WithContext(context.Background(), log)}
}

func std(t *testing.T) *env { return newEnv(t, opts{}) }

func (e *env) request(rail string) contract.TransferRequest {
	e.n++
	return contract.TransferRequest{RequestID: "01a210a5-5380-7000-8000-0000000000" + string(rune('a'+e.n/10)) + string(rune('0'+e.n%10)),
		TransferID: tid, AttemptNo: 1, Rail: rail, Amount: contract.Amount{Value: "150000000.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: "022", AccountNo: acct}, IssuedAt: time.Now(), Nonce: "n"}
}

func (e *env) bifast() contract.TransferRequest { return e.request(contract.RailBIFAST) }

func (e *env) fault(body string) {
	e.t.Helper()
	resp, err := http.Post(e.sim.URL+"/_sim/faults", "application/json", strings.NewReader(body))
	if err != nil || resp.StatusCode != 201 {
		e.t.Fatalf("fault: %v %v", resp, err)
	}
	resp.Body.Close()
}

func (e *env) reset() {
	e.t.Helper()
	resp, err := http.Post(e.sim.URL+"/_sim/reset", "application/json", nil)
	if err != nil {
		e.t.Fatal(err)
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

// next waits for the poller's next report.
func (e *env) next() report {
	e.t.Helper()
	select {
	case r := <-e.rep.ch:
		return r
	case <-time.After(3 * time.Second):
		e.t.Fatalf("no report; got %+v; logs %s", e.rep.all(), e.logs)
		return report{}
	}
}

// waitLog waits until the logs contain s.
func (e *env) waitLog(s string) {
	e.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !strings.Contains(e.logs.String(), s) {
		if time.Now().After(deadline) {
			e.t.Fatalf("logs lack %s: %s", s, e.logs)
		}
		time.Sleep(5 * time.Millisecond)
	}
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

// T§11.4: validate → funding → remit SUBMITTED is ACCEPTED; the poller then
// reports PROCESSING (IN_CLEARING) and SUCCEEDED (SETTLED), in that order.
func TestAcceptedThenProcessingThenSucceeded(t *testing.T) {
	e := std(t)
	req := e.bifast()
	got := e.bank.Transfer(e.ctx, req)
	want(t, got, contract.StatusAccepted, "", false, true, "IN_CLEARING")
	if !strings.HasPrefix(got.BankReference, "BIFAST-") {
		t.Fatalf("bank_reference = %q, want bank-rail-ref", got.BankReference)
	}
	p := e.next()
	want(t, p.res, contract.StatusProcessing, "", false, true, "IN_CLEARING")
	s := e.next()
	want(t, s.res, contract.StatusSucceeded, "", false, true, "SETTLED")
	if p.transferID != tid || p.requestID != req.RequestID || s.res.BankReference != got.BankReference {
		t.Fatalf("reports = %+v %+v", p, s)
	}
	e.waitLog(`"event":"bank.poll"`)
	if c := e.calls(); c["acct-validate"] != 1 || c["funding-check"] != 1 || c["remit-single"] != 1 {
		t.Fatalf("calls = %v", c)
	}
	if n := len(e.rep.all()); n != 2 {
		t.Fatalf("%d reports, want 2", n)
	}
	if !strings.Contains(e.logs.String(), `"outcome":"success"`) || !strings.Contains(e.logs.String(), `"polls":2`) {
		t.Fatalf("bank.poll success with polls missing: %s", e.logs)
	}
}

// Every rail S:D offers; anything else is refused before any call.
func TestRails(t *testing.T) {
	for _, rail := range []string{contract.RailBIFAST, contract.RailSKNBI, contract.RailRTGS} {
		e := std(t)
		want(t, e.bank.Transfer(e.ctx, e.request(rail)), contract.StatusAccepted, "", false, true, "")
	}
	e := std(t)
	want(t, e.bank.Transfer(e.ctx, e.request(contract.RailRTOL)), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	if c := e.calls(); c["acct-validate"] != 0 || c["remit-single"] != 0 {
		t.Fatalf("calls = %v", c)
	}
}

func TestInvalidAccount(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"acct-validate"},"action":"status","code":"14"}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonInvalidAccount, false, true, "14")
	if c := e.calls(); c["remit-single"] != 0 {
		t.Fatalf("remitted after a failed validation: %v", c)
	}
}

// funding-check refusals: no remittance was sent, so another bank may try.
func TestFundingCheck(t *testing.T) {
	for code, reason := range map[string]contract.Reason{"WINDOW_CLOSED": contract.ReasonCutoff, "LOW_LIQUIDITY": contract.ReasonInsufficientSourceFunds} {
		e := std(t)
		e.fault(`{"match":{"step":"funding-check"},"action":"status","code":"` + code + `"}`)
		want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, reason, true, true, "")
		if c := e.calls(); c["remit-single"] != 0 {
			t.Fatalf("%s: remitted: %v", code, c)
		}
	}
}

// A failed call before remit-single never sent money: FAILED, not reached.
func TestPreRemitErrorsAreRailUnavailable(t *testing.T) {
	for _, step := range []string{"acct-validate", "funding-check"} {
		e := std(t)
		e.fault(`{"match":{"step":"` + step + `"},"action":"status","http_status":503}`)
		want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
		if c := e.calls(); c["remit-single"] != 0 {
			t.Fatalf("%s: remitted: %v", step, c)
		}
	}
}

func TestBadCredentialsAreAuthError(t *testing.T) {
	e := newEnv(t, opts{clientKey: "someone-else"})
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonAuthError, true, false, "41")
}

// A remit-single timeout or 5xx is ambiguous: UNKNOWN, no poller (MOM's
// inquiry resolves it), and the inquiry finds the money moving.
func TestRemitTimeoutOr5xxIsUnknown(t *testing.T) {
	e := newEnv(t, opts{timeout: 200 * time.Millisecond})
	e.fault(`{"match":{"step":"remit-single"},"action":"timeout","delay_ms":2000}`)
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusUnknown, "", false, true, "")
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusProcessing, "", false, true, "IN_CLEARING")
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusSucceeded, "", false, true, "SETTLED")
	time.Sleep(50 * time.Millisecond)
	if n := len(e.rep.all()); n != 0 {
		t.Fatalf("an UNKNOWN transfer started a poller: %d reports", n)
	}
	for _, code := range []int{500, 502, 504} {
		e := std(t)
		e.fault(`{"match":{"step":"remit-single"},"action":"status","http_status":` + strconv.Itoa(code) + `}`)
		want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusUnknown, "", false, true, "")
	}
}

// remit-status REJECTED or RETURNED is FAILED BANK_REJECTED (T§11.4 (A)).
func TestRejectedOrReturned(t *testing.T) {
	for _, final := range []string{"REJECTED", "RETURNED"} {
		e := std(t)
		e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_fail","code":"` + final + `"}`)
		want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
		want(t, e.next().res, contract.StatusProcessing, "", false, true, "")
		want(t, e.next().res, contract.StatusFailed, contract.ReasonBankRejected, true, true, final)
	}
}

// P§3.6: a status that was never published is recovered by MOM's inquiry.
func TestLostEventRecoveredByInquiry(t *testing.T) {
	e := std(t)
	e.rep.fail = true
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "")
	e.waitLog(`"reason":"report_failed"`)
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusSucceeded, "", false, true, "SETTLED")
}

func TestInquireUnknownOrderIsFailedNotReached(t *testing.T) {
	e := std(t)
	want(t, e.bank.InquireStatus(e.ctx, tid, "01a210a5-5380-7000-8000-00000000ffff"), contract.StatusFailed, contract.ReasonOther, true, false, "14")
}

func TestInquire5xxIsUnknown(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"remit-status"},"action":"status","http_status":500}`)
	want(t, e.bank.InquireStatus(e.ctx, tid, "01a210a5-5380-7000-8000-00000000ffff"), contract.StatusUnknown, "", false, true, "")
}

// After SUBMITTED, an "order not found" must never be reported as FAILED:
// MOM would fail over and could pay twice. The poller keeps asking until
// its deadline and reports nothing.
func TestPollerNeverReportsNotFoundAfterSubmit(t *testing.T) {
	e := newEnv(t, opts{pollFor: 150 * time.Millisecond})
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_success","polls":-1}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next().res, contract.StatusProcessing, "", false, true, "")
	e.reset() // the bank forgets the order
	e.waitLog(`"reason":"deadline"`)
	for _, r := range e.rep.all() {
		if r.res.Status == contract.StatusFailed {
			t.Fatalf("reported %+v", r.res)
		}
	}
}

func TestPollDeadline(t *testing.T) {
	e := newEnv(t, opts{pollFor: 100 * time.Millisecond})
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_success","polls":-1}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.waitLog(`"reason":"deadline"`)
	for _, r := range e.rep.all() {
		if r.res.Status != contract.StatusProcessing {
			t.Fatalf("reported %+v before any terminal state", r.res)
		}
	}
	if n := len(e.rep.all()); n != 1 {
		t.Fatalf("%d PROCESSING reports, want 1", n)
	}
}

// The same attempt sent twice (the bank answers SUBMITTED again) is polled once.
func TestRepeatTransferPollsOnce(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_success","polls":3}`)
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "")
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "")
	want(t, e.next().res, contract.StatusProcessing, "", false, true, "")
	want(t, e.next().res, contract.StatusSucceeded, "", false, true, "")
	e.waitLog(`"event":"bank.poll"`)
	time.Sleep(50 * time.Millisecond)
	if n := len(e.rep.all()); n != 2 {
		t.Fatalf("%d reports, want 2: %+v", n, e.rep.all())
	}
}

func TestCloseStopsPollers(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_success","polls":-1}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.next()
	done := make(chan struct{})
	go func() { e.bank.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not stop the poller")
	}
	if !strings.Contains(e.logs.String(), `"reason":"shutdown"`) {
		t.Fatalf("no bank.poll shutdown: %s", e.logs)
	}
	// A transfer after Close is still answered, but not polled.
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
}

type refuse struct{ suffix string }

func (r refuse) RoundTrip(req *http.Request) (*http.Response, error) {
	if strings.HasSuffix(req.URL.Path, r.suffix) {
		return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	}
	return http.DefaultTransport.RoundTrip(req)
}

// Refused at dial: provably not sent, so MOM may retry here.
func TestConnectionRefusedNeverReachedBank(t *testing.T) {
	for _, step := range []string{"/acct-validate", "/funding-check", "/remit-single"} {
		e := newEnv(t, opts{transport: refuse{step}})
		want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	}
}

// S:D's X-Signature-512, checked against openssl:
// printf '%s' "$body" | openssl dgst -sha512 -hmac dev-bank-d-secret -binary | openssl base64 -A
func TestSignatureKnownAnswer(t *testing.T) {
	body := `{"meta":{"client-trace-id":"TRACE-D-001","sent-at":"2026-11-25T09:40:00+07:00"},"data":{"dest-bank-id":"022","dest-acct-no":"700192837400","rail-mode":"BIFAST"}}`
	const want = "mCZQkWh0nNTkxAnDcFN4RzHSqu8uxa7ALdHV9mxxeAwba5dyTREzlQkNSPcU+t0Wcr5v9uzcK+YiyAoDHCUclw=="
	if got := stubd.Signature(secret, []byte(body)); got != want {
		t.Fatalf("signature = %s", got)
	}
}

// The remittance carries the request's own identity and whole-rupiah amount,
// so InquireStatus needs nothing but request_id.
func TestRemitPayload(t *testing.T) {
	var mu sync.Mutex
	var bodies []string
	rec := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/remit-single") {
			b, _ := readAll(r)
			mu.Lock()
			bodies = append(bodies, string(b))
			mu.Unlock()
		}
		return http.DefaultTransport.RoundTrip(r)
	})
	e := newEnv(t, opts{transport: rec})
	req := e.request(contract.RailRTGS)
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "")
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("%d remit-single calls", len(bodies))
	}
	for _, s := range []string{`"pg-order-id":"` + req.RequestID + `"`, `"rail-mode":"RTGS"`,
		`"funding-acct-no":"` + funding + `"`, `"currency-iso":"IDR"`, `"dest-acct-name":"CV SIMULASI 7400"`} {
		if !strings.Contains(bodies[0], s) {
			t.Fatalf("remit body lacks %s: %s", s, bodies[0])
		}
	}
	// tx-amount is a JSON integer of whole rupiah, never "150000000.00" or a string.
	var sent struct {
		Data struct {
			Amount json.RawMessage `json:"tx-amount"`
		}
	}
	if err := json.Unmarshal([]byte(bodies[0]), &sent); err != nil || string(sent.Data.Amount) != "150000000" {
		t.Fatalf("tx-amount = %s, %v", sent.Data.Amount, err)
	}
}

func TestLogsMaskedAndSecretFree(t *testing.T) {
	e := std(t)
	e.bank.Transfer(e.ctx, e.bifast())
	e.next()
	e.next()
	e.waitLog(`"event":"bank.poll"`)
	out := e.logs.String()
	if strings.Count(out, `"event":"bank.call"`) < 5 || !strings.Contains(out, `"account":"********7400"`) {
		t.Fatalf("log = %s", out)
	}
	for _, leak := range []string{acct, secret, "X-Signature-512", "CV SIMULASI"} {
		if strings.Contains(out, leak) {
			t.Fatalf("log contains %q", leak)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	log := logger.New(&bytes.Buffer{}, slog.LevelDebug, "d")
	good := stubd.Config{BaseURL: "http://x", ClientKey: "k", Secret: "s", FundingAccount: "f", PollInterval: time.Second, PollFor: time.Minute}
	if _, err := stubd.New(stubd.Config{BaseURL: "http://x"}, http.DefaultClient, &reporter{}, log); err == nil {
		t.Fatal("built without credentials")
	}
	if _, err := stubd.New(good, http.DefaultClient, nil, log); err == nil {
		t.Fatal("built without a reporter: statuses would never reach MOM")
	}
	bad := good
	bad.PollInterval = 0
	if _, err := stubd.New(bad, http.DefaultClient, &reporter{}, log); err == nil {
		t.Fatal("built with a zero poll interval")
	}
	b, err := stubd.New(good, http.DefaultClient, &reporter{}, log)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func readAll(r *http.Request) ([]byte, error) {
	var buf bytes.Buffer
	_, err := buf.ReadFrom(r.Body)
	r.Body = ioNopCloser{bytes.NewReader(buf.Bytes())}
	return buf.Bytes(), err
}

type ioNopCloser struct{ *bytes.Reader }

func (ioNopCloser) Close() error { return nil }

// scripted is a bank that answers each step with a fixed body, for answers
// the simulator never gives.
func scripted(t *testing.T, answers map[string]string) *env {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		step := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		body, ok := answers[step]
		if !ok {
			t.Errorf("unexpected call to %s", step)
			w.WriteHeader(http.StatusTeapot)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return newEnv(t, opts{baseURL: srv.URL})
}

const (
	found   = `{"meta":{"status-code":"00"},"data":{"registered-owner":"CV X"}}`
	funded  = `{"meta":{"status-code":"00"},"data":{"available-liquidity":950000000,"rail-window-open":true}}`
	settled = `{"meta":{"status-code":"00"},"data":{"bank-rail-ref":"R1","tx-state":"SETTLED"}}`
)

func TestUnusualAnswers(t *testing.T) {
	for name, c := range map[string]struct {
		answers        map[string]string
		status         contract.Status
		reason         contract.Reason
		retry, reached bool
	}{
		"validate: other code": {map[string]string{"acct-validate": `{"meta":{"status-code":"05"},"data":{}}`},
			contract.StatusFailed, contract.ReasonBankRejected, true, true},
		"funding: other code, window open, funded": {map[string]string{"acct-validate": found,
			"funding-check": `{"meta":{"status-code":"09"},"data":{"available-liquidity":950000000,"rail-window-open":true}}`},
			contract.StatusFailed, contract.ReasonBankRejected, true, true},
		"funding: fields missing": {map[string]string{"acct-validate": found, "funding-check": `{"meta":{"status-code":"00"},"data":{}}`},
			contract.StatusFailed, contract.ReasonRailUnavailable, true, false},
		"remit: settled at once": {map[string]string{"acct-validate": found, "funding-check": funded, "remit-single": settled},
			contract.StatusSucceeded, "", false, true},
		"remit: rejected in the submit answer": {map[string]string{"acct-validate": found, "funding-check": funded,
			"remit-single": `{"meta":{"status-code":"00"},"data":{"tx-state":"REJECTED"}}`},
			contract.StatusUnknown, "", false, true},
		"remit: non-00 code": {map[string]string{"acct-validate": found, "funding-check": funded,
			"remit-single": `{"meta":{"status-code":"94"},"data":{"tx-state":"IN_CLEARING"}}`},
			contract.StatusUnknown, "", false, true},
	} {
		t.Run(name, func(t *testing.T) {
			e := scripted(t, c.answers)
			want(t, e.bank.Transfer(e.ctx, e.bifast()), c.status, c.reason, c.retry, c.reached, "")
			time.Sleep(30 * time.Millisecond)
			if n := len(e.rep.all()); n != 0 {
				t.Fatalf("%d reports: no poller should run", n)
			}
		})
	}
}
