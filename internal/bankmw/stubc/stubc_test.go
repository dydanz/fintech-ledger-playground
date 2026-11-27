package stubc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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

	"fintech-ledger-playground/internal/bankmw/stubc"
	"fintech-ledger-playground/internal/banksim"
	simc "fintech-ledger-playground/internal/banksim/stubc"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

const (
	apiKey, aaaSecret = "dev-bank-c-api-key", "dev-bank-c-aaa-secret"
	vault             = "VAULT-PG-881"
	acct              = "4110029384"
	tid               = "01a1d000-0000-7000-8000-000000000001"
)

type report struct {
	requestID string
	res       contract.BankResult
}

type reporter struct {
	mu   sync.Mutex
	got  []report
	fail bool
	ch   chan report
}

func (r *reporter) Report(_ context.Context, _, requestID string, res contract.BankResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("kafka down")
	}
	r.got = append(r.got, report{requestID, res})
	r.ch <- report{requestID, res}
	return nil
}

func (r *reporter) statuses() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, g := range r.got {
		s := string(g.res.Status)
		if g.res.Reason != "" {
			s += "/" + string(g.res.Reason)
		}
		out = append(out, s)
	}
	return out
}

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
	timeout          time.Duration
	pollFor          time.Duration
	ackTTL, execTTL  time.Duration
	apiKey           string
	aaaSecret        string // the BMW's; the bank's is always aaaSecret
	webhookDelay     time.Duration
	transport        http.RoundTripper
	bankURL          string // a scripted bank instead of the simulator
	recordedWebhooks *webhooks
}

// webhooks records what the bank sent the BMW, for replays.
type webhooks struct {
	mu     sync.Mutex
	bodies map[string][]byte // by path suffix
}

func (w *webhooks) get(stage string) []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.bodies[stage]
}

type env struct {
	t     *testing.T
	sim   *httptest.Server
	hooks *httptest.Server // the BMW's webhook listener
	bank  *stubc.Bank
	rep   *reporter
	logs  *syncBuffer
	ctx   context.Context
	n     int
	o     opts

	mu      sync.Mutex
	handler http.Handler
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
	if o.ackTTL == 0 {
		o.ackTTL = 2 * time.Minute
	}
	if o.execTTL == 0 {
		o.execTTL = 3 * time.Minute
	}
	if o.apiKey == "" {
		o.apiKey = apiKey
	}
	if o.aaaSecret == "" {
		o.aaaSecret = aaaSecret
	}
	e := &env{t: t, o: o, logs: &syncBuffer{}}
	e.hooks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o.recordedWebhooks != nil && r.Body != nil {
			body, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(body))
			o.recordedWebhooks.mu.Lock()
			o.recordedWebhooks.bodies[r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]] = body
			o.recordedWebhooks.mu.Unlock()
		}
		e.mu.Lock()
		h := e.handler
		e.mu.Unlock()
		h.ServeHTTP(w, r)
	}))
	t.Cleanup(e.hooks.Close)
	base := o.bankURL
	if base == "" {
		s := banksim.New(simc.Steps)
		r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "sim"), s)
		if err != nil {
			t.Fatal(err)
		}
		simc.Register(r, s, simc.Config{APIKey: apiKey, AAASecret: aaaSecret, Fee: 2500, AckTTL: o.ackTTL, ExecTTL: o.execTTL,
			WebhookDelay: o.webhookDelay})
		e.sim = httptest.NewServer(r)
		t.Cleanup(e.sim.Close)
		base = e.sim.URL
	}
	o.bankURL = base
	e.o = o
	e.start()
	return e
}

// start builds a fresh BMW process (a restart, when called again).
func (e *env) start() {
	e.t.Helper()
	log := logger.New(e.logs, slog.LevelDebug, "bank-middleware-c")
	e.rep = &reporter{ch: make(chan report, 32)}
	b, err := stubc.New(stubc.Config{BaseURL: e.o.bankURL, APIKey: e.o.apiKey, AAASecret: e.o.aaaSecret, SourceVault: vault,
		CallbackBaseURL: e.hooks.URL + stubc.WebhookPath, PollInterval: 20 * time.Millisecond, PollFor: e.o.pollFor},
		&http.Client{Timeout: e.o.timeout, Transport: e.o.transport}, e.rep, log)
	if err != nil {
		e.t.Fatal(err)
	}
	r := gin.New()
	r.Use(func(c *gin.Context) { c.Request = c.Request.WithContext(logger.WithContext(c.Request.Context(), log)) })
	b.Routes(r)
	e.mu.Lock()
	e.handler, e.bank = r, b
	e.mu.Unlock()
	e.t.Cleanup(b.Close)
	e.ctx = logger.WithContext(context.Background(), log)
}

func std(t *testing.T) *env { return newEnv(t, opts{}) }

func (e *env) request(rail string) contract.TransferRequest {
	e.n++
	return contract.TransferRequest{RequestID: "01a210a5-5380-7000-8000-0000000000" + string(rune('a'+e.n/10)) + string(rune('0'+e.n%10)),
		TransferID: tid, AttemptNo: 1, Rail: rail, Amount: contract.Amount{Value: "1500000.00", Currency: "IDR"},
		Beneficiary: contract.Beneficiary{BankCode: "013", AccountNo: acct}, IssuedAt: time.Now(), Nonce: "n"}
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

func (e *env) next() contract.BankResult {
	e.t.Helper()
	select {
	case r := <-e.rep.ch:
		return r.res
	case <-time.After(4 * time.Second):
		e.t.Fatalf("no report; got %v; logs %s", e.rep.statuses(), e.logs)
		return contract.BankResult{}
	}
}

func (e *env) waitLog(s string) {
	e.t.Helper()
	deadline := time.Now().Add(4 * time.Second)
	for !strings.Contains(e.logs.String(), s) {
		if time.Now().After(deadline) {
			e.t.Fatalf("logs lack %s: %s", s, e.logs)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// webhook posts a crafted webhook to the BMW.
func (e *env) webhook(stage, event string, body map[string]any) int {
	e.t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, e.hooks.URL+stubc.WebhookPath+"/"+stage, bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-BankC-Event", event)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
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

// T§11.3: initiate → ACCEPTED; ACK-1 → PROCESSING; aaa-verify; ACK-2;
// execute SETTLED → SUCCEEDED. One execute.
func TestHandshakeSettles(t *testing.T) {
	e := std(t)
	got := e.bank.Transfer(e.ctx, e.bifast())
	want(t, got, contract.StatusAccepted, "", false, true, "INIT_ACCEPTED")
	if !strings.HasPrefix(got.BankReference, "BSESS-") {
		t.Fatalf("bank_reference = %q, want bank_session_id", got.BankReference)
	}
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	s := e.next()
	want(t, s, contract.StatusSucceeded, "", false, true, "SETTLED")
	if !strings.HasPrefix(s.BankReference, "CORE-C-") {
		t.Fatalf("bank_reference = %q, want core_settlement_id", s.BankReference)
	}
	e.waitLog(`"event":"bank.session"`)
	if c := e.calls(); c["initiate"] != 1 || c["aaa-verify"] != 1 || c["execute"] != 1 {
		t.Fatalf("calls = %v", c)
	}
	n := 0
	for _, l := range strings.Split(e.logs.String(), "\n") {
		if strings.Contains(l, `"event":"bank.webhook"`) && strings.Contains(l, `"outcome":"success"`) {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("%d accepted webhooks logged, want 2: %s", n, e.logs)
	}
}

func TestOnlyBIFAST(t *testing.T) {
	e := std(t)
	for _, rail := range []string{contract.RailRTOL, contract.RailRTGS, contract.RailSKNBI, contract.RailIntrabank} {
		want(t, e.bank.Transfer(e.ctx, e.request(rail)), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	}
	if c := e.calls(); c["initiate"] != 0 {
		t.Fatalf("calls = %v", c)
	}
}

// ACK-2 refusals (T§11.3 (A)): FAILED, and execute is never sent.
func TestAck2Refusals(t *testing.T) {
	for code, w := range map[string]struct {
		reason contract.Reason
		retry  bool
	}{
		"ACCT_INVALID":    {contract.ReasonInvalidAccount, false},
		"ACCT_CLOSED":     {contract.ReasonAccountClosed, false},
		"REQUEST_INVALID": {contract.ReasonBankRejected, true},
	} {
		t.Run(code, func(t *testing.T) {
			e := std(t)
			e.fault(`{"match":{"step":"ack-2"},"action":"status","code":"` + code + `"}`)
			want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
			want(t, e.next(), contract.StatusProcessing, "", false, true, "")
			want(t, e.next(), contract.StatusFailed, w.reason, w.retry, true, "")
			if c := e.calls(); c["execute"] != 0 {
				t.Fatalf("executed after a refusal: %v", c)
			}
		})
	}
}

// P§5 #9, as agreed 2026-11-27: a dropped ACK-2 carries the only execution
// token, so execute can't be sent. The BMW polls until the bank says the
// deadline passed (EXPIRED), then reports FAILED OTHER, retry elsewhere.
func TestDroppedAck2EndsFailedOnceExpired(t *testing.T) {
	e := newEnv(t, opts{execTTL: 200 * time.Millisecond})
	e.fault(`{"match":{"step":"ack-2"},"action":"drop_webhook"}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	want(t, e.next(), contract.StatusFailed, contract.ReasonOther, true, true, "EXPIRED")
	if c := e.calls(); c["execute"] != 0 || c["status"] == 0 {
		t.Fatalf("calls = %v: want polls and no execute", c)
	}
}

// A dropped ACK-1: no AAA, no ACK-2, no execute; the bank expires it.
func TestDroppedAck1EndsFailedOnceExpired(t *testing.T) {
	e := newEnv(t, opts{ackTTL: 200 * time.Millisecond})
	e.fault(`{"match":{"step":"ack-1"},"action":"drop_webhook"}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "") // from the poll: AWAITING_AAA_VERIFY
	want(t, e.next(), contract.StatusFailed, contract.ReasonOther, true, true, "EXPIRED")
	if c := e.calls(); c["aaa-verify"] != 0 || c["execute"] != 0 {
		t.Fatalf("calls = %v", c)
	}
}

// Execute timeout: UNKNOWN by event, then the status poll finds SETTLED.
// Execute is sent exactly once.
func TestExecuteTimeoutThenPollSettles(t *testing.T) {
	e := newEnv(t, opts{timeout: 200 * time.Millisecond})
	e.fault(`{"match":{"step":"execute"},"action":"timeout","delay_ms":2000}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	want(t, e.next(), contract.StatusUnknown, "", false, true, "")
	want(t, e.next(), contract.StatusSucceeded, "", false, true, "SETTLED")
	if c := e.calls(); c["execute"] != 1 {
		t.Fatalf("execute sent %d times, want 1", c["execute"])
	}
}

// Execute 5xx: never retried (the token is single-use and the bank may have
// acted). The bank never executed it, so it expires: FAILED OTHER.
func TestExecute5xxNeverRetried(t *testing.T) {
	// execution_deadline has whole seconds: a TTL under 1 s can truncate to
	// the past, and the BMW then rightly refuses to execute.
	e := newEnv(t, opts{execTTL: 1500 * time.Millisecond})
	e.fault(`{"match":{"step":"execute"},"action":"status","http_status":503}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	want(t, e.next(), contract.StatusUnknown, "", false, true, "")
	want(t, e.next(), contract.StatusFailed, contract.ReasonOther, true, true, "EXPIRED")
	if c := e.calls(); c["execute"] != 1 {
		t.Fatalf("execute sent %d times, want 1", c["execute"])
	}
}

func TestExecutePending(t *testing.T) {
	for action, final := range map[string]struct {
		status contract.Status
		reason contract.Reason
		retry  bool
	}{
		"pending_then_success": {contract.StatusSucceeded, "", false},
		"pending_then_fail":    {contract.StatusFailed, contract.ReasonBankRejected, true},
	} {
		t.Run(action, func(t *testing.T) {
			e := std(t)
			e.fault(`{"match":{"step":"execute"},"action":"` + action + `","polls":2}`)
			want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
			want(t, e.next(), contract.StatusProcessing, "", false, true, "")
			want(t, e.next(), final.status, final.reason, final.retry, true, "")
			e.waitLog(`"event":"bank.session"`)
			if got := e.rep.statuses(); len(got) != 2 {
				t.Fatalf("reports = %v, want PROCESSING once, then the final one", got)
			}
		})
	}
}

// P§5 #10: a restart mid-handshake loses the session. ACK-2 then reaches a
// process without it: 404, never executed. Status inquiry reads the bank.
func TestRestartMidHandshakeNeverExecutes(t *testing.T) {
	e := newEnv(t, opts{execTTL: 400 * time.Millisecond})
	e.fault(`{"match":{"step":"ack-2"},"action":"delay","delay_ms":300}`)
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	e.waitLog(`"step":"aaa-verify","http_status":202`) // AAA passed: the bank will send ACK-2
	e.bank.Close()                                     // the process stops
	e.waitLog(`"reason":"shutdown"`)
	e.start() // and starts again, with no sessions
	e.waitLog(`"reason":"unknown_session"`)
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusProcessing, "", false, true, "AWAITING_EXECUTE")
	time.Sleep(500 * time.Millisecond)
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusFailed, contract.ReasonOther, true, true, "EXPIRED")
	if c := e.calls(); c["execute"] != 0 {
		t.Fatalf("executed after a restart: %v", c)
	}
}

// An initiate timeout is UNKNOWN; the BMW drops the session, so a late ACK-1
// is refused and nothing is ever executed.
func TestInitiateTimeoutIsUnknownAndNeverExecutes(t *testing.T) {
	e := newEnv(t, opts{timeout: 200 * time.Millisecond})
	e.fault(`{"match":{"step":"initiate"},"action":"timeout","delay_ms":2000}`)
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusUnknown, "", false, true, "")
	e.waitLog(`"reason":"unknown_session"`)
	if c := e.calls(); c["aaa-verify"] != 0 || c["execute"] != 0 {
		t.Fatalf("calls = %v", c)
	}
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusProcessing, "", false, true, "AWAITING_AAA_VERIFY")
}

func TestInitiateRefusals(t *testing.T) {
	e := newEnv(t, opts{apiKey: "someone-else"})
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonAuthError, true, false, "")
	for _, code := range []int{500, 503} {
		e := std(t)
		e.fault(`{"match":{"step":"initiate"},"action":"status","http_status":` + strconv.Itoa(code) + `}`)
		want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusUnknown, "", false, true, "")
	}
	r := refuse{}
	e = newEnv(t, opts{transport: r})
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
}

// The same attempt sent again while its session lives: ACCEPTED, no second
// initiate, one handshake.
func TestRepeatTransferOneSession(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":3}`)
	req := e.bifast()
	first := e.bank.Transfer(e.ctx, req)
	want(t, first, contract.StatusAccepted, "", false, true, "")
	again := e.bank.Transfer(e.ctx, req)
	want(t, again, contract.StatusAccepted, "", false, true, "")
	if again.BankReference != first.BankReference {
		t.Fatalf("references %q and %q", first.BankReference, again.BankReference)
	}
	e.next()
	want(t, e.next(), contract.StatusSucceeded, "", false, true, "")
	if c := e.calls(); c["initiate"] != 1 || c["execute"] != 1 {
		t.Fatalf("calls = %v", c)
	}
}

// A replayed ACK-2 (the bank delivering twice) never executes twice.
func TestReplayedAck2ExecutesOnce(t *testing.T) {
	rec := &webhooks{bodies: map[string][]byte{}}
	e := newEnv(t, opts{recordedWebhooks: rec})
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":10}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.next()
	deadline := time.Now().Add(3 * time.Second)
	for e.calls()["execute"] == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.get("ack-2"), &body); err != nil {
		t.Fatal(err)
	}
	if code := e.webhook("ack-2", "ACK-2", body); code != http.StatusOK {
		t.Fatalf("replayed ACK-2: %d, want 200 (received, ignored)", code)
	}
	want(t, e.next(), contract.StatusSucceeded, "", false, true, "")
	if c := e.calls(); c["execute"] != 1 {
		t.Fatalf("execute sent %d times", c["execute"])
	}
	if !strings.Contains(e.logs.String(), `"duplicate":true`) {
		t.Fatalf("replay not logged as a duplicate: %s", e.logs)
	}
}

// Webhooks are accepted only for a live session in this process, in its
// stage, before expires_at, and (ACK-2) for our own account and amount.
func TestWebhookChecks(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"ack-1"},"action":"delay","delay_ms":3000}`)
	req := e.bifast()
	res := e.bank.Transfer(e.ctx, req)
	want(t, res, contract.StatusAccepted, "", false, true, "")
	sess, ref := res.BankReference, req.RequestID
	future := time.Now().Add(time.Minute).Format(time.RFC3339)
	ack1 := func(mod func(map[string]any)) map[string]any {
		b := map[string]any{"ack_code": "OK ACK-1", "bank_session_id": sess, "pg_trx_ref": ref, "aaa_challenge_nonce": "n1",
			"required_hash_alg": "SHA-256", "canonical_fields_order": simc.CanonicalFieldsOrder, "expires_at": future}
		if mod != nil {
			mod(b)
		}
		return b
	}
	for name, c := range map[string]struct {
		stage, event string
		body         map[string]any
		code         int
		reason       string
	}{
		"unknown pg_trx_ref":     {"ack-1", "ACK-1", ack1(func(b map[string]any) { b["pg_trx_ref"] = "01a210a5-5380-7000-8000-00000000dead" }), 404, "unknown_session"},
		"wrong bank_session_id":  {"ack-1", "ACK-1", ack1(func(b map[string]any) { b["bank_session_id"] = "BSESS-0000001" }), 404, "unknown_session"},
		"event header mismatch":  {"ack-1", "ACK-2", ack1(nil), 400, "invalid_input"},
		"unknown hash algorithm": {"ack-1", "ACK-1", ack1(func(b map[string]any) { b["required_hash_alg"] = "MD5" }), 400, "invalid_input"},
		"unknown canonical field": {"ack-1", "ACK-1", ack1(func(b map[string]any) {
			b["canonical_fields_order"] = []string{"pg_trx_ref", "beneficiary_name"}
		}), 400, "invalid_input"},
		"expired": {"ack-1", "ACK-1", ack1(func(b map[string]any) { b["expires_at"] = time.Now().Add(-time.Second).Format(time.RFC3339) }), 410, "expired"},
		"ACK-2 before ACK-1": {"ack-2", "ACK-2", map[string]any{"ack_code": "OK ACK-2", "bank_session_id": sess, "pg_trx_ref": ref, "request_valid": true,
			"acct_valid": true, "bank_acct_no": acct, "remit_amt": 1500000, "execution_token": "T", "execution_deadline": future}, 409, "wrong_stage"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := e.webhook(c.stage, c.event, c.body); got != c.code {
				t.Fatalf("status %d, want %d", got, c.code)
			}
			if !strings.Contains(e.logs.String(), `"reason":"`+c.reason+`"`) {
				t.Fatalf("no bank.webhook %s: %s", c.reason, e.logs)
			}
		})
	}
	if c := e.calls(); c["aaa-verify"] != 0 || c["execute"] != 0 {
		t.Fatalf("a refused webhook moved the handshake: %v", c)
	}
}

// ACK-2 for another account or amount than we initiated: never executed.
func TestAck2MismatchNeverExecutes(t *testing.T) {
	for name, mod := range map[string]func(map[string]any){
		"account": func(b map[string]any) { b["bank_acct_no"] = "4110029999" },
		"amount":  func(b map[string]any) { b["remit_amt"] = 9500000 },
	} {
		t.Run(name, func(t *testing.T) {
			rec := &webhooks{bodies: map[string][]byte{}}
			e := newEnv(t, opts{recordedWebhooks: rec, execTTL: 300 * time.Millisecond})
			e.fault(`{"match":{"step":"ack-2"},"action":"drop_webhook"}`)
			want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
			e.next()
			// Build the ACK-2 the bank would have sent, from the live session.
			var ack1 map[string]any
			_ = json.Unmarshal(rec.get("ack-1"), &ack1)
			body := map[string]any{"ack_code": "OK ACK-2", "bank_session_id": ack1["bank_session_id"], "pg_trx_ref": ack1["pg_trx_ref"],
				"request_valid": true, "acct_valid": true, "bank_acct_no": acct, "remit_amt": 1500000, "execution_token": "EXEC-TOK-X",
				"execution_deadline": time.Now().Add(time.Minute).Format(time.RFC3339)}
			mod(body)
			deadline := time.Now().Add(2 * time.Second)
			for e.calls()["aaa-verify"] == 0 && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			if code := e.webhook("ack-2", "ACK-2", body); code != http.StatusBadRequest {
				t.Fatalf("mismatched ACK-2: %d, want 400", code)
			}
			e.waitLog(`"reason":"mismatch"`)
			want(t, e.next(), contract.StatusFailed, contract.ReasonOther, true, true, "EXPIRED")
			if c := e.calls(); c["execute"] != 0 {
				t.Fatalf("executed a mismatched ACK-2: %v", c)
			}
		})
	}
}

func TestHandshakeDeadline(t *testing.T) {
	e := newEnv(t, opts{pollFor: 150 * time.Millisecond})
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":-1}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.waitLog(`"reason":"deadline"`)
	for _, s := range e.rep.statuses() {
		if s != "PROCESSING" {
			t.Fatalf("reported %s before a terminal state", s)
		}
	}
}

func TestLostTerminalReportRecoveredByInquiry(t *testing.T) {
	e := std(t)
	e.rep.fail = true
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "")
	e.waitLog(`"reason":"report_failed"`)
	want(t, e.bank.InquireStatus(e.ctx, req.TransferID, req.RequestID), contract.StatusSucceeded, "", false, true, "SETTLED")
}

func TestCloseStopsSessions(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":-1}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.next()
	done := make(chan struct{})
	go func() { e.bank.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not stop the session")
	}
	e.waitLog(`"reason":"shutdown"`)
	before := e.calls()["initiate"]
	// No handshake can run any more: refuse before initiating.
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	if e.calls()["initiate"] != before {
		t.Fatal("initiated after Close")
	}
}

// T§11.3 status mapping, including the (A) final_state values.
func TestInquireStatusMapping(t *testing.T) {
	for state, w := range map[string]struct {
		status  contract.Status
		reason  contract.Reason
		retry   bool
		reached bool
	}{
		"SETTLED":                      {contract.StatusSucceeded, "", false, true},
		"PROCESSING":                   {contract.StatusProcessing, "", false, true},
		"AWAITING_ACK_1":               {contract.StatusProcessing, "", false, true},
		"AWAITING_AAA_VERIFY":          {contract.StatusProcessing, "", false, true},
		"VALIDATING_ACCOUNT_FOR_ACK_2": {contract.StatusProcessing, "", false, true},
		"AWAITING_EXECUTE":             {contract.StatusProcessing, "", false, true},
		"FAILED":                       {contract.StatusFailed, contract.ReasonBankRejected, true, true},
		"EXPIRED":                      {contract.StatusFailed, contract.ReasonOther, true, true},
		"SOMETHING_NEW":                {contract.StatusUnknown, "", false, true},
	} {
		t.Run(state, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(`{"pg_trx_ref":"x","final_state":"` + state + `","core_settlement_id":"CORE-1"}`))
			}))
			defer srv.Close()
			e := newEnv(t, opts{bankURL: srv.URL})
			got := e.bank.InquireStatus(e.ctx, tid, "01a210a5-5380-7000-8000-0000000000ff")
			want(t, got, w.status, w.reason, w.retry, w.reached, state)
			if state == "SETTLED" && got.BankReference != "CORE-1" {
				t.Fatalf("bank_reference = %q", got.BankReference)
			}
		})
	}
	e := std(t)
	want(t, e.bank.InquireStatus(e.ctx, tid, "01a210a5-5380-7000-8000-0000000000ff"), contract.StatusFailed, contract.ReasonOther, true, false, "NOT_FOUND")
	e.fault(`{"match":{"step":"status"},"action":"status","http_status":500}`)
	want(t, e.bank.InquireStatus(e.ctx, tid, "01a210a5-5380-7000-8000-0000000000ff"), contract.StatusUnknown, "", false, true, "")
}

// The AAA digest and signature from the contract's own example (S:C §3),
// checked against openssl:
//
//	d=$(printf '%s' 'TRX-C-20261125-7701|013|4110029384|1500000|n9f8a7c6e5d4b3a2' | openssl dgst -sha256 -r | cut -d' ' -f1)
//	printf '%s' "$d" | openssl dgst -sha256 -hmac dev-bank-c-aaa-secret -binary | openssl base64 -A
func TestAAAKnownAnswer(t *testing.T) {
	values := map[string]string{"pg_trx_ref": "TRX-C-20261125-7701", "dest_bank_code": "013", "bank_acct_no": "4110029384",
		"remit_amt": "1500000", "aaa_challenge_nonce": "n9f8a7c6e5d4b3a2"}
	d, err := stubc.PayloadHash(values, simc.CanonicalFieldsOrder)
	if err != nil || d != "81f7a7b8d6da846c1a78fcb1c3bc5aecd10cf4f35770c215b885574629bef3ba" {
		t.Fatalf("digest = %s, %v", d, err)
	}
	if s := stubc.AAASignature(aaaSecret, d); s != "9Ek3PUorBs89uqelGRP4RiyG0oiS8z5ZMZrT8YLFyI0=" {
		t.Fatalf("signature = %s", s)
	}
	if _, err := stubc.PayloadHash(values, []string{"pg_trx_ref", "beneficiary_name"}); err == nil {
		t.Fatal("hashed a field it doesn't know")
	}
}

func TestLogsSecretFree(t *testing.T) {
	rec := &webhooks{bodies: map[string][]byte{}}
	e := newEnv(t, opts{recordedWebhooks: rec})
	e.bank.Transfer(e.ctx, e.bifast())
	e.next()
	e.next()
	e.waitLog(`"event":"bank.session"`)
	out := e.logs.String()
	var ack1, ack2 map[string]any
	_ = json.Unmarshal(rec.get("ack-1"), &ack1)
	_ = json.Unmarshal(rec.get("ack-2"), &ack2)
	if !strings.Contains(out, `"account":"******9384"`) {
		t.Fatalf("no masked account: %s", out)
	}
	nonce, _ := ack1["aaa_challenge_nonce"].(string)
	token, _ := ack2["execution_token"].(string)
	name, _ := ack2["resolved_acct_name"].(string)
	for _, leak := range []string{acct, apiKey, aaaSecret, "Bearer", nonce, token, name} {
		if leak == "" || strings.Contains(out, leak) {
			t.Fatalf("log contains %q", leak)
		}
	}
}

func TestNewValidatesConfig(t *testing.T) {
	log := logger.New(&bytes.Buffer{}, slog.LevelDebug, "c")
	good := stubc.Config{BaseURL: "http://x", APIKey: "k", AAASecret: "s", SourceVault: "v", CallbackBaseURL: "http://me/callbacks/bank-c",
		PollInterval: time.Second, PollFor: time.Minute}
	if _, err := stubc.New(stubc.Config{BaseURL: "http://x"}, http.DefaultClient, &reporter{}, log); err == nil {
		t.Fatal("built without credentials")
	}
	if _, err := stubc.New(good, http.DefaultClient, nil, log); err == nil {
		t.Fatal("built without a reporter")
	}
	bad := good
	bad.PollFor = 0
	if _, err := stubc.New(bad, http.DefaultClient, &reporter{}, log); err == nil {
		t.Fatal("built with no poll duration")
	}
	b, err := stubc.New(good, http.DefaultClient, &reporter{}, log)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
}

type refuse struct{}

func (refuse) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

// The bank refuses our AAA (wrong secret, or a tampered hash): no token can
// be issued, so nothing can move. FAILED AUTH_ERROR, retry elsewhere.
func TestAAARefusedIsAuthError(t *testing.T) {
	e := newEnv(t, opts{aaaSecret: "not-the-banks"})
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	want(t, e.next(), contract.StatusFailed, contract.ReasonAuthError, true, true, "AAA_FAILED")
	if c := e.calls(); c["execute"] != 0 {
		t.Fatalf("calls = %v", c)
	}
}

// An ACK-2 that arrives past its own execution_deadline: execute is never
// sent, so nothing moved. FAILED OTHER, retry elsewhere.
func TestAck2PastDeadlineNeverExecutes(t *testing.T) {
	rec := &webhooks{bodies: map[string][]byte{}}
	e := newEnv(t, opts{recordedWebhooks: rec})
	e.fault(`{"match":{"step":"ack-2"},"action":"drop_webhook"}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.next()
	var ack1 map[string]any
	_ = json.Unmarshal(rec.get("ack-1"), &ack1)
	deadline := time.Now().Add(2 * time.Second)
	for e.calls()["aaa-verify"] == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	body := map[string]any{"ack_code": "OK ACK-2", "bank_session_id": ack1["bank_session_id"], "pg_trx_ref": ack1["pg_trx_ref"],
		"request_valid": true, "acct_valid": true, "bank_acct_no": acct, "remit_amt": 1500000, "execution_token": "EXEC-TOK-X",
		"execution_deadline": time.Now().Add(-time.Second).Format(time.RFC3339)}
	if code := e.webhook("ack-2", "ACK-2", body); code != http.StatusOK {
		t.Fatalf("ACK-2: %d", code)
	}
	want(t, e.next(), contract.StatusFailed, contract.ReasonOther, true, true, "DEADLINE_PASSED")
	if c := e.calls(); c["execute"] != 0 {
		t.Fatalf("executed past the deadline: %v", c)
	}
}

// MOM may retry an attempt that never reached the bank on the same node
// (reached_bank false): the failed initiate must leave no session behind.
func TestRetryAfterRefusedInitiate(t *testing.T) {
	var mu sync.Mutex
	refused := 0
	e := newEnv(t, opts{transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/initiate") && refused == 0 {
			refused++
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
		}
		return http.DefaultTransport.RoundTrip(r)
	})})
	req := e.bifast()
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusFailed, contract.ReasonRailUnavailable, true, false, "")
	want(t, e.bank.Transfer(e.ctx, req), contract.StatusAccepted, "", false, true, "INIT_ACCEPTED")
	if c := e.calls(); c["initiate"] != 1 {
		t.Fatalf("calls = %v: the retry must initiate", c)
	}
}

// After initiate, "not found" from the poll is never reported as FAILED:
// MOM would fail over and could pay twice. The session runs to its deadline.
func TestSessionNeverReportsNotFound(t *testing.T) {
	e := newEnv(t, opts{pollFor: 200 * time.Millisecond})
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":-1}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	resp, err := http.Post(e.sim.URL+"/_sim/reset", "application/json", nil) // the bank forgets the transfer
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	e.waitLog(`"reason":"deadline"`)
	for _, s := range e.rep.statuses() {
		if strings.HasPrefix(s, "FAILED") {
			t.Fatalf("reported %s", s)
		}
	}
}

// execute 410 EXECUTION_EXPIRED: the bank says it never executed. FAILED.
func TestExecuteExpiredIsFailed(t *testing.T) {
	e := std(t)
	e.fault(`{"match":{"step":"execute"},"action":"status","http_status":410,"code":"EXECUTION_EXPIRED"}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	want(t, e.next(), contract.StatusFailed, contract.ReasonOther, true, true, "EXECUTION_EXPIRED")
}

// The bank delivering ACK-2 several times: each is answered at once, and
// execute is sent once.
func TestManyReplayedAck2sAnswered(t *testing.T) {
	rec := &webhooks{bodies: map[string][]byte{}}
	e := newEnv(t, opts{recordedWebhooks: rec})
	e.fault(`{"match":{"step":"execute"},"action":"pending_then_success","polls":10}`)
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	e.next()
	deadline := time.Now().Add(3 * time.Second)
	for e.calls()["execute"] == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	client := &http.Client{Timeout: time.Second}
	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest(http.MethodPost, e.hooks.URL+stubc.WebhookPath+"/ack-2", bytes.NewReader(rec.get("ack-2")))
		req.Header.Set("X-BankC-Event", "ACK-2")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("replay %d: %v", i+1, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("replay %d: %d", i+1, resp.StatusCode)
		}
	}
	if c := e.calls(); c["execute"] != 1 {
		t.Fatalf("execute sent %d times", c["execute"])
	}
}

// A refusing ACK-2 is in flight while the poll already reads FAILED: the
// session waits for the webhook's precise reason (INVALID_ACCOUNT, no
// retry elsewhere) rather than the poll's BANK_REJECTED.
func TestAck2ReasonBeatsPolledFailed(t *testing.T) {
	var e *env
	var once sync.Once
	var mu sync.Mutex
	var sess, ref string
	bank := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/initiate"):
			var in map[string]any
			_ = json.NewDecoder(r.Body).Decode(&in)
			mu.Lock()
			sess, ref = "BSESS-0000042", in["pg_trx_ref"].(string)
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"ack_stage":"INIT_ACCEPTED","bank_session_id":"BSESS-0000042","workflow_state":"AWAITING_ACK_1"}`))
			go func() {
				time.Sleep(10 * time.Millisecond)
				e.webhook("ack-1", "ACK-1", map[string]any{"bank_session_id": sess, "pg_trx_ref": ref, "aaa_challenge_nonce": "n1",
					"required_hash_alg": "SHA-256", "canonical_fields_order": simc.CanonicalFieldsOrder,
					"expires_at": time.Now().Add(time.Minute).Format(time.RFC3339)})
			}()
		case strings.HasSuffix(r.URL.Path, "/aaa-verify"):
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"aaa_status":"AAA_PASSED"}`))
		case strings.Contains(r.URL.Path, "/status/"):
			_, _ = w.Write([]byte(`{"final_state":"FAILED"}`))
			once.Do(func() { // the refusing ACK-2 lands after the first FAILED poll
				go e.webhook("ack-2", "ACK-2", map[string]any{"bank_session_id": sess, "pg_trx_ref": ref, "request_valid": true,
					"acct_valid": false, "acct_status": "NOT_FOUND"})
			})
		default:
			t.Errorf("unexpected %s", r.URL.Path)
		}
	}))
	defer bank.Close()
	e = newEnv(t, opts{bankURL: bank.URL})
	want(t, e.bank.Transfer(e.ctx, e.bifast()), contract.StatusAccepted, "", false, true, "")
	want(t, e.next(), contract.StatusProcessing, "", false, true, "")
	want(t, e.next(), contract.StatusFailed, contract.ReasonInvalidAccount, false, true, "NOT_FOUND")
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
