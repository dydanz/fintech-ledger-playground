//go:build integration

package service_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/bmwclient"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/routing"
	"fintech-ledger-playground/internal/moneyout/service"
	"fintech-ledger-playground/internal/moneyout/signing"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

// Three bank middlewares, all BIFAST: bmw-a, then bmw-c, then bmw-d.
const dispatchRoutes = `
version: 1
currency: IDR
rail_order: [BIFAST]
bank_middlewares:
  - {name: bmw-a, rails: [BIFAST], same_node_retries: 2}
  - {name: bmw-c, rails: [BIFAST]}
  - {name: bmw-d, rails: [BIFAST]}
limits:
  BIFAST: {max: 250000000}
rules: [bmw_enabled, rail_capability, amount_rail_limits]
`

var (
	succeeded   = contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: "REF-OK", BankRawCode: "00"}
	accepted    = contract.BankResult{Status: contract.StatusAccepted, ReachedBank: true, BankReference: "REF-ACC"}
	processing  = contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true}
	unknownRes  = contract.BankResult{Status: contract.StatusUnknown, ReachedBank: true}
	railDown    = contract.BankResult{Status: contract.StatusFailed, Reason: contract.ReasonRailUnavailable, RetryElsewhere: true, ReachedBank: true, BankRawCode: "503"}
	invalidAcct = contract.BankResult{Status: contract.StatusFailed, Reason: contract.ReasonInvalidAccount, ReachedBank: true, BankRawCode: "14"}
)

// scripted is one bank middleware's bank: Transfer answers in turn (the
// last one repeats), InquireStatus answers inquiry.
type scripted struct {
	mu         sync.Mutex
	answers    []contract.BankResult
	inquiry    contract.BankResult
	delay      time.Duration
	onTransfer func(contract.TransferRequest) // runs before answering
	transfers  []contract.TransferRequest
	inquiries  int
}

func (s *scripted) Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult {
	s.mu.Lock()
	s.transfers = append(s.transfers, req)
	res := s.answers[min(len(s.transfers), len(s.answers))-1]
	d, hook := s.delay, s.onTransfer
	s.mu.Unlock()
	if hook != nil {
		hook(req)
	}
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
	return res
}

func (s *scripted) InquireStatus(context.Context, string, string) contract.BankResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inquiries++
	return s.inquiry
}

func (s *scripted) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.transfers)
}

type denv struct {
	*env
	banks    map[string]*scripted
	urls     map[string]string
	servers  map[string]*httptest.Server
	router   *service.Router
	dispatch *service.Dispatch
}

type dopts struct {
	timeout    time.Duration
	inquiryFor time.Duration
	down       []string // bank middlewares not listening
	foreignKey []string // bank middlewares that trust another MOM key
	retryBase  time.Duration
}

func newDispatchEnv(t *testing.T, o dopts) *denv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	if o.timeout == 0 {
		o.timeout = 2 * time.Second
	}
	if o.inquiryFor == 0 {
		o.inquiryFor = 5 * time.Second
	}
	if o.retryBase == 0 {
		o.retryBase = 10 * time.Millisecond
	}
	e := &denv{env: newEnv(t), banks: map[string]*scripted{}, urls: map[string]string{}, servers: map[string]*httptest.Server{}}
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	for _, name := range []string{"bmw-a", "bmw-c", "bmw-d"} {
		b := &scripted{answers: []contract.BankResult{succeeded}, inquiry: succeeded}
		e.banks[name] = b
		if contains(o.down, name) {
			e.urls[name] = "http://127.0.0.1:1" // nothing listens
			continue
		}
		key := pub
		if contains(o.foreignKey, name) {
			key = otherPub
		}
		r, err := bankmw.NewRouter(bankmw.Deps{Name: name, Logger: logger.New(&bytes.Buffer{}, slog.LevelDebug, name), Bank: b,
			Verifier: signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": key}, 5*time.Minute, 1000)})
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(r)
		t.Cleanup(srv.Close)
		e.urls[name], e.servers[name] = srv.URL, srv
	}
	c, err := routing.Parse([]byte(dispatchRoutes))
	if err != nil {
		t.Fatal(err)
	}
	client := bmwclient.New(signing.NewSigner("mom-1", priv), e.urls, &http.Client{Timeout: o.timeout})
	e.router = service.NewRouter(e.store, staticRoutes{c}, time.Now)
	e.dispatch = service.NewDispatch(e.store, staticRoutes{c}, client, service.DispatchConfig{
		InquiryInterval: 20 * time.Millisecond, InquiryFor: o.inquiryFor, StaleDispatch: 300 * time.Millisecond,
		RetryBase: o.retryBase, RetryMax: 4 * o.retryBase, Concurrency: 8})
	return e
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// drive runs the jobs until the transfer reaches one of states.
func (e *denv) drive(id string, states ...transfer.State) repository.Transfer {
	e.t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for {
		for _, run := range []func(context.Context) error{e.router.Run, e.dispatch.RunDispatch, e.dispatch.RunInquiries} {
			if err := run(e.ctx); err != nil {
				e.t.Fatal(err)
			}
		}
		tr, _, err := e.store.GetTransfer(e.ctx, id)
		if err != nil {
			e.t.Fatal(err)
		}
		for _, s := range states {
			if tr.State == s {
				return tr
			}
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("%s stuck in %s, want %v; logs %s", id, tr.State, states, e.logs)
		}
		time.Sleep(15 * time.Millisecond)
	}
}

func (e *denv) attempts(id string) []repository.Attempt {
	e.t.Helper()
	_, as, err := e.store.GetTransfer(e.ctx, id)
	if err != nil {
		e.t.Fatal(err)
	}
	return as
}

func (e *denv) results(id string) []contract.TransferResult {
	e.t.Helper()
	var out []contract.TransferResult
	for _, r := range e.outboxRows(id) {
		if r.topic == contract.TopicResults {
			var res contract.TransferResult
			if err := r.env.DecodePayload(&res); err != nil {
				e.t.Fatal(err)
			}
			out = append(out, res)
		}
	}
	return out
}

func (e *denv) journal(id string) []string {
	var out []string
	for _, r := range e.outboxRows(id) {
		if r.topic == contract.TopicJournal {
			out = append(out, r.env.EventType)
		}
	}
	return out
}

// bankEvent is what a bank middleware publishes on money-out.bank-events.v1.
func bankEvent(t *testing.T, transferID, requestID, bmw string, res contract.BankResult) contract.Envelope {
	t.Helper()
	ev, err := contract.NewEvent(contract.EventBankStatusReported, contract.StreamTransfer, transferID, 0, time.Now(), "",
		contract.BankStatusReported{TransferID: transferID, RequestID: requestID, BankMiddleware: bmw, Result: res, ReportedAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func (e *denv) oneResult(id string, status contract.Status, reason contract.Reason, bmw string) contract.TransferResult {
	e.t.Helper()
	rs := e.results(id)
	if len(rs) != 1 {
		e.t.Fatalf("%d results to the core, want exactly 1: %+v", len(rs), rs)
	}
	r := rs[0]
	if r.Status != status || r.Reason != reason || r.BankMiddleware != bmw {
		e.t.Fatalf("result = %+v, want %s %s %s", r, status, reason, bmw)
	}
	return r
}

func TestDispatchSyncSuccess(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	id := e.receiveAt("022", "750965.00")
	tr := e.drive(id, transfer.Succeeded)
	r := e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
	if r.Rail != "BIFAST" || r.BankReference != "REF-OK" || r.Attempts != 1 || tr.CoreAck != "pending" {
		t.Fatalf("result %+v core_ack %s", r, tr.CoreAck)
	}
	got := e.banks["bmw-a"].transfers[0]
	if got.TransferID != id || got.AttemptNo != 1 || got.Amount.Value != "750965.00" || got.Rail != "BIFAST" || got.Beneficiary.BankCode != "022" {
		t.Fatalf("bank got %+v", got)
	}
	if id, err := uuid.Parse(got.RequestID); err != nil || id.Version() != 7 {
		t.Fatalf("request_id %q is not a UUIDv7", got.RequestID)
	}
	if j := strings.Join(e.journal(id), " "); j != "TransferReceived TransferRouted AttemptDispatching TransferSucceeded" {
		t.Fatalf("journal = %s", j)
	}
	if as := e.attempts(id); len(as) != 1 || as[0].Status != "SUCCEEDED" || as[0].BankReference != "REF-OK" || as[0].BankMiddleware != "bmw-a" {
		t.Fatalf("attempts = %+v", as)
	}
}

// P§5 #4: bank A times out, the inquiry later finds it settled → SUCCEEDED,
// and no other bank is ever called.
func TestTimeoutThenInquirySucceedsNoOtherBank(t *testing.T) {
	e := newDispatchEnv(t, dopts{timeout: 200 * time.Millisecond})
	e.banks["bmw-a"].delay = time.Second
	e.banks["bmw-a"].inquiry = unknownRes
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Unknown)
	e.banks["bmw-a"].mu.Lock()
	e.banks["bmw-a"].inquiry = succeeded
	e.banks["bmw-a"].mu.Unlock()
	e.drive(id, transfer.Succeeded)
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
	if e.banks["bmw-c"].calls()+e.banks["bmw-d"].calls() != 0 || e.banks["bmw-a"].calls() != 1 {
		t.Fatalf("calls a=%d c=%d d=%d", e.banks["bmw-a"].calls(), e.banks["bmw-c"].calls(), e.banks["bmw-d"].calls())
	}
}

// P§5 #5: A definitively rejects with RAIL_UNAVAILABLE → failover → C succeeds.
func TestRailUnavailableFailsOver(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{railDown}
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	r := e.oneResult(id, contract.StatusSucceeded, "", "bmw-c")
	if r.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", r.Attempts)
	}
	as := e.attempts(id)
	if len(as) != 2 || as[0].Status != "FAILED" || as[0].Reason != "RAIL_UNAVAILABLE" || as[1].BankMiddleware != "bmw-c" {
		t.Fatalf("attempts = %+v", as)
	}
}

// P§5 #6: INVALID_ACCOUNT → FAILED, no failover.
func TestInvalidAccountNoFailover(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{invalidAcct}
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Failed)
	e.oneResult(id, contract.StatusFailed, contract.ReasonInvalidAccount, "bmw-a")
	if e.banks["bmw-c"].calls() != 0 {
		t.Fatal("failed over on INVALID_ACCOUNT")
	}
}

// P§5 #7: pending forever → NEEDS_MANUAL_REVIEW at the inquiry deadline, and
// nothing terminal goes to the core.
func TestPendingForeverParksForReview(t *testing.T) {
	e := newDispatchEnv(t, dopts{inquiryFor: 300 * time.Millisecond})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.NeedsManualReview)
	if rs := e.results(id); len(rs) != 0 {
		t.Fatalf("results to the core: %+v", rs)
	}
	if !strings.Contains(e.logs.String(), `"reason":"inquiry_deadline"`) {
		t.Fatalf("no transfer.review: %s", e.logs)
	}
	if e.banks["bmw-a"].inquiries == 0 || e.banks["bmw-c"].calls() != 0 {
		t.Fatalf("inquiries %d, c calls %d", e.banks["bmw-a"].inquiries, e.banks["bmw-c"].calls())
	}
}

// UNKNOWN never fails over (P§3.2): it resolves by inquiry, or parks.
func TestUnknownNeverFailsOver(t *testing.T) {
	e := newDispatchEnv(t, dopts{timeout: 150 * time.Millisecond, inquiryFor: 400 * time.Millisecond})
	e.banks["bmw-a"].delay = time.Second
	e.banks["bmw-a"].inquiry = unknownRes
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.NeedsManualReview)
	if e.banks["bmw-c"].calls()+e.banks["bmw-d"].calls() != 0 {
		t.Fatal("an UNKNOWN attempt failed over")
	}
	if len(e.results(id)) != 0 {
		t.Fatal("a terminal result for an UNKNOWN transfer")
	}
}

// P§5 #12: four MOM dispatchers at once → one dispatch per transfer.
func TestConcurrentDispatchersDispatchOnce(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].delay = 50 * time.Millisecond
	var ids []string
	for i := 0; i < 10; i++ {
		ids = append(ids, e.receiveAt("022", "750965.00"))
	}
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.dispatch.RunDispatch(e.ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	per := map[string]int{}
	for _, req := range e.banks["bmw-a"].transfers {
		per[req.TransferID]++
	}
	for _, id := range ids {
		if per[id] != 1 {
			t.Fatalf("%s dispatched %d times", id, per[id])
		}
		e.drive(id, transfer.Succeeded)
		e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
	}
}

// Review Focus 1: a SUCCEEDED for attempt 1 after MOM failed over to
// attempt 2. It must not finish the transfer silently: journaled, the
// transfer parked (a possible double payment), and an error to alert on.
func TestLateEventForSupersededAttemptParksForReview(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{railDown}
	e.banks["bmw-c"].answers = []contract.BankResult{accepted}
	e.banks["bmw-c"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Accepted, transfer.Processing)
	first := e.attempts(id)[0]
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, first.RequestID, "bmw-a", succeeded)); err != nil {
		t.Fatal(err)
	}
	tr, _, _ := e.store.GetTransfer(e.ctx, id)
	if tr.State != transfer.NeedsManualReview {
		t.Fatalf("state %s, want NEEDS_MANUAL_REVIEW", tr.State)
	}
	j := strings.Join(e.journal(id), " ")
	if !strings.HasSuffix(j, "BankStatusReceived TransferNeedsReview") {
		t.Fatalf("journal = %s", j)
	}
	if len(e.results(id)) != 0 {
		t.Fatal("a result went to the core")
	}
	if !strings.Contains(e.logs.String(), `"event":"transfer.late_event"`) || !strings.Contains(e.logs.String(), `"reason":"possible_double_payment"`) ||
		!strings.Contains(e.logs.String(), `"level":"error"`) {
		t.Fatalf("no error-level transfer.late_event: %s", e.logs)
	}
	// Attempt 2 stays parked even when it settles: ops decide (Q3).
	cur := e.attempts(id)[1]
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, cur.RequestID, "bmw-c", succeeded)); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := e.store.GetTransfer(e.ctx, id); tr.State != transfer.NeedsManualReview || len(e.results(id)) != 0 {
		t.Fatalf("a parked double payment finished: %s, %d results", tr.State, len(e.results(id)))
	}
}

// The same, after the transfer already SUCCEEDED on attempt 2: the state
// can't move, but the event is journaled and alerted on.
func TestLateEventAfterSuccessAlerts(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{railDown}
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	first := e.attempts(id)[0]
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, first.RequestID, "bmw-a", succeeded)); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := e.store.GetTransfer(e.ctx, id); tr.State != transfer.Succeeded {
		t.Fatalf("state %s", tr.State)
	}
	if !strings.HasSuffix(strings.Join(e.journal(id), " "), "BankStatusReceived") || !strings.Contains(e.logs.String(), `"event":"transfer.late_event"`) {
		t.Fatalf("journal %v logs %s", e.journal(id), e.logs)
	}
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-c")
}

// A failed attempt that later reports money moving, before the next
// dispatch: never failed over.
func TestFailedAttemptThatSettlesIsNotFailedOver(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{railDown}
	id := e.receiveAt("022", "750965.00")
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.dispatch.RunDispatch(e.ctx); err != nil { // attempt 1 fails; the next dispatch hasn't run
		t.Fatal(err)
	}
	tr, as, _ := e.store.GetTransfer(e.ctx, id)
	if tr.State != transfer.AttemptFailed || len(as) != 1 {
		t.Fatalf("state %s, %d attempts", tr.State, len(as))
	}
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, as[0].RequestID, "bmw-a", processing)); err != nil {
		t.Fatal(err)
	}
	e.drive(id, transfer.NeedsManualReview)
	if e.banks["bmw-c"].calls() != 0 {
		t.Fatal("failed over after the failed attempt reported money moving")
	}
}

// P§3.3: refused at connect, provably never sent → retried on the same node
// (same_node_retries: 2) with backoff, then failed over.
func TestSameNodeRetryThenFailover(t *testing.T) {
	e := newDispatchEnv(t, dopts{down: []string{"bmw-a"}})
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	var who []string
	for _, a := range e.attempts(id) {
		who = append(who, a.BankMiddleware+":"+a.Status)
	}
	if got := strings.Join(who, " "); got != "bmw-a:FAILED bmw-a:FAILED bmw-a:FAILED bmw-c:SUCCEEDED" {
		t.Fatalf("attempts = %s", got)
	}
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-c")
}

// A 401 from the middleware is a key bug, not a transient: no retry on the
// node, straight to the next candidate.
func TestInvalidSignatureNotRetriedOnNode(t *testing.T) {
	e := newDispatchEnv(t, dopts{foreignKey: []string{"bmw-a"}})
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	as := e.attempts(id)
	if len(as) != 2 || as[0].BankMiddleware != "bmw-a" || as[0].Reason != "AUTH_ERROR" || as[1].BankMiddleware != "bmw-c" {
		t.Fatalf("attempts = %+v", as)
	}
}

// Every candidate fails definitively → FAILED with the last reason, one result.
func TestCandidatesExhausted(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	for _, b := range e.banks {
		b.answers = []contract.BankResult{railDown}
	}
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Failed)
	r := e.oneResult(id, contract.StatusFailed, contract.ReasonRailUnavailable, "bmw-d")
	if r.Attempts != 3 {
		t.Fatalf("attempts = %d", r.Attempts)
	}
}

// M3: an async bank's event can beat its own synchronous ACCEPTED. MOM keeps
// the newer state and ignores the late synchronous one.
func TestEventBeforeSyncAnswer(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = processing
	e.banks["bmw-a"].onTransfer = func(req contract.TransferRequest) {
		if err := e.dispatch.ReceiveBankEvent(context.Background(), bankEvent(t, req.TransferID, req.RequestID, "bmw-a", processing)); err != nil {
			t.Error(err)
		}
	}
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Processing)
	time.Sleep(50 * time.Millisecond)
	if tr, _, _ := e.store.GetTransfer(e.ctx, id); tr.State != transfer.Processing {
		t.Fatalf("state %s: the late synchronous ACCEPTED moved it back", tr.State)
	}
	a := e.attempts(id)[0]
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, a.RequestID, "bmw-a", succeeded)); err != nil {
		t.Fatal(err)
	}
	e.drive(id, transfer.Succeeded)
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
}

func TestDuplicateBankEventIgnored(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Accepted, transfer.Processing)
	ev := bankEvent(t, id, e.attempts(id)[0].RequestID, "bmw-a", succeeded)
	for i := 0; i < 3; i++ {
		if err := e.dispatch.ReceiveBankEvent(e.ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	for _, j := range e.journal(id) {
		if j == contract.EventBankStatusReceived {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d BankStatusReceived for one event", n)
	}
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
}

func TestBankEventForUnknownAttempt(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	ev := bankEvent(t, uuid.NewString(), uuid.NewString(), "bmw-a", succeeded)
	if err := e.dispatch.ReceiveBankEvent(e.ctx, ev); err != nil {
		t.Fatalf("an event for no attempt is skipped, not retried: %v", err)
	}
	if !strings.Contains(e.logs.String(), `"reason":"unknown_attempt"`) {
		t.Fatalf("no denied transfer.bank_event: %s", e.logs)
	}
}

// Q4: a parked transfer whose current attempt later settles finishes.
func TestParkedTransferFinishesOnLateAnswer(t *testing.T) {
	e := newDispatchEnv(t, dopts{inquiryFor: 200 * time.Millisecond})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.NeedsManualReview)
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, e.attempts(id)[0].RequestID, "bmw-a", succeeded)); err != nil {
		t.Fatal(err)
	}
	e.drive(id, transfer.Succeeded)
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
}

// MOM stopping between the DISPATCHING commit and the answer: the attempt
// may have been sent. The inquiry job picks it up once stale; it never
// re-sends blindly.
func TestInterruptedDispatchRecoveredByInquiry(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].delay = 2 * time.Second
	e.banks["bmw-a"].inquiry = succeeded
	id := e.receiveAt("022", "750965.00")
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(e.ctx, 100*time.Millisecond) // the process stops mid-call
	defer cancel()
	if err := e.dispatch.RunDispatch(ctx); err != nil && ctx.Err() == nil {
		t.Fatal(err)
	}
	if tr, _, _ := e.store.GetTransfer(e.ctx, id); tr.State != transfer.Dispatching {
		t.Fatalf("state %s after the interrupted call, want DISPATCHING", tr.State)
	}
	if strings.Contains(e.logs.String(), `"reason":"pg_error"`) {
		t.Fatalf("stopping tried to apply the interrupted answer: %s", e.logs)
	}
	e.drive(id, transfer.Succeeded)
	if e.banks["bmw-a"].calls() != 1 || e.banks["bmw-c"].calls() != 0 {
		t.Fatalf("calls a=%d c=%d: an interrupted dispatch must be inquired, not re-sent", e.banks["bmw-a"].calls(), e.banks["bmw-c"].calls())
	}
}

// A synchronous UNKNOWN (the middleware timed out) that arrives after the
// bank's own PROCESSING event never moves the transfer back.
func TestLateSyncUnknownAfterEvent(t *testing.T) {
	e := newDispatchEnv(t, dopts{timeout: 300 * time.Millisecond})
	e.banks["bmw-a"].delay = time.Second
	e.banks["bmw-a"].inquiry = processing
	e.banks["bmw-a"].onTransfer = func(req contract.TransferRequest) {
		if err := e.dispatch.ReceiveBankEvent(context.Background(), bankEvent(t, req.TransferID, req.RequestID, "bmw-a", processing)); err != nil {
			t.Error(err)
		}
	}
	id := e.receiveAt("022", "750965.00")
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.dispatch.RunDispatch(e.ctx); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := e.store.GetTransfer(e.ctx, id); tr.State != transfer.Processing || e.attempts(id)[0].Status != "PROCESSING" {
		t.Fatalf("state %s, attempt %s: the late UNKNOWN moved it back", tr.State, e.attempts(id)[0].Status)
	}
}

// An ACCEPTED (from an inquiry or an event) never moves PROCESSING back.
func TestAcceptedNeverRegressesProcessing(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{processing}
	e.banks["bmw-a"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Processing)
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, e.attempts(id)[0].RequestID, "bmw-a", accepted)); err != nil {
		t.Fatal(err)
	}
	if tr, _, _ := e.store.GetTransfer(e.ctx, id); tr.State != transfer.Processing || e.attempts(id)[0].Status != "PROCESSING" {
		t.Fatalf("state %s attempt %s", tr.State, e.attempts(id)[0].Status)
	}
}

// A parked transfer whose attempt then fails definitively: FAILED, one
// result, no failover (Q4).
func TestParkedTransferFailedNoFailover(t *testing.T) {
	e := newDispatchEnv(t, dopts{inquiryFor: 200 * time.Millisecond})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.NeedsManualReview)
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, e.attempts(id)[0].RequestID, "bmw-a", railDown)); err != nil {
		t.Fatal(err)
	}
	e.drive(id, transfer.Failed)
	e.oneResult(id, contract.StatusFailed, contract.ReasonRailUnavailable, "bmw-a")
	if e.banks["bmw-c"].calls() != 0 {
		t.Fatal("a parked transfer failed over")
	}
}

// An inquiry the middleware can't answer says nothing about the bank. It
// must never read as "not sent": that would re-send a transfer that may
// already have gone out.
func TestUnreachableInquiryIsNotAFailure(t *testing.T) {
	e := newDispatchEnv(t, dopts{timeout: 150 * time.Millisecond})
	e.banks["bmw-a"].delay = time.Second
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Unknown)
	e.servers["bmw-a"].Close() // the middleware is gone
	for i := 0; i < 10; i++ {
		if err := e.dispatch.RunInquiries(e.ctx); err != nil {
			t.Fatal(err)
		}
		if err := e.dispatch.RunDispatch(e.ctx); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}
	tr, as, _ := e.store.GetTransfer(e.ctx, id)
	if tr.State != transfer.Unknown || len(as) != 1 || as[0].Status != "UNKNOWN" {
		t.Fatalf("state %s, %d attempts: an unanswered inquiry was taken as a failure", tr.State, len(as))
	}
	if !strings.Contains(e.logs.String(), `"event":"transfer.inquiry"`) || !strings.Contains(e.logs.String(), `"reason":"connection_refused"`) {
		t.Fatalf("the failed inquiry isn't logged: %s", e.logs)
	}
}

// The same-node retry waits for its backoff (P§3.3).
func TestSameNodeRetryWaitsForBackoff(t *testing.T) {
	e := newDispatchEnv(t, dopts{down: []string{"bmw-a"}, retryBase: 400 * time.Millisecond})
	id := e.receiveAt("022", "750965.00")
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ { // within the backoff: no second attempt
		if err := e.dispatch.RunDispatch(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(e.attempts(id)); n != 1 {
		t.Fatalf("%d attempts before the backoff ran out, want 1", n)
	}
	time.Sleep(600 * time.Millisecond) // 400 ms ± 20 %
	if err := e.dispatch.RunDispatch(e.ctx); err != nil {
		t.Fatal(err)
	}
	if as := e.attempts(id); len(as) != 2 || as[1].BankMiddleware != "bmw-a" {
		t.Fatalf("attempts after the backoff = %+v", as)
	}
}

// An event that names an attempt of another transfer is refused.
func TestBankEventForAnotherTransfersAttempt(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = processing
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Accepted, transfer.Processing)
	before, _, _ := e.store.GetTransfer(e.ctx, id)
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, uuid.NewString(), e.attempts(id)[0].RequestID, "bmw-a", succeeded)); err != nil {
		t.Fatal(err)
	}
	if after, _, _ := e.store.GetTransfer(e.ctx, id); after.State != before.State || after.Version != before.Version {
		t.Fatalf("another transfer's event moved %s: %s v%d → %s v%d", id, before.State, before.Version, after.State, after.Version)
	}
	if !strings.Contains(e.logs.String(), `"reason":"unknown_attempt"`) {
		t.Fatalf("not refused: %s", e.logs)
	}
}

// An UNKNOWN is stored as possibly reached, whatever the event says.
func TestUnknownEventStoredAsReached(t *testing.T) {
	e := newDispatchEnv(t, dopts{})
	e.banks["bmw-a"].answers = []contract.BankResult{accepted}
	e.banks["bmw-a"].inquiry = accepted
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Accepted)
	if err := e.dispatch.ReceiveBankEvent(e.ctx, bankEvent(t, id, e.attempts(id)[0].RequestID, "bmw-a",
		contract.BankResult{Status: contract.StatusUnknown})); err != nil {
		t.Fatal(err)
	}
	if a := e.attempts(id)[0]; a.Status != "UNKNOWN" || !a.ReachedBank {
		t.Fatalf("attempt = %s reached=%v", a.Status, a.ReachedBank)
	}
}
