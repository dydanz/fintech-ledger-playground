//go:build integration

package service_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/moneyout/breaker"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/routing"
	"fintech-ledger-playground/internal/moneyout/service"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

func breakerCfg() *breaker.Config {
	return &breaker.Config{Failures: 3, Window: time.Minute, ErrorRatePercent: 0, Cooldown: 300 * time.Millisecond, Probes: 1,
		ProbeTimeout: 5 * time.Second}
}

func (e *denv) breakerState(bmw string) breaker.State {
	e.t.Helper()
	rows, err := e.breakers.List(e.ctx)
	if err != nil {
		e.t.Fatal(err)
	}
	for _, r := range rows {
		if r.BankMiddleware == bmw && r.Rail == "BIFAST" {
			return r.State
		}
	}
	return breaker.Closed
}

// breakerEvents are the BreakerChanged journal events for bmw/BIFAST.
func (e *denv) breakerEvents(bmw string) []string {
	var out []string
	for _, r := range e.outboxRows(bmw + "/BIFAST") {
		var p struct {
			From   string `json:"from_state"`
			To     string `json:"to_state"`
			Reason string `json:"reason"`
		}
		_ = r.env.DecodePayload(&p)
		out = append(out, p.From+">"+p.To+":"+p.Reason)
	}
	return out
}

func (e *denv) openA() {
	e.t.Helper()
	for i := 0; i < 3; i++ {
		if err := e.breakers.Record(e.ctx, "bmw-a", "BIFAST", breaker.Failure, false); err != nil {
			e.t.Fatal(err)
		}
	}
	if e.breakerState("bmw-a") != breaker.Open {
		e.t.Fatal("bmw-a not open")
	}
}

// P§5 #15 (MOM's part): A fails repeatedly → its breaker opens → a new
// transfer goes straight to the next candidate, A is never called for it.
func TestBreakerOpensAndNewTransfersRouteAround(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.banks["bmw-a"].answers = []contract.BankResult{railDown}
	for i := 0; i < 3; i++ { // each fails once on A, then succeeds on C
		id := e.receiveAt("022", "750965.00")
		e.drive(id, transfer.Succeeded)
		e.oneResult(id, contract.StatusSucceeded, "", "bmw-c")
	}
	if got := e.breakerState("bmw-a"); got != breaker.Open {
		t.Fatalf("bmw-a breaker %s after 3 failures, want OPEN", got)
	}
	if ev := e.breakerEvents("bmw-a"); len(ev) != 1 || ev[0] != "CLOSED>OPEN:consecutive_failures" {
		t.Fatalf("BreakerChanged = %v", ev)
	}
	calls := e.banks["bmw-a"].calls()
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	as := e.attempts(id)
	if len(as) != 1 || as[0].BankMiddleware != "bmw-c" || e.banks["bmw-a"].calls() != calls {
		t.Fatalf("attempts %+v, a calls %d → %d: an open breaker was dispatched to", as, calls, e.banks["bmw-a"].calls())
	}
	if !strings.Contains(e.logs.String(), `"event":"transfer.breaker"`) || !strings.Contains(e.logs.String(), `"to_state":"OPEN"`) {
		t.Fatalf("no transfer.breaker: %s", e.logs)
	}
}

// Business refusals don't count (P§3.3): INVALID_ACCOUNT five times over
// leaves the breaker closed.
func TestBusinessFailuresDontCount(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.banks["bmw-a"].answers = []contract.BankResult{invalidAcct}
	for i := 0; i < 5; i++ {
		id := e.receiveAt("022", "750965.00")
		e.drive(id, transfer.Failed)
	}
	if got := e.breakerState("bmw-a"); got != breaker.Closed {
		t.Fatalf("bmw-a breaker %s after business refusals", got)
	}
}

// An open breaker never stops inquiries: an UNKNOWN on A keeps resolving
// against A (P§3.3, P§5 #15).
func TestOpenBreakerDoesNotStopInquiries(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg(), timeout: 150 * time.Millisecond})
	e.banks["bmw-a"].delay = time.Second
	e.banks["bmw-a"].inquiry = unknownRes
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Unknown)
	e.openA()
	before := e.banks["bmw-a"].inquiries
	e.banks["bmw-a"].mu.Lock()
	e.banks["bmw-a"].inquiry = succeeded
	e.banks["bmw-a"].mu.Unlock()
	e.drive(id, transfer.Succeeded)
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
	if e.banks["bmw-a"].inquiries <= before || e.banks["bmw-c"].calls() != 0 {
		t.Fatalf("inquiries %d → %d, c calls %d", before, e.banks["bmw-a"].inquiries, e.banks["bmw-c"].calls())
	}
}

// After the cool-down one probe goes to A; its success closes the breaker.
func TestHalfOpenProbeClosesBreaker(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.openA()
	time.Sleep(350 * time.Millisecond)
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-a")
	if got := e.breakerState("bmw-a"); got != breaker.Closed {
		t.Fatalf("after a good probe: %s", got)
	}
	if got := strings.Join(e.breakerEvents("bmw-a"), " "); got != "CLOSED>OPEN:consecutive_failures OPEN>HALF_OPEN:cooldown HALF_OPEN>CLOSED:probe_succeeded" {
		t.Fatalf("BreakerChanged = %s", got)
	}
}

// A failed probe reopens it, and the transfer fails over.
func TestHalfOpenProbeFailureReopens(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.banks["bmw-a"].answers = []contract.BankResult{railDown}
	e.openA()
	time.Sleep(350 * time.Millisecond)
	id := e.receiveAt("022", "750965.00")
	e.drive(id, transfer.Succeeded)
	e.oneResult(id, contract.StatusSucceeded, "", "bmw-c")
	if got := e.breakerState("bmw-a"); got != breaker.Open {
		t.Fatalf("after a bad probe: %s", got)
	}
	if ev := e.breakerEvents("bmw-a"); ev[len(ev)-1] != "HALF_OPEN>OPEN:probe_failed" {
		t.Fatalf("BreakerChanged = %v", ev)
	}
}

// Half-open lets one probe through at a time; the others go elsewhere.
func TestOneProbeAtATime(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.banks["bmw-a"].delay = 300 * time.Millisecond
	e.openA()
	time.Sleep(350 * time.Millisecond)
	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, e.receiveAt("022", "750965.00"))
	}
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := e.dispatch.RunDispatch(e.ctx); err != nil { // the three in parallel
		t.Fatal(err)
	}
	if n := e.banks["bmw-a"].calls(); n != 1 {
		t.Fatalf("%d calls to the half-open bmw-a, want 1 probe", n)
	}
	for _, id := range ids {
		e.drive(id, transfer.Succeeded)
	}
}

// The breaker is shared: another MOM instance's Breakers sees it open.
func TestBreakerSharedAcrossInstances(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.openA()
	other := service.NewBreakers(e.store, *breakerCfg())
	if err := other.Refresh(e.ctx); err != nil {
		t.Fatal(err)
	}
	if !other.Open("bmw-a", "BIFAST") || other.Open("bmw-c", "BIFAST") {
		t.Fatal("another instance doesn't see bmw-a open")
	}
}

// Routing records the open breaker as the reason a candidate was dropped.
func TestRoutingSkipsOpenBreaker(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.openA()
	if err := e.breakers.Refresh(e.ctx); err != nil {
		t.Fatal(err)
	}
	id := e.receiveAt("022", "750965.00")
	if err := e.router.Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	tr, _, _ := e.store.GetTransfer(e.ctx, id)
	var res routing.Result
	if err := json.Unmarshal(tr.Routing, &res); err != nil {
		t.Fatal(err)
	}
	dropped := false
	for _, d := range res.Decisions {
		dropped = dropped || (d.BankMiddleware == "bmw-a" && d.Rule == "breaker_open" && !d.Kept)
	}
	if !dropped || len(res.Candidates) == 0 || res.Candidates[0].BankMiddleware != "bmw-c" {
		t.Fatalf("routing = %s", tr.Routing)
	}
}

// What counts (P§3.3): each of these, three times, opens the breaker.
func TestWhatCountsAgainstTheBreaker(t *testing.T) {
	for name, set := range map[string]func(*denv){
		"BANK_REJECTED": func(e *denv) {
			e.banks["bmw-a"].answers = []contract.BankResult{{Status: contract.StatusFailed, Reason: contract.ReasonBankRejected, RetryElsewhere: true, ReachedBank: true}}
		},
		"AUTH_ERROR": func(e *denv) {
			e.banks["bmw-a"].answers = []contract.BankResult{{Status: contract.StatusFailed, Reason: contract.ReasonAuthError, RetryElsewhere: true, ReachedBank: true}}
		},
		"UNKNOWN from the bank": func(e *denv) { e.banks["bmw-a"].answers = []contract.BankResult{unknownRes} },
		"middleware timeout":    func(e *denv) { e.banks["bmw-a"].delay = time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			e := newDispatchEnv(t, dopts{breaker: breakerCfg(), timeout: 150 * time.Millisecond})
			set(e)
			e.banks["bmw-a"].inquiry = unknownRes
			for i := 0; i < 3; i++ {
				e.receiveAt("022", "750965.00")
				if err := e.router.Run(e.ctx); err != nil {
					t.Fatal(err)
				}
				if err := e.dispatch.RunDispatch(e.ctx); err != nil {
					t.Fatal(err)
				}
			}
			if got := e.breakerState("bmw-a"); got != breaker.Open {
				t.Fatalf("after 3 × %s: %s, want OPEN", name, got)
			}
		})
	}
}

// Past the cool-down routing keeps an open breaker's candidate, so the
// dispatcher can probe it: otherwise it would never be asked again and
// stay open for good (found by the live P§5 #15 check).
func TestRoutingKeepsBreakerPastCooldown(t *testing.T) {
	e := newDispatchEnv(t, dopts{breaker: breakerCfg()})
	e.openA()
	if err := e.breakers.Refresh(e.ctx); err != nil {
		t.Fatal(err)
	}
	if !e.breakers.Open("bmw-a", "BIFAST") {
		t.Fatal("inside the cool-down: not open to routing")
	}
	time.Sleep(350 * time.Millisecond)
	if e.breakers.Open("bmw-a", "BIFAST") {
		t.Fatal("past the cool-down: routing still drops it, so no probe could ever run")
	}
}
