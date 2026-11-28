package breaker_test

import (
	"testing"
	"time"

	"fintech-ledger-playground/internal/moneyout/breaker"
)

var cfg = breaker.Config{Failures: 3, Window: time.Minute, ErrorRatePercent: 50, MinCalls: 6, Cooldown: 10 * time.Second,
	Probes: 1, ProbeTimeout: 30 * time.Second}

var t0 = time.Date(2026, 11, 28, 10, 0, 0, 0, time.UTC)

func fresh() breaker.Row {
	return breaker.Row{BankMiddleware: "bmw-a", Rail: "BIFAST", State: breaker.Closed, WindowStart: t0}
}

func record(r breaker.Row, at time.Time, kinds ...breaker.Kind) (breaker.Row, []breaker.Change) {
	var cs []breaker.Change
	for _, k := range kinds {
		var c *breaker.Change
		r, c = cfg.Record(r, k, false, at)
		if c != nil {
			cs = append(cs, *c)
		}
	}
	return r, cs
}

const (
	F = breaker.Failure
	S = breaker.Success
	I = breaker.Ignore
)

func TestConsecutiveFailuresOpen(t *testing.T) {
	r, cs := record(fresh(), t0, F, F)
	if r.State != breaker.Closed || len(cs) != 0 {
		t.Fatalf("2 failures: %s %v", r.State, cs)
	}
	r, cs = record(r, t0, F)
	if r.State != breaker.Open || len(cs) != 1 || cs[0].Reason != "consecutive_failures" || cs[0].From != breaker.Closed || !r.OpenedAt.Equal(t0) {
		t.Fatalf("3rd failure: %+v %v", r, cs)
	}
}

// A success in between resets the consecutive count.
func TestSuccessResetsConsecutive(t *testing.T) {
	r, _ := record(fresh(), t0, F, F, S, F, F)
	if r.State != breaker.Closed {
		t.Fatalf("state %s", r.State)
	}
}

// Business refusals and MOM's own bugs never count (P§3.3).
func TestIgnoredNeverCounts(t *testing.T) {
	r, _ := record(fresh(), t0, F, F, I, I, I, I, I, I, I, I)
	if r.State != breaker.Closed || r.Consecutive != 2 {
		t.Fatalf("state %s consecutive %d", r.State, r.Consecutive)
	}
}

// The error rate over the window opens it without a run of failures.
func TestErrorRateOpens(t *testing.T) {
	r, cs := record(fresh(), t0, F, S, F, S, F) // 3 of 5: under MinCalls
	if r.State != breaker.Closed {
		t.Fatalf("under MinCalls: %s", r.State)
	}
	r, cs = record(r, t0, S) // 3 of 6 = 50 %
	if r.State != breaker.Open || len(cs) != 1 || cs[0].Reason != "error_rate" {
		t.Fatalf("50 %% of 6: %s %v", r.State, cs)
	}
}

// The window rolls: old failures don't add to new ones.
func TestWindowRolls(t *testing.T) {
	r, _ := record(fresh(), t0, F, S, F, S, F)
	r, _ = record(r, t0.Add(2*time.Minute), S) // a new window: 0 of 1
	if r.State != breaker.Closed || r.Calls != 1 || r.Failures != 0 {
		t.Fatalf("after the window: %+v", r)
	}
}

func open() breaker.Row {
	r, _ := record(fresh(), t0, F, F, F)
	return r
}

// Open skips new dispatches until the cool-down; then one probe at a time.
func TestOpenThenHalfOpenProbe(t *testing.T) {
	r := open()
	if _, d := cfg.Allow(r, t0.Add(9*time.Second)); d.Allowed || d.Change != nil {
		t.Fatalf("inside the cool-down: %+v", d)
	}
	r, d := cfg.Allow(r, t0.Add(10*time.Second))
	if !d.Allowed || !d.Probe || r.State != breaker.HalfOpen || d.Change == nil || d.Change.Reason != "cooldown" || r.ProbesInFlight != 1 {
		t.Fatalf("after the cool-down: %+v %+v", r, d)
	}
	if _, d2 := cfg.Allow(r, t0.Add(11*time.Second)); d2.Allowed {
		t.Fatal("a second probe while one is in flight")
	}
	closed, c := cfg.Record(r, S, true, t0.Add(12*time.Second))
	if closed.State != breaker.Closed || c == nil || c.Reason != "probe_succeeded" || closed.ProbesInFlight != 0 || closed.Consecutive != 0 {
		t.Fatalf("probe success: %+v %v", closed, c)
	}
	reopened, c := cfg.Record(r, F, true, t0.Add(12*time.Second))
	if reopened.State != breaker.Open || c == nil || c.Reason != "probe_failed" || !reopened.OpenedAt.Equal(t0.Add(12*time.Second)) {
		t.Fatalf("probe failure: %+v %v", reopened, c)
	}
}

// A probe that never reports (MOM stopped) frees its slot after ProbeTimeout.
func TestStaleProbeReleased(t *testing.T) {
	r, _ := cfg.Allow(open(), t0.Add(10*time.Second))
	if _, d := cfg.Allow(r, t0.Add(39*time.Second)); d.Allowed {
		t.Fatal("slot freed before ProbeTimeout")
	}
	if _, d := cfg.Allow(r, t0.Add(41*time.Second)); !d.Allowed || !d.Probe {
		t.Fatalf("stale probe not released: %+v", d)
	}
}

// A probe ignored (a business refusal) frees the slot and leaves it half-open.
func TestIgnoredProbeFreesSlot(t *testing.T) {
	r, _ := cfg.Allow(open(), t0.Add(10*time.Second))
	r, c := cfg.Record(r, I, true, t0.Add(11*time.Second))
	if r.State != breaker.HalfOpen || r.ProbesInFlight != 0 || c != nil {
		t.Fatalf("%+v %v", r, c)
	}
}

// A result for a call made before the breaker opened (not a probe) changes
// nothing while open or half-open.
func TestLateNonProbeResultIgnored(t *testing.T) {
	r := open()
	if got, c := cfg.Record(r, S, false, t0.Add(time.Second)); got.State != breaker.Open || c != nil {
		t.Fatalf("open: %+v %v", got, c)
	}
	h, _ := cfg.Allow(r, t0.Add(10*time.Second))
	if got, c := cfg.Record(h, S, false, t0.Add(11*time.Second)); got.State != breaker.HalfOpen || got.ProbesInFlight != 1 || c != nil {
		t.Fatalf("half-open: %+v %v", got, c)
	}
}

func TestClosedAllows(t *testing.T) {
	if _, d := cfg.Allow(fresh(), t0); !d.Allowed || d.Probe || d.Change != nil {
		t.Fatalf("%+v", d)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mod := range map[string]func(*breaker.Config){
		"no failures":   func(c *breaker.Config) { c.Failures = 0 },
		"no window":     func(c *breaker.Config) { c.Window = 0 },
		"rate over 100": func(c *breaker.Config) { c.ErrorRatePercent = 101 },
		"no cooldown":   func(c *breaker.Config) { c.Cooldown = 0 },
		"no probes":     func(c *breaker.Config) { c.Probes = 0 },
	} {
		c := cfg
		mod(&c)
		if c.Validate() == nil {
			t.Errorf("%s accepted", name)
		}
	}
}
