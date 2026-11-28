// Package breaker is the circuit breaker per (bank_middleware, rail) (P§3.3,
// spec/money-out TRD §6.3): its states, what counts and the transitions. It
// does no I/O; the row lives in MOM's database, changed by compare-and-set,
// so every MOM instance sees the same breaker.
package breaker

import (
	"errors"
	"time"
)

type State string

const (
	Closed   State = "CLOSED"
	Open     State = "OPEN"
	HalfOpen State = "HALF_OPEN"
)

// Kind is what one dispatch outcome means for the breaker.
type Kind int

const (
	Ignore  Kind = iota // a business refusal, or MOM's own bug: the bank isn't at fault
	Success             // the bank answered
	Failure             // a connection failure, a timeout, RAIL_UNAVAILABLE, BANK_REJECTED, AUTH_ERROR
)

type Config struct {
	Failures         int           // consecutive failures that open it
	Window           time.Duration // the rolling window of the error rate
	ErrorRatePercent int           // failures × 100 / calls at or above which it opens; 0 off
	MinCalls         int           // calls in the window before the rate counts
	Cooldown         time.Duration // open → half-open
	Probes           int           // dispatches allowed at once while half-open
	ProbeTimeout     time.Duration // a probe that never reports frees its slot after this
}

func (c Config) Validate() error {
	switch {
	case c.Failures < 1:
		return errors.New("breaker: failures must be at least 1")
	case c.Window <= 0 || c.Cooldown <= 0 || c.ProbeTimeout <= 0:
		return errors.New("breaker: window, cooldown and probe timeout must be positive")
	case c.ErrorRatePercent < 0 || c.ErrorRatePercent > 100:
		return errors.New("breaker: error rate must be 0-100 percent")
	case c.MinCalls < 0:
		return errors.New("breaker: min calls must not be negative")
	case c.Probes < 1:
		return errors.New("breaker: probes must be at least 1")
	}
	return nil
}

// Row is one breaker.
type Row struct {
	BankMiddleware string
	Rail           string
	State          State
	Consecutive    int
	Calls          int // in the window
	Failures       int // in the window
	WindowStart    time.Time
	OpenedAt       time.Time
	ProbesInFlight int
	ProbeAt        time.Time
	Version        int64
	UpdatedAt      time.Time
}

// Change is a state transition, journaled as BreakerChanged.
type Change struct {
	From, To State
	// Reason: consecutive_failures, error_rate, cooldown, probe_succeeded,
	// probe_failed.
	Reason string
}

// Decision is whether a new dispatch may use the breaker's candidate.
type Decision struct {
	Allowed bool
	Probe   bool // a half-open probe: its result closes or reopens the breaker
	Change  *Change
}

// Allow decides a new dispatch. Inquiries never ask (P§3.3).
func (c Config) Allow(r Row, now time.Time) (Row, Decision) {
	var d Decision
	if r.State == Open {
		if now.Before(r.OpenedAt.Add(c.Cooldown)) {
			return r, d
		}
		r.State, r.ProbesInFlight = HalfOpen, 0
		d.Change = &Change{From: Open, To: HalfOpen, Reason: "cooldown"}
	}
	if r.State == HalfOpen {
		if r.ProbesInFlight >= c.Probes && now.Sub(r.ProbeAt) > c.ProbeTimeout {
			r.ProbesInFlight = 0 // probes that never reported
		}
		if r.ProbesInFlight >= c.Probes {
			return r, d
		}
		r.ProbesInFlight++
		r.ProbeAt = now
		d.Allowed, d.Probe = true, true
		return r, d
	}
	d.Allowed = true
	return r, d
}

// Record counts one dispatch outcome. probe says the call was a half-open
// probe (Allow's Decision.Probe).
func (c Config) Record(r Row, k Kind, probe bool, now time.Time) (Row, *Change) {
	if r.State == HalfOpen && probe {
		r.ProbesInFlight = max(r.ProbesInFlight-1, 0)
		switch k {
		case Success:
			return c.close(r, now), &Change{From: HalfOpen, To: Closed, Reason: "probe_succeeded"}
		case Failure:
			return c.open(r, now), &Change{From: HalfOpen, To: Open, Reason: "probe_failed"}
		}
		return r, nil
	}
	if r.State != Closed || k == Ignore { // a late answer to a call made before it opened
		return r, nil
	}
	if now.Sub(r.WindowStart) >= c.Window {
		r.Calls, r.Failures, r.WindowStart = 0, 0, now
	}
	r.Calls++
	if k == Failure {
		r.Failures++
		r.Consecutive++
	} else {
		r.Consecutive = 0
	}
	switch {
	case r.Consecutive >= c.Failures:
		return c.open(r, now), &Change{From: Closed, To: Open, Reason: "consecutive_failures"}
	case c.ErrorRatePercent > 0 && r.Calls >= c.MinCalls && r.Failures*100 >= c.ErrorRatePercent*r.Calls:
		return c.open(r, now), &Change{From: Closed, To: Open, Reason: "error_rate"}
	}
	return r, nil
}

func (c Config) open(r Row, now time.Time) Row {
	r.State, r.OpenedAt, r.ProbesInFlight = Open, now, 0
	r.Consecutive, r.Calls, r.Failures, r.WindowStart = 0, 0, 0, now
	return r
}

func (c Config) close(r Row, now time.Time) Row {
	r.State, r.ProbesInFlight = Closed, 0
	r.Consecutive, r.Calls, r.Failures, r.WindowStart = 0, 0, 0, now
	return r
}
