// Package banksim is the shared core of the test-only bank simulators
// (spec/money-out Development Plan M2.1): a fault registry driven over HTTP,
// per-step call counts, and helpers the four stub packages share. It holds
// no bank concept; each stub defines its own steps and how it renders a
// fault.
package banksim

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gin-gonic/gin"

	coreapi "fintech-ledger-playground/internal/api"
)

// Fault actions (P§5: delay, timeout, 5xx, pending-then-success,
// pending-then-fail, dropped webhook).
const (
	ActionDelay              = "delay"                // wait DelayMS, then answer normally
	ActionTimeout            = "timeout"              // process the request, then hold until the caller gives up
	ActionStatus             = "status"               // answer HTTPStatus with the bank's error for Code, without processing
	ActionPendingThenSuccess = "pending_then_success" // accept as pending; Polls status replies stay pending, then success
	ActionPendingThenFail    = "pending_then_fail"    // the same, then the bank's failure for Code
	ActionDropWebhook        = "drop_webhook"         // never send this webhook
)

// Match selects the requests a fault applies to.
type Match struct {
	Step      string `json:"step"`
	AccountNo string `json:"account_no,omitempty"` // "" matches any account
}

// Fault is one injected behaviour.
type Fault struct {
	ID         int64  `json:"id"`
	Match      Match  `json:"match"`
	Action     string `json:"action"`
	Times      int    `json:"times"`                 // requests it applies to (default 1)
	DelayMS    int    `json:"delay_ms,omitempty"`    // delay; timeout's longest hold (default 30s)
	HTTPStatus int    `json:"http_status,omitempty"` // status (default 200)
	Code       string `json:"code,omitempty"`        // the bank code to answer with (status, pending_then_fail)
	Polls      int    `json:"polls,omitempty"`       // pending_then_*: pending replies before the final one; -1 forever (default 1)
}

// Steps names a bank's steps: All may be matched; Pending may take
// pending_then_*; Webhooks are outbound callbacks (only delay, status and
// drop_webhook apply to them).
type Steps struct {
	All      []string
	Pending  []string
	Webhooks []string
}

// Sim holds the faults and call counts of one simulator.
type Sim struct {
	steps Steps

	mu      sync.Mutex
	nextID  int64
	faults  []*Fault
	calls   map[string]int
	onReset []func()
}

func New(steps Steps) *Sim { return &Sim{steps: steps, calls: map[string]int{}} }

// Take returns the first fault matching step and account and uses up one of
// its times, or nil.
func (s *Sim) Take(step, accountNo string) *Fault {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, f := range s.faults {
		if f.Match.Step != step || (f.Match.AccountNo != "" && f.Match.AccountNo != accountNo) {
			continue
		}
		taken := *f
		if f.Times--; f.Times == 0 {
			s.faults = slices.Delete(s.faults, i, i+1)
		}
		return &taken
	}
	return nil
}

// Count records one call that reached step's handler.
func (s *Sim) Count(step string) {
	s.mu.Lock()
	s.calls[step]++
	s.mu.Unlock()
}

// OnReset registers the bank's own state to clear on POST /_sim/reset.
func (s *Sim) OnReset(f func()) {
	s.mu.Lock()
	s.onReset = append(s.onReset, f)
	s.mu.Unlock()
}

func (s *Sim) add(f Fault) (Fault, error) {
	if err := s.validate(&f); err != nil {
		return Fault{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	f.ID = s.nextID
	s.faults = append(s.faults, &f)
	return f, nil
}

func (s *Sim) validate(f *Fault) error {
	step := f.Match.Step
	webhook := slices.Contains(s.steps.Webhooks, step)
	switch {
	case !slices.Contains(s.steps.All, step):
		return fmt.Errorf("unknown step %q; this bank has %v", step, s.steps.All)
	case f.Times < 0:
		return errors.New("times must be positive")
	case f.Polls < -1:
		return errors.New("polls must be -1 (forever) or more")
	}
	if f.Times == 0 {
		f.Times = 1
	}
	switch f.Action {
	case ActionDelay:
		if f.DelayMS <= 0 {
			return errors.New("delay needs delay_ms")
		}
	case ActionTimeout:
		if webhook {
			return errors.New("timeout doesn't apply to a webhook; use delay or drop_webhook")
		}
	case ActionStatus:
		if f.HTTPStatus == 0 {
			f.HTTPStatus = http.StatusOK
		}
		if f.HTTPStatus < 100 || f.HTTPStatus > 599 {
			return errors.New("http_status must be 100-599")
		}
	case ActionPendingThenSuccess, ActionPendingThenFail:
		if !slices.Contains(s.steps.Pending, step) {
			return fmt.Errorf("%s applies only to %v", f.Action, s.steps.Pending)
		}
		if f.Polls == 0 {
			f.Polls = 1
		}
	case ActionDropWebhook:
		if !webhook {
			return fmt.Errorf("drop_webhook applies only to %v", s.steps.Webhooks)
		}
	default:
		return fmt.Errorf("unknown action %q", f.Action)
	}
	return nil
}

// Hold applies f's delay or timeout to the request, after the stub has
// processed it. It reports whether the stub should go on to answer.
func Hold(ctx context.Context, f *Fault) bool {
	if f == nil {
		return true
	}
	switch f.Action {
	case ActionDelay:
		select {
		case <-time.After(time.Duration(f.DelayMS) * time.Millisecond):
			return true
		case <-ctx.Done():
			return false
		}
	case ActionTimeout:
		hold := 30 * time.Second
		if f.DelayMS > 0 {
			hold = time.Duration(f.DelayMS) * time.Millisecond
		}
		select {
		case <-time.After(hold):
		case <-ctx.Done():
		}
		return false
	}
	return true
}

// NewEngine is the simulator's HTTP engine: the repo's base engine and the
// /_sim control routes. Stubs add their bank's routes to it.
func NewEngine(log *slog.Logger, s *Sim) (*gin.Engine, error) {
	r, err := coreapi.NewBaseEngine(log, nil, func(context.Context) error { return nil })
	if err != nil {
		return nil, err
	}
	ctl := r.Group("/_sim")
	ctl.POST("/faults", func(c *gin.Context) {
		var f Fault
		dec := json.NewDecoder(c.Request.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&f); err != nil {
			coreapi.AbortError(c, http.StatusBadRequest, "invalid_input", "fault: "+err.Error())
			return
		}
		added, err := s.add(f)
		if err != nil {
			coreapi.AbortError(c, http.StatusBadRequest, "invalid_input", err.Error())
			return
		}
		c.JSON(http.StatusCreated, added)
	})
	ctl.GET("/faults", func(c *gin.Context) {
		s.mu.Lock()
		out := make([]Fault, 0, len(s.faults))
		for _, f := range s.faults {
			out = append(out, *f)
		}
		s.mu.Unlock()
		c.JSON(http.StatusOK, out)
	})
	ctl.DELETE("/faults", func(c *gin.Context) {
		s.mu.Lock()
		s.faults = nil
		s.mu.Unlock()
		c.Status(http.StatusNoContent)
	})
	ctl.GET("/calls", func(c *gin.Context) {
		s.mu.Lock()
		out := make(map[string]int, len(s.calls))
		for k, v := range s.calls {
			out[k] = v
		}
		s.mu.Unlock()
		c.JSON(http.StatusOK, out)
	})
	ctl.POST("/reset", func(c *gin.Context) {
		s.mu.Lock()
		s.faults, s.calls = nil, map[string]int{}
		hooks := slices.Clone(s.onReset)
		s.mu.Unlock()
		for _, h := range hooks {
			h()
		}
		c.Status(http.StatusNoContent)
	})
	return r, nil
}

// Minify compacts a JSON body, as the banks' signatures are computed over
// the minified body. An empty body stays empty.
func Minify(body []byte) ([]byte, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	var b bytes.Buffer
	if err := json.Compact(&b, body); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
