package banksim_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/logger"
)

var steps = banksim.Steps{
	All:      []string{"inquiry", "transfer", "status", "ack-1"},
	Pending:  []string{"transfer"},
	Webhooks: []string{"ack-1"},
}

func newSim(t *testing.T) (*banksim.Sim, http.Handler) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	s := banksim.New(steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "bank-simulator"), s)
	if err != nil {
		t.Fatal(err)
	}
	return s, r
}

func do(t *testing.T, h http.Handler, method, path, body string) (int, string) {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w.Code, w.Body.String()
}

func TestFaultIsTakenTimesThenGone(t *testing.T) {
	s, h := newSim(t)
	if code, body := do(t, h, "POST", "/_sim/faults", `{"match":{"step":"transfer","account_no":"0201245681"},"action":"status","http_status":503,"times":2}`); code != 201 {
		t.Fatalf("add: %d %s", code, body)
	}
	if f := s.Take("transfer", "9999999999"); f != nil {
		t.Fatalf("other account matched: %+v", f)
	}
	if f := s.Take("status", "0201245681"); f != nil {
		t.Fatalf("other step matched: %+v", f)
	}
	for i := 0; i < 2; i++ {
		if f := s.Take("transfer", "0201245681"); f == nil || f.HTTPStatus != 503 {
			t.Fatalf("take %d: %+v", i+1, f)
		}
	}
	if f := s.Take("transfer", "0201245681"); f != nil {
		t.Fatalf("third take after times=2: %+v", f)
	}
}

func TestFaultWithoutAccountMatchesAny(t *testing.T) {
	s, h := newSim(t)
	do(t, h, "POST", "/_sim/faults", `{"match":{"step":"inquiry"},"action":"delay","delay_ms":5}`)
	if f := s.Take("inquiry", "123456"); f == nil || f.Times != 1 {
		t.Fatalf("take = %+v (times defaults to 1)", f)
	}
}

func TestFaultValidation(t *testing.T) {
	_, h := newSim(t)
	for name, body := range map[string]string{
		"unknown action":         `{"match":{"step":"transfer"},"action":"explode"}`,
		"unknown step":           `{"match":{"step":"bulk"},"action":"timeout"}`,
		"no step":                `{"match":{},"action":"timeout"}`,
		"delay without delay_ms": `{"match":{"step":"transfer"},"action":"delay"}`,
		"status out of range":    `{"match":{"step":"transfer"},"action":"status","http_status":99}`,
		"pending on status step": `{"match":{"step":"status"},"action":"pending_then_success"}`,
		"drop on non-webhook":    `{"match":{"step":"transfer"},"action":"drop_webhook"}`,
		"timeout on webhook":     `{"match":{"step":"ack-1"},"action":"timeout"}`,
		"negative times":         `{"match":{"step":"transfer"},"action":"timeout","times":-1}`,
		"polls below -1":         `{"match":{"step":"transfer"},"action":"pending_then_fail","polls":-2}`,
		"unknown field":          `{"match":{"step":"transfer"},"action":"timeout","bogus":1}`,
		"not json":               `nope`,
	} {
		if code, out := do(t, h, "POST", "/_sim/faults", body); code != 400 {
			t.Errorf("%s: %d %s", name, code, out)
		}
	}
}

func TestPendingFaultDefaults(t *testing.T) {
	s, h := newSim(t)
	do(t, h, "POST", "/_sim/faults", `{"match":{"step":"transfer"},"action":"pending_then_fail","code":"06"}`)
	f := s.Take("transfer", "1")
	if f == nil || f.Polls != 1 || f.Code != "06" {
		t.Fatalf("fault = %+v, want polls 1", f)
	}
	do(t, h, "POST", "/_sim/faults", `{"match":{"step":"transfer"},"action":"pending_then_success","polls":-1}`)
	if f := s.Take("transfer", "1"); f == nil || f.Polls != -1 {
		t.Fatalf("forever: %+v", f)
	}
}

func TestCallsAndReset(t *testing.T) {
	s, h := newSim(t)
	s.Count("transfer")
	s.Count("transfer")
	s.Count("status")
	do(t, h, "POST", "/_sim/faults", `{"match":{"step":"transfer"},"action":"timeout"}`)
	_, body := do(t, h, "GET", "/_sim/calls", "")
	var calls map[string]int
	if err := json.Unmarshal([]byte(body), &calls); err != nil || calls["transfer"] != 2 || calls["status"] != 1 {
		t.Fatalf("calls = %s", body)
	}
	resetHook := false
	s.OnReset(func() { resetHook = true })
	if code, _ := do(t, h, "POST", "/_sim/reset", ""); code != 204 {
		t.Fatalf("reset: %d", code)
	}
	if _, body := do(t, h, "GET", "/_sim/calls", ""); body != "{}" {
		t.Fatalf("calls after reset = %s", body)
	}
	if s.Take("transfer", "") != nil || !resetHook {
		t.Fatal("reset left a fault or skipped the bank's own state")
	}
}

func TestListAndClearFaults(t *testing.T) {
	_, h := newSim(t)
	do(t, h, "POST", "/_sim/faults", `{"match":{"step":"transfer"},"action":"timeout"}`)
	if _, body := do(t, h, "GET", "/_sim/faults", ""); !strings.Contains(body, `"action":"timeout"`) {
		t.Fatalf("list = %s", body)
	}
	if code, _ := do(t, h, "DELETE", "/_sim/faults", ""); code != 204 {
		t.Fatalf("clear: %d", code)
	}
	if _, body := do(t, h, "GET", "/_sim/faults", ""); body != "[]" {
		t.Fatalf("after clear = %s", body)
	}
}

func TestHoldDelayThenContinues(t *testing.T) {
	start := time.Now()
	if !banksim.Hold(context.Background(), &banksim.Fault{Action: banksim.ActionDelay, DelayMS: 30}) {
		t.Fatal("delay should let the request continue")
	}
	if time.Since(start) < 30*time.Millisecond {
		t.Fatal("no delay")
	}
	if !banksim.Hold(context.Background(), nil) {
		t.Fatal("no fault should continue")
	}
}

// A timeout processes the request, then holds until the caller gives up.
func TestHoldTimeoutWaitsForTheCaller(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	start := time.Now()
	if banksim.Hold(ctx, &banksim.Fault{Action: banksim.ActionTimeout, DelayMS: 5000}) {
		t.Fatal("timeout should not continue")
	}
	if d := time.Since(start); d < 40*time.Millisecond || d > 2*time.Second {
		t.Fatalf("held %v, want until the caller's deadline", d)
	}
}

func TestMinify(t *testing.T) {
	got, err := banksim.Minify([]byte("{\n  \"a\": 1,\n  \"b\": [1, 2]\n}"))
	if err != nil || string(got) != `{"a":1,"b":[1,2]}` {
		t.Fatalf("minify = %s, %v", got, err)
	}
	if got, err := banksim.Minify(nil); err != nil || len(got) != 0 {
		t.Fatalf("empty body = %q, %v", got, err)
	}
	if _, err := banksim.Minify([]byte("{")); err == nil {
		t.Fatal("bad JSON minified")
	}
}
