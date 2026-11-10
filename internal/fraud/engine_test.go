package fraud_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fintech-ledger-playground/internal/fraud"
	"fintech-ledger-playground/internal/idempotency"
	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/outbox"
)

const trace = "4bf92f3577b34da6a3ce929d0e0e4736"

// fakeConsumer hands out one batch, then blocks until ctx ends.
type fakeConsumer struct {
	mu      sync.Mutex
	batches [][]kafka.Message
	commits [][]kafka.Message
}

func (f *fakeConsumer) Poll(ctx context.Context) ([]kafka.Message, error) {
	f.mu.Lock()
	if len(f.batches) > 0 {
		b := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return b, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakeConsumer) Commit(_ context.Context, msgs []kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.commits = append(f.commits, msgs)
	return nil
}

func (f *fakeConsumer) committed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.commits {
		n += len(c)
	}
	return n
}

type memSeen struct {
	mu   sync.Mutex
	ids  map[string]bool
	down atomic.Bool
}

func (m *memSeen) Seen(_ context.Context, id string) (bool, error) {
	if m.down.Load() {
		return false, fmt.Errorf("dial tcp: %w", syscall.ECONNREFUSED)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.ids[id], nil
}

func (m *memSeen) Mark(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ids == nil {
		m.ids = map[string]bool{}
	}
	m.ids[id] = true
	return nil
}

type fakeFreezer struct {
	mu    sync.Mutex
	calls []string // idempotency keys
	spans []string
	err   func(n int) error
}

func (f *fakeFreezer) Freeze(_ context.Context, d fraud.Decision, traceID, spanID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fraud.IdempotencyKey(d))
	f.spans = append(f.spans, traceID+"/"+spanID)
	if f.err != nil {
		return f.err(len(f.calls))
	}
	return nil
}

func (f *fakeFreezer) n() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func settledMsg(t *testing.T, eventID, txID string, typ ledger.TransactionType, at time.Time) kafka.Message {
	t.Helper()
	m, err := outbox.Encode(ledger.Event{ID: eventID, StreamID: txID, StreamVersion: 3, AccountID: "acct-1", TraceID: trace,
		OccurredAt: at, Payload: ledger.TransactionSettled{Type: typ, Amount: 10, Currency: "IDR"}}, "ledger-1")
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Value: m.Value}
}

func topups(t *testing.T, n int) []kafka.Message {
	var out []kafka.Message
	for i := range n {
		out = append(out, settledMsg(t, fmt.Sprintf("ev-%d", i), fmt.Sprintf("tx-%d", i), ledger.Credit, t0.Add(time.Duration(i)*time.Minute)))
	}
	return out
}

func newEngine(t *testing.T, c fraud.Consumer, seen fraud.SeenStore, f fraud.Freezer, retry time.Duration) (*fraud.Engine, *syncBuf) {
	t.Helper()
	ev, _ := newEvaluator(t)
	e, err := fraud.NewEngine(c, seen, ev, f, fraud.EngineConfig{FreezeRetry: retry, MinBackoff: 5 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return e, &syncBuf{}
}

func run(t *testing.T, e *fraud.Engine, logs *syncBuf, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "fraud-engine")), d)
	defer cancel()
	_ = e.Run(ctx)
}

func TestEngineFreezesOnFourthTopupAndCommits(t *testing.T) {
	batch := topups(t, 4)
	batch = append(batch, batch[1])                                       // a redelivered event
	batch = append(batch, kafka.Message{Value: []byte("{not json")})      // malformed: skipped
	batch = append(batch, settledMsg(t, "ev-v", "tx-v", ledger.Void, t0)) // ignored
	c := &fakeConsumer{batches: [][]kafka.Message{batch}}
	f := &fakeFreezer{}
	e, logs := newEngine(t, c, &memSeen{}, f, time.Second)
	run(t, e, logs, 300*time.Millisecond)

	if f.n() != 1 {
		t.Fatalf("freeze calls = %v", f.calls)
	}
	key := f.calls[0]
	if key != fmt.Sprintf("fraud:topup-velocity:acct-1:%d", t0.UnixMilli()) || !idempotency.ValidKey(key) {
		t.Fatalf("idempotency key %q", key)
	}
	if !strings.HasPrefix(f.spans[0], trace+"/") {
		t.Fatalf("trace not continued: %s", f.spans[0])
	}
	if c.committed() != len(batch) {
		t.Fatalf("committed %d of %d", c.committed(), len(batch))
	}
	out := logs.String()
	for _, want := range []string{`"event":"fraud.decision"`, `"outcome":"success"`, `"rule":"topup_velocity"`, `"trace_id":"` + trace,
		`"event":"fraud.event"`, `"reason":"malformed_event"`} {
		if !strings.Contains(out, want) {
			t.Errorf("logs missing %s:\n%s", want, out)
		}
	}
	// The span in the log is the span sent to ledger-api.
	if !strings.Contains(out, `"span_id":"`+strings.TrimPrefix(f.spans[0], trace+"/")) {
		t.Errorf("log span differs from traceparent span %s", f.spans[0])
	}
}

// ledger-api unavailable: the freeze is retried, a failure is reported each
// FreezeRetry window, and nothing is committed until it succeeds.
func TestEngineRetriesFreezeWithoutCommitting(t *testing.T) {
	c := &fakeConsumer{batches: [][]kafka.Message{topups(t, 4)}}
	f := &fakeFreezer{err: func(int) error { return errors.New("freeze call: dial tcp: connection refused") }}
	e, logs := newEngine(t, c, &memSeen{}, f, 50*time.Millisecond)
	run(t, e, logs, 300*time.Millisecond)
	if f.n() < 3 {
		t.Fatalf("only %d freeze attempts", f.n())
	}
	if c.committed() != 0 {
		t.Fatalf("committed %d while the freeze kept failing", c.committed())
	}
	if !strings.Contains(logs.String(), `"reason":"freeze_call_failed"`) || !strings.Contains(logs.String(), `"level":"error"`) {
		t.Fatalf("failure not logged:\n%s", logs)
	}

	// Recovers: the 5th attempt succeeds, then the batch commits.
	c2 := &fakeConsumer{batches: [][]kafka.Message{topups(t, 4)}}
	f2 := &fakeFreezer{err: func(n int) error {
		if n < 5 {
			return errors.New("freeze call: 503 ledger_unavailable")
		}
		return nil
	}}
	e2, logs2 := newEngine(t, c2, &memSeen{}, f2, time.Second)
	run(t, e2, logs2, 500*time.Millisecond)
	if f2.n() != 5 || c2.committed() != 4 || !strings.Contains(logs2.String(), `"attempts":5`) {
		t.Fatalf("calls=%d committed=%d\n%s", f2.n(), c2.committed(), logs2)
	}
}

func TestEngineRejectedFreezeIsReportedAndSkipped(t *testing.T) {
	c := &fakeConsumer{batches: [][]kafka.Message{topups(t, 4)}}
	f := &fakeFreezer{err: func(int) error { return fmt.Errorf("%w: 403 forbidden_operation", fraud.ErrFreezeRejected) }}
	e, logs := newEngine(t, c, &memSeen{}, f, time.Second)
	run(t, e, logs, 200*time.Millisecond)
	if f.n() != 1 || c.committed() != 4 || !strings.Contains(logs.String(), `"reason":"freeze_rejected"`) {
		t.Fatalf("calls=%d committed=%d\n%s", f.n(), c.committed(), logs)
	}
}

func TestEngineWaitsOutRedis(t *testing.T) {
	seen := &memSeen{}
	seen.down.Store(true)
	c := &fakeConsumer{batches: [][]kafka.Message{topups(t, 1)}}
	e, logs := newEngine(t, c, seen, &fakeFreezer{}, time.Second)
	go func() { time.Sleep(100 * time.Millisecond); seen.down.Store(false) }()
	run(t, e, logs, 400*time.Millisecond)
	if c.committed() != 1 || !strings.Contains(logs.String(), `"dependency":"redis"`) || !strings.Contains(logs.String(), `"reason":"refused"`) {
		t.Fatalf("committed=%d\n%s", c.committed(), logs)
	}
}

func TestHTTPFreezer(t *testing.T) {
	var got *http.Request
	var body string
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		var b bytes.Buffer
		b.ReadFrom(r.Body)
		body = b.String()
		w.WriteHeader(status)
		fmt.Fprint(w, `{"error":{"code":"x"}}`)
	}))
	defer srv.Close()
	f := fraud.NewHTTPFreezer(srv.URL+"/", "tok-123", time.Second)
	d := fraud.Decision{Rule: fraud.RuleTopupVelocity, AccountID: "acct-1", WindowStart: t0, WindowEnd: t0.Add(3 * time.Minute), Count: 4}
	if err := f.Freeze(context.Background(), d, trace, "00f067aa0ba902b7"); err != nil {
		t.Fatal(err)
	}
	if got.URL.Path != "/internal/v1/accounts/acct-1/freeze" || got.Header.Get("Authorization") != "Bearer tok-123" ||
		got.Header.Get("traceparent") != "00-"+trace+"-00f067aa0ba902b7-01" || !idempotency.ValidKey(got.Header.Get("Idempotency-Key")) ||
		!strings.Contains(body, `"rule":"topup_velocity"`) || !strings.Contains(body, `"reason":"topup_velocity: window from `+t0.UTC().Format("2006-01-02T15:04:05.000Z")+`"`) {
		t.Fatalf("request %s %v body %s", got.URL, got.Header, body)
	}
	for code, rejected := range map[int]bool{503: false, 500: false, 409: false, 429: false, 401: true, 403: true, 404: true, 400: true} {
		status = code
		err := f.Freeze(context.Background(), d, trace, "00f067aa0ba902b7")
		if err == nil || errors.Is(err, fraud.ErrFreezeRejected) != rejected {
			t.Errorf("%d: err=%v, want rejected=%v", code, err, rejected)
		}
	}
	if len(fraud.Reason(fraud.Decision{Rule: fraud.RuleDebitVolume, Count: 999, Sum: 999_999_999_999, Currency: "IDR"})) > 64 {
		t.Error("debit reason longer than 64 characters")
	}
}

// Every decision for one window must send the same key and body: ledger-api
// refuses a reused key with a different request (idempotency_key_reused).
// As events keep landing in a window, its count, sum and end grow.
func TestFreezeRequestIsFixedPerWindow(t *testing.T) {
	type req struct{ key, body string }
	capture := func(d fraud.Decision) req {
		t.Helper()
		var got req
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var b bytes.Buffer
			b.ReadFrom(r.Body)
			got = req{r.Header.Get("Idempotency-Key"), b.String()}
		}))
		defer srv.Close()
		if err := fraud.NewHTTPFreezer(srv.URL, "tok", time.Second).Freeze(context.Background(), d, trace, "00f067aa0ba902b7"); err != nil {
			t.Fatal(err)
		}
		return got
	}
	for _, rule := range []string{fraud.RuleTopupVelocity, fraud.RuleDebitVolume} {
		first := capture(fraud.Decision{Rule: rule, AccountID: "acct-1", Currency: "IDR", WindowStart: t0, WindowEnd: t0.Add(time.Minute), Count: 4, Sum: 400})
		later := capture(fraud.Decision{Rule: rule, AccountID: "acct-1", Currency: "IDR", WindowStart: t0, WindowEnd: t0.Add(4 * time.Minute), Count: 9, Sum: 900})
		if first != later {
			t.Errorf("%s: the same window sent different requests:\n %+v\n %+v", rule, first, later)
		}
		next := capture(fraud.Decision{Rule: rule, AccountID: "acct-1", Currency: "IDR", WindowStart: t0.Add(time.Second), WindowEnd: t0.Add(4 * time.Minute), Count: 4})
		if next.key == first.key {
			t.Errorf("%s: a new window reused the key %s", rule, first.key)
		}
		if n := len(fraud.Reason(fraud.Decision{Rule: rule, Currency: "IDR", WindowStart: t0})); n > 64 {
			t.Errorf("%s: reason is %d characters, max 64", rule, n)
		}
	}
	// debit_volume windows are per currency: same start, other currency, other key.
	idr := fraud.IdempotencyKey(fraud.Decision{Rule: fraud.RuleDebitVolume, AccountID: "acct-1", Currency: "IDR", WindowStart: t0})
	usd := fraud.IdempotencyKey(fraud.Decision{Rule: fraud.RuleDebitVolume, AccountID: "acct-1", Currency: "USD", WindowStart: t0})
	if idr == usd || !idempotency.ValidKey(idr) {
		t.Errorf("debit_volume keys: IDR %q, USD %q", idr, usd)
	}
}
