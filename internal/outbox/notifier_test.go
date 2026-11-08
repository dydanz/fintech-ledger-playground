package outbox_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"auth-api/internal/logger"
	"auth-api/internal/outbox"
)

// fakeExec records each statement; it fails while err is set.
type fakeExec struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeExec) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, sql)
	return pgconn.CommandTag{}, f.err
}

func (f *fakeExec) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *fakeExec) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
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

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func startNotifier(t *testing.T, db outbox.Execer, every time.Duration) (*outbox.Notifier, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	ctx, cancel := context.WithCancel(logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "ledger-api")))
	n := outbox.NewNotifier(db, every)
	done := make(chan struct{})
	go func() { n.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return n, logs
}

// A burst of commits costs one NOTIFY per interval, and a commit after the
// last NOTIFY still gets one.
func TestNotifierCoalesces(t *testing.T) {
	db := &fakeExec{}
	n, _ := startNotifier(t, db, 100*time.Millisecond)

	for range 1000 {
		n.Notify()
	}
	eventually(t, "the first NOTIFY", func() bool { return db.count() >= 1 })
	time.Sleep(250 * time.Millisecond)
	if got := db.count(); got > 2 {
		t.Fatalf("%d NOTIFYs for one burst, want at most 2 (one now, one for calls during the interval)", got)
	}
	before := db.count()
	n.Notify()
	eventually(t, "a NOTIFY for a later commit", func() bool { return db.count() == before+1 })
	db.mu.Lock()
	first := db.calls[0]
	db.mu.Unlock()
	if !strings.Contains(first, "pg_notify") {
		t.Fatalf("statement = %q", first)
	}
}

func TestNotifierNilIsANoop(t *testing.T) {
	var n *outbox.Notifier
	n.Notify() // must not panic: repositories built without a notifier rely on the poll
}

// A failing NOTIFY is logged once per outage, as a degraded dependency call,
// and the notifier keeps going once Postgres is back.
func TestNotifierLogsFailureOncePerOutage(t *testing.T) {
	db := &fakeExec{err: errors.Join(errors.New("dial"), syscall.ECONNREFUSED)}
	n, logs := startNotifier(t, db, time.Millisecond)

	for i := range 5 {
		n.Notify()
		eventually(t, "a NOTIFY attempt", func() bool { return db.count() == i+1 })
	}
	if got := strings.Count(logs.String(), `"event":"dependency.call"`); got != 1 {
		t.Fatalf("%d dependency.call events for one outage, want 1:\n%s", got, logs)
	}
	for _, want := range []string{`"outcome":"failure"`, `"reason":"refused"`, `"dependency":"postgres"`, `"operation":"outbox_notify"`, `"level":"warn"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("log lacks %s:\n%s", want, logs)
		}
	}

	db.fail(nil)
	n.Notify()
	eventually(t, "a NOTIFY after recovery", func() bool { return db.count() == 6 })
	db.fail(context.DeadlineExceeded)
	n.Notify()
	eventually(t, "a second outage logged", func() bool { return strings.Count(logs.String(), `"event":"dependency.call"`) == 2 })
	if !strings.Contains(logs.String(), `"reason":"timeout"`) {
		t.Fatalf("second outage reason:\n%s", logs)
	}
}
