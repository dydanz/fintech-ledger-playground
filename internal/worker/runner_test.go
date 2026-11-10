//go:build integration

package worker_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/testutil/pgtest"
	"fintech-ledger-playground/internal/worker"
)

// Two replicas: exactly one runs the job; when it stops, the other takes
// over within AcquireEvery.
func TestOneLeaderPerJobAndFailover(t *testing.T) {
	pool := pgtest.New(t)
	var runsA, runsB atomic.Int64
	newRunner := func(n *atomic.Int64) *worker.Runner {
		r := worker.NewRunner(pool, worker.Job{Name: "sweeper", Interval: 20 * time.Millisecond,
			Run: func(context.Context) error { n.Add(1); return nil }})
		r.AcquireEvery = 50 * time.Millisecond
		return r
	}
	logs := &bytes.Buffer{}
	base := logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "ledger-worker"))

	ctxA, stopA := context.WithCancel(base)
	doneA := make(chan struct{})
	go func() { newRunner(&runsA).Run(ctxA); close(doneA) }()
	time.Sleep(100 * time.Millisecond) // A leads
	ctxB, stopB := context.WithCancel(base)
	defer stopB()
	doneB := make(chan struct{})
	go func() { newRunner(&runsB).Run(ctxB); close(doneB) }()

	time.Sleep(300 * time.Millisecond)
	if runsA.Load() == 0 || runsB.Load() != 0 {
		t.Fatalf("while A leads: A ran %d, B ran %d", runsA.Load(), runsB.Load())
	}

	stopA()
	<-doneA
	deadline := time.Now().Add(time.Second)
	for runsB.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runsB.Load() == 0 {
		t.Fatal("B did not take over after A stopped")
	}
	stopB()
	<-doneB
	for _, want := range []string{`"state":"acquired"`, `"state":"released"`, `"job":"sweeper"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs missing %s:\n%s", want, logs)
		}
	}
}

// A leader whose connection dies loses the lock; another replica takes over.
func TestLeaderConnectionLossFailsOver(t *testing.T) {
	pool := pgtest.New(t)
	var runsA, runsB atomic.Int64
	mk := func(n *atomic.Int64) *worker.Runner {
		r := worker.NewRunner(pool, worker.Job{Name: "purge", Interval: 20 * time.Millisecond,
			Run: func(context.Context) error { n.Add(1); return nil }})
		r.AcquireEvery = 50 * time.Millisecond
		return r
	}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go mk(&runsA).Run(ctx)
	time.Sleep(100 * time.Millisecond)
	go mk(&runsB).Run(ctx)
	time.Sleep(100 * time.Millisecond)
	if runsA.Load() == 0 || runsB.Load() != 0 {
		t.Fatalf("A ran %d, B ran %d", runsA.Load(), runsB.Load())
	}

	// Kill the leader's lock session from outside, as a crashed replica would.
	// pg_locks is cluster-wide: stay inside this test's database.
	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`); err != nil {
		t.Fatal(err)
	}
	// Whichever replica wins, some leader runs the job again.
	time.Sleep(50 * time.Millisecond)
	before := runsA.Load() + runsB.Load()
	deadline := time.Now().Add(2 * time.Second)
	for runsA.Load()+runsB.Load() <= before+2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runsA.Load()+runsB.Load() <= before+2 {
		t.Fatal("no replica took over after the leader's session died")
	}
	var holders int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted
		AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&holders); err != nil || holders != 1 {
		t.Fatalf("advisory lock holders = %d, want 1 (%v)", holders, err)
	}
}

// A job that runs until its context ends (the relay) is stopped as soon as
// the leader's lock connection dies, before another replica can lead.
func TestLongRunningJobStopsWhenLeaseIsLost(t *testing.T) {
	pool := pgtest.New(t)
	stopped := make(chan struct{}, 1)
	r := worker.NewRunner(pool, worker.Job{Name: "relay", Interval: 10 * time.Millisecond,
		Run: func(ctx context.Context) error {
			<-ctx.Done()
			select {
			case stopped <- struct{}{}:
			default:
			}
			return nil
		}})
	r.AcquireEvery, r.LeaseCheck = time.Hour, 20*time.Millisecond // no re-acquire within the test
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go r.Run(ctx)
	time.Sleep(100 * time.Millisecond)

	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend(pid) FROM pg_locks
		WHERE locktype = 'advisory' AND granted AND pid <> pg_backend_pid()
		  AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("job kept running after its lease was lost")
	}
}
