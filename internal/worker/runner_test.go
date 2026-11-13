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

// A hung leader (a frozen process: its connections stay open, but it stops
// pinging) loses the lock after LeaseTimeout, and another replica takes over
// (#59, chaos runbook §6.3). LeaseCheck of an hour stands in for the freeze.
func TestHungLeaderLosesTheLock(t *testing.T) {
	pool := pgtest.New(t)
	var runsA, runsB atomic.Int64
	hung := worker.NewRunner(pool, worker.Job{Name: "sweeper", Interval: time.Hour,
		Run: func(context.Context) error { runsA.Add(1); return nil }})
	hung.AcquireEvery, hung.LeaseCheck, hung.LeaseTimeout = time.Hour, time.Hour, 300*time.Millisecond
	follower := worker.NewRunner(pool, worker.Job{Name: "sweeper", Interval: 20 * time.Millisecond,
		Run: func(context.Context) error { runsB.Add(1); return nil }})
	follower.AcquireEvery = 50 * time.Millisecond

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go hung.Run(ctx)
	time.Sleep(100 * time.Millisecond)
	go follower.Run(ctx)
	time.Sleep(100 * time.Millisecond)
	if runsA.Load() != 1 || runsB.Load() != 0 {
		t.Fatalf("before the timeout: hung ran %d, follower ran %d", runsA.Load(), runsB.Load())
	}
	deadline := time.Now().Add(3 * time.Second)
	for runsB.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if runsB.Load() == 0 {
		t.Fatal("the follower never took over from the hung leader")
	}
}

// The lock connection's idle_session_timeout must not leak into the pool:
// a pooled connection with it set would be killed while idle.
func TestLeaseTimeoutIsResetBeforeTheConnectionReturnsToThePool(t *testing.T) {
	pool := pgtest.New(t)
	r := worker.NewRunner(pool, worker.Job{Name: "purge", Interval: 20 * time.Millisecond,
		Run: func(context.Context) error { return nil }})
	r.LeaseTimeout = 300 * time.Millisecond
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { r.Run(ctx); close(done) }()
	time.Sleep(100 * time.Millisecond)
	stop()
	<-done

	conns := pool.AcquireAllIdle(context.Background())
	defer func() {
		for _, c := range conns {
			c.Release()
		}
	}()
	if len(conns) == 0 {
		t.Fatal("no idle connections to check")
	}
	for _, c := range conns {
		var v string
		if err := c.QueryRow(context.Background(), `SHOW idle_session_timeout`).Scan(&v); err != nil || v != "0" {
			t.Fatalf("pooled connection has idle_session_timeout = %q (%v), want 0", v, err)
		}
	}
}
