// Package worker runs ledger-worker's periodic jobs. Every replica runs a
// Runner, and each job has one leader across replicas: the replica holding
// that job's session-level pg_advisory_lock (spec/TRD §3). When the leader's
// connection dies, Postgres releases the lock and another replica takes over
// within AcquireEvery.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"auth-api/internal/logger"
)

// Job is one periodic task. Run should log its own events (logging rule §4);
// the runner only logs leadership changes.
type Job struct {
	Name     string
	Interval time.Duration
	Run      func(ctx context.Context) error
}

type Runner struct {
	db           *pgxpool.Pool
	jobs         []Job
	AcquireEvery time.Duration // how often a follower retries for leadership
	LeaseCheck   time.Duration // how often a leader confirms its lock connection
}

func NewRunner(db *pgxpool.Pool, jobs ...Job) *Runner {
	return &Runner{db: db, jobs: jobs, AcquireEvery: 5 * time.Second, LeaseCheck: time.Second}
}

// Run blocks until ctx ends, running every job on this replica while it
// leads that job.
func (r *Runner) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, j := range r.jobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runJob(ctx, j)
		}()
	}
	wg.Wait()
}

func (r *Runner) runJob(ctx context.Context, j Job) {
	log := logger.FromContext(ctx).With("job", j.Name)
	ctx = logger.WithContext(ctx, log)
	for ctx.Err() == nil {
		led, err := r.lead(ctx, log, j)
		if err != nil && ctx.Err() == nil && !led {
			log.WarnContext(ctx, "job leadership check failed", "event", "app.leader", "outcome", "failure",
				"reason", "pg_error", "error", err)
		}
		sleep(ctx, r.AcquireEvery)
	}
}

// lead tries to become the job's leader and, if it does, runs the job every
// Interval until ctx ends or the lock connection fails. While leading, the
// lock connection is pinged every LeaseCheck; if it fails, the job's context
// is cancelled at once, so a long-running job (the outbox relay) stops
// before another replica can take over. led reports whether this replica
// held the lock at all.
func (r *Runner) lead(ctx context.Context, log *slog.Logger, j Job) (led bool, err error) {
	conn, err := r.db.Acquire(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Release()
	key := "ledger-worker:" + j.Name
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1, 0))`, key).Scan(&led); err != nil || !led {
		return false, err
	}
	log.InfoContext(ctx, "job leadership acquired", "event", "app.leader", "outcome", "success", "state", "acquired")

	jobCtx, cancelJob := context.WithCancel(ctx)
	lost := make(chan error, 1)
	var watch sync.WaitGroup
	watch.Add(1)
	go func() { // the lease: the lock lives exactly as long as conn
		defer watch.Done()
		for sleep(jobCtx, r.LeaseCheck) {
			if err := conn.Ping(jobCtx); err != nil && jobCtx.Err() == nil {
				lost <- err
				cancelJob()
				return
			}
		}
	}()
	defer func() {
		cancelJob()
		watch.Wait() // conn is free again
		// A session lock outlives the statement: release it explicitly (on
		// a fresh context, since ctx may be done), or drop the connection.
		uctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		if _, uerr := conn.Exec(uctx, `SELECT pg_advisory_unlock(hashtextextended($1, 0))`, key); uerr != nil {
			conn.Conn().Close(uctx)
		}
		log.InfoContext(ctx, "job leadership released", "event", "app.leader", "outcome", "success", "state", "released")
	}()

	for {
		_ = j.Run(jobCtx) // the job has logged its outcome
		if !sleep(jobCtx, j.Interval) {
			break
		}
	}
	select {
	case err := <-lost:
		log.WarnContext(ctx, "job leadership lost", "event", "app.leader", "outcome", "failure",
			"reason", "connection_lost", "error", err)
		return true, errors.Join(errors.New("leadership connection lost"), err)
	default:
		return true, nil
	}
}

// sleep waits d or until ctx ends, reporting whether the full wait elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
