// Command ledger-worker runs the ledger's background jobs: the outbox relay
// to Kafka (spec/TRD §7.3), the stuck-transaction sweeper (§7.5), the
// reconciler (§7.6), partition maintenance (§7.7), and the idempotency-key and outbox purges. Every replica
// runs every job, but only the holder of a job's advisory lock executes it.
//
// `ledger-worker rebuild-projections [--apply] [--account <id>]` instead
// recomputes the projections from the events and journal, prints what
// differs, writes only with --apply, and exits.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"auth-api/internal/archive"
	"auth-api/internal/config"
	"auth-api/internal/kafka"
	"auth-api/internal/logger"
	"auth-api/internal/outbox"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/startup"
	"auth-api/internal/worker"
)

func main() {
	// Used as the container healthcheck: the runtime image has no shell or curl.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(startup.Healthcheck("WORKER_HTTP_ADDR", ":8082"))
	}

	cfg, err := config.LoadWorker()
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.ServiceName)
	slog.SetDefault(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}
	if len(os.Args) > 1 && os.Args[1] == "rebuild-projections" {
		os.Exit(rebuildProjections(cfg, log, os.Args[2:]))
	}

	if err := run(cfg, log); err != nil {
		log.Error("worker exited with error", "event", "app.lifecycle", "outcome", "failure", "reason", "startup_failed", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.WorkerConfig, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logger.WithContext(ctx, log)

	// Per job: one leader-lock connection (the relay also holds a LISTEN one),
	// plus the work itself.
	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 16)
	if err != nil {
		return err
	}
	defer db.Close()

	// The sweeper's events wake the relay like ledger-api's (outbox.Notifier).
	notifier := outbox.NewNotifier(db, 10*time.Millisecond)
	go notifier.Run(ctx)
	repo := repository.NewLedgerRepo(db, cfg.EventsTopic, notifier)
	// No Redis cache here: Postgres is the authority, and a response the
	// sweeper completes is cached by ledger-api on its first replay.
	ledgerSvc, err := service.NewLedgerService(repo, service.LedgerConfig{
		Currencies:     cfg.Currencies,
		MaxAmount:      cfg.MaxAmount,
		ProcessTimeout: cfg.ProcessTimeout,
		IdempotencyTTL: cfg.IdempotencyTTL,
	})
	if err != nil {
		return err
	}
	recovery, err := service.NewRecovery(ledgerSvc, repo, service.RecoveryConfig{
		SweepAfter: cfg.SweepAfter, PendingMaxAge: cfg.PendingMaxAge, SweepBatch: 100, PurgeBatch: 1000,
	})
	if err != nil {
		return err
	}

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, cfg.EventsTopic)
	if err != nil {
		return err
	}
	defer producer.Close()
	if err := producer.Connect(ctx, log); err != nil {
		return err
	}
	relay, err := outbox.NewRelay(repository.NewOutboxRepo(db), producer, outbox.RelayConfig{
		PollInterval: cfg.OutboxPoll, BatchSize: cfg.OutboxBatch, LagEvery: 30 * time.Second,
		Retention: cfg.OutboxRetention, PurgeBatch: 1000, MaxBackoff: 5 * time.Second, EscalateAfter: time.Minute,
		PublishTimeout: cfg.OutboxPublish,
	})
	if err != nil {
		return err
	}

	reconciler, err := service.NewReconciler(repository.NewReconcileRepo(db), service.ReconcileConfig{
		// Comfortably past the longest write transaction (Tx2 is bounded by
		// LEDGER_PROCESS_TIMEOUT). The watermark uses the Postgres clock.
		Overlap:   max(time.Minute, 2*cfg.ProcessTimeout),
		FullEvery: 24 * time.Hour, Batch: 500, BatchTimeout: 30 * time.Second, MaxReports: 100,
	})
	if err != nil {
		return err
	}

	partitions, err := service.NewPartitions(repository.NewPartitionRepo(db), service.PartitionConfig{MonthsAhead: 3, MinAhead: 2})
	if err != nil {
		return err
	}

	jobs := []worker.Job{}
	if cfg.Archive.BucketURI == "" {
		log.InfoContext(ctx, "archiver disabled: ARCHIVE_BUCKET_URI is not set", "event", "app.lifecycle", "outcome", "success")
	} else {
		a := cfg.Archive
		objects, err := archive.NewS3(archive.S3Config{BucketURI: a.BucketURI, Endpoint: a.Endpoint, AccessKey: a.AccessKey,
			SecretKey: a.SecretKey, Region: a.Region, Secure: a.Secure, RetentionDays: a.RetentionDays, CreateBucket: a.CreateBucket})
		if err != nil {
			return err
		}
		archiver, err := service.NewArchiver(repository.NewArchiveRepo(db), objects, service.ArchiveConfig{
			After: a.After, HoldAll: a.HoldAll, Hold: a.Hold, FileRows: 100_000})
		if err != nil {
			return err
		}
		jobs = append(jobs, worker.Job{Name: "archiver", Interval: 24 * time.Hour, Run: archiver.Run})
	}

	runner := worker.NewRunner(db,
		// Run returns only when leadership ends; Interval is the pause before
		// the next attempt after an unexpected return.
		worker.Job{Name: "outbox_relay", Interval: cfg.OutboxPoll, Run: relay.Run},
		worker.Job{Name: "outbox_purge", Interval: 10 * time.Minute, Run: relay.Purge},
		worker.Job{Name: "sweeper", Interval: cfg.SweepInterval, Run: recovery.Sweep},
		worker.Job{Name: "idempotency_purge", Interval: cfg.PurgeInterval, Run: recovery.PurgeIdempotencyKeys},
		worker.Job{Name: "reconciler", Interval: cfg.ReconcileEvery, Run: reconciler.Run},
		// Months ahead are plenty; hourly keeps the alert's signal fresh.
		worker.Job{Name: "partitions", Interval: time.Hour, Run: partitions.Run},
	)
	runner.AddJobs(jobs...)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		runner.Run(ctx)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	err = startup.Serve(ctx, log, srv, cfg.ShutdownTimeout)
	stop()
	wg.Wait() // jobs finish their current run and release their locks
	return err
}

// rebuildProjections is the reviewed repair path (spec/TRD §7.6). Exit codes:
// 0 nothing differed, or --apply wrote every difference; 3 a dry run found
// differences; 1 a row could not be rebuilt, or the run failed; 2 bad usage.
func rebuildProjections(cfg config.WorkerConfig, log *slog.Logger, args []string) int {
	fs := flag.NewFlagSet("rebuild-projections", flag.ContinueOnError)
	apply := fs.Bool("apply", false, "write the rebuilt rows (default: report the differences only)")
	account := fs.String("account", "", "rebuild only this account, its ledgers and its transactions")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "rebuild-projections: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logger.WithContext(ctx, log)
	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 2)
	if err != nil {
		return 1
	}
	defer db.Close()
	rb, err := service.NewRebuilder(repository.NewRebuildRepo(db, cfg.ProcessTimeout), 1000)
	if err != nil {
		return 1
	}
	rep, err := rb.Rebuild(ctx, service.RebuildOptions{Apply: *apply, AccountID: *account})
	switch {
	case err != nil:
		return 1
	case !*apply && rep.Changed > 0:
		fmt.Fprintf(os.Stderr, "rebuild-projections: %d row(s) differ; review the ledger.rebuild lines, then rerun with --apply\n", rep.Changed)
		return 3
	default:
		return 0
	}
}
