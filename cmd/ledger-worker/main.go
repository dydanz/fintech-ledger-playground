// Command ledger-worker runs the ledger's background jobs: the outbox relay
// to Kafka (spec/TRD §7.3), the stuck-transaction sweeper (§7.5), and the
// idempotency-key and outbox purges. Every replica
// runs every job, but only the holder of a job's advisory lock executes it.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

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
	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 12)
	if err != nil {
		return err
	}
	defer db.Close()

	repo := repository.NewLedgerRepo(db, cfg.EventsTopic)
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

	runner := worker.NewRunner(db,
		// Run returns only when leadership ends; Interval is the pause before
		// the next attempt after an unexpected return.
		worker.Job{Name: "outbox_relay", Interval: cfg.OutboxPoll, Run: relay.Run},
		worker.Job{Name: "outbox_purge", Interval: 10 * time.Minute, Run: relay.Purge},
		worker.Job{Name: "sweeper", Interval: cfg.SweepInterval, Run: recovery.Sweep},
		worker.Job{Name: "idempotency_purge", Interval: cfg.PurgeInterval, Run: recovery.PurgeIdempotencyKeys},
	)
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
