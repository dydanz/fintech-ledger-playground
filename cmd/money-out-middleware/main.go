// Command money-out-middleware is the money-out router and journal
// (spec/money-out TRD): it consumes do_transfer commands from
// money-out.commands.v1 through the idempotency gate, keeps each transfer's
// state in its own database, journals every transition to
// money-out.journal.v1 through its outbox, and serves the read-only ops API.
// Routing, dispatch and reconciliation arrive in later phases (M4, M6).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	moapi "fintech-ledger-playground/internal/moneyout/api"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/service"
	"fintech-ledger-playground/internal/outbox"
	"fintech-ledger-playground/internal/startup"
	"fintech-ledger-playground/internal/worker"
)

func main() {
	// The container healthcheck: the runtime image has no shell or curl.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(startup.Healthcheck("MOM_HTTP_ADDR", ":8085"))
	}
	cfg, err := config.LoadMoneyOut()
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.ServiceName)
	slog.SetDefault(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}
	if err := run(cfg, log); err != nil {
		log.Error("money-out-middleware exited with error", "event", "app.lifecycle", "outcome", "failure", "reason", "startup_failed", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.MoneyOutConfig, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logger.WithContext(ctx, log)

	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 16)
	if err != nil {
		return err
	}
	defer db.Close()
	notifier := outbox.NewNotifier(db, 10*time.Millisecond)
	go notifier.Run(ctx)
	store := repository.NewStore(db).WithNotifier(notifier)

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, contract.TopicJournal)
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

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.Group, contract.TopicCommands)
	if err != nil {
		return err
	}
	defer consumer.Close()
	if err := startup.Retry(ctx, log, "kafka", consumer.Ping); err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		worker.NewRunner(db,
			worker.Job{Name: "outbox_relay", Interval: cfg.OutboxPoll, Run: relay.Run},
			worker.Job{Name: "outbox_purge", Interval: 10 * time.Minute, Run: relay.Purge},
		).Run(ctx)
	}()
	go func() {
		defer wg.Done()
		_ = service.NewConsumer(consumer, service.NewIntake(store), 100*time.Millisecond, 5*time.Second).Run(ctx)
	}()
	log.InfoContext(ctx, "money-out-middleware consuming", "event", "app.lifecycle", "outcome", "success",
		"topic", contract.TopicCommands, "group", cfg.Group)

	gin.SetMode(gin.ReleaseMode)
	router, err := moapi.NewRouter(moapi.Deps{Logger: log, Transfers: store, TrustedProxies: cfg.TrustedProxies,
		Ready: func(ctx context.Context) error { return errors.Join(db.Ping(ctx), consumer.Ping(ctx)) }})
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: router, ReadHeaderTimeout: 5 * time.Second,
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	err = startup.Serve(ctx, log, srv, cfg.ShutdownTimeout)
	stop()
	wg.Wait() // finish the current command; an uncommitted batch is redelivered
	return err
}
