// Command money-out-middleware is the money-out router and journal
// (spec/money-out TRD): it consumes do_transfer commands from
// money-out.commands.v1 through the idempotency gate, keeps each transfer's
// state in its own database, journals every transition to
// money-out.journal.v1 through its outbox, and serves the read-only ops API.
// It routes each RECEIVED transfer with the rule chain in MOM_ROUTING_FILE,
// reloaded when the file changes (M4.1), and, with MOM_BANK_MIDDLEWARES set,
// dispatches signed attempts to the bank middlewares, retries, fails over,
// inquires open attempts and applies their bank events (M4.2), with a
// circuit breaker per bank middleware and rail (M4.3).
// Reconciliation arrives in M6.
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
	"fintech-ledger-playground/internal/moneyout/bmwclient"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/routing"
	"fintech-ledger-playground/internal/moneyout/service"
	"fintech-ledger-playground/internal/moneyout/signing"
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
	breakers := service.NewBreakers(store, cfg.Breaker)

	// No routing, no start: there's no previous chain to fall back on.
	routes, err := routing.NewWatcher(ctx, cfg.RoutingFile, routing.Deps{Breakers: breakers})
	if err != nil {
		return err
	}

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

	jobs := []worker.Job{
		{Name: "outbox_relay", Interval: cfg.OutboxPoll, Run: relay.Run},
		{Name: "outbox_purge", Interval: 10 * time.Minute, Run: relay.Purge},
		{Name: "router", Interval: 200 * time.Millisecond, Run: service.NewRouter(store, routes, time.Now).Run},
	}
	var bankEvents *kafka.Consumer
	var dispatch *service.Dispatch
	if len(cfg.BankMiddlewares) > 0 {
		key, err := signing.ParsePrivateKey(cfg.SigningKey)
		if err != nil {
			return fmt.Errorf("MOM_SIGNING_KEY: %w", err)
		}
		client := bmwclient.New(signing.NewSigner(cfg.SigningKID, key), cfg.BankMiddlewares, &http.Client{Timeout: cfg.BMWTimeout})
		dispatch = service.NewDispatch(store, routes, client, service.DispatchConfig{InquiryInterval: cfg.InquiryInterval,
			InquiryFor: cfg.InquiryFor, StaleDispatch: cfg.StaleDispatch, RetryBase: cfg.RetryBase, RetryMax: cfg.RetryMax}).
			WithBreakers(breakers)
		jobs = append(jobs,
			worker.Job{Name: "dispatcher", Interval: 200 * time.Millisecond, Run: dispatch.RunDispatch},
			worker.Job{Name: "inquiries", Interval: min(cfg.InquiryInterval, time.Second), Run: dispatch.RunInquiries})
		if bankEvents, err = kafka.NewConsumer(cfg.KafkaBrokers, cfg.BankEventsGroup, contract.TopicBankEvents); err != nil {
			return err
		}
		defer bankEvents.Close()
	} else {
		log.WarnContext(ctx, "MOM_BANK_MIDDLEWARES is empty: transfers are routed but not dispatched", "event", "app.lifecycle",
			"outcome", "success", "dispatch", false)
	}

	var wg sync.WaitGroup
	wg.Add(4)
	go func() {
		defer wg.Done()
		routes.Run(ctx, cfg.RoutingReload)
	}()
	go func() { // every instance: the breaker_open rule reads it
		defer wg.Done()
		breakers.RunRefresh(ctx, time.Second)
	}()
	go func() {
		defer wg.Done()
		worker.NewRunner(db, jobs...).Run(ctx)
	}()
	if bankEvents != nil { // every instance consumes: the group spreads the partitions
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = service.NewConsumer(bankEvents, service.HandlerFunc(dispatch.ReceiveBankEvent), 100*time.Millisecond, 5*time.Second).
				WithNames("money_out_bank_events_poll", "money_out_bank_events_commit", "transfer.bank_event").Run(ctx)
		}()
	}
	go func() {
		defer wg.Done()
		_ = service.NewConsumer(consumer, service.NewIntake(store), 100*time.Millisecond, 5*time.Second).Run(ctx)
	}()
	log.InfoContext(ctx, "money-out-middleware consuming", "event", "app.lifecycle", "outcome", "success",
		"topic", contract.TopicCommands, "group", cfg.Group)

	gin.SetMode(gin.ReleaseMode)
	pairs := func() [][2]string {
		var out [][2]string
		for _, b := range routes.Chain().Config().BankMiddlewares {
			for _, r := range b.Rails {
				out = append(out, [2]string{b.Name, r})
			}
		}
		return out
	}
	router, err := moapi.NewRouter(moapi.Deps{Logger: log, Transfers: store, TrustedProxies: cfg.TrustedProxies, Clients: cfg.Clients,
		Breakers: breakers, Pairs: pairs,
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
