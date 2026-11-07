// Command fraud-engine consumes ledger.events.v1, evaluates the velocity
// rules and freezes offending accounts through ledger-api (spec/TRD §7.4).
// Replicas share the consumer group fraud-engine, up to the topic's 12
// partitions.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"auth-api/internal/config"
	"auth-api/internal/fraud"
	"auth-api/internal/kafka"
	"auth-api/internal/logger"
	"auth-api/internal/startup"
)

func main() {
	// Used as the container healthcheck: the runtime image has no shell or curl.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(startup.Healthcheck("FRAUD_HTTP_ADDR", ":8083"))
	}

	cfg, err := config.LoadFraud()
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.ServiceName)
	slog.SetDefault(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}
	if err := run(cfg, log); err != nil {
		log.Error("fraud engine exited with error", "event", "app.lifecycle", "outcome", "failure", "reason", "startup_failed", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.FraudConfig, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logger.WithContext(ctx, log)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := startup.Retry(ctx, log, "redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() }); err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, cfg.Group, cfg.EventsTopic)
	if err != nil {
		return err
	}
	defer consumer.Close()
	if err := startup.Retry(ctx, log, "kafka", consumer.Ping); err != nil {
		return fmt.Errorf("connect kafka: %w", err)
	}

	eval, err := fraud.NewEvaluator(fraud.NewRedisWindowStore(rdb), fraud.Config{
		TopupMax: cfg.TopupMax, TopupWindow: cfg.TopupWindow,
		DebitWindow: cfg.DebitWindow, DebitMaxCount: cfg.DebitMaxCount, DebitLimits: cfg.DebitLimits,
	})
	if err != nil {
		return err
	}
	engine, err := fraud.NewEngine(consumer, fraud.NewRedisSeenStore(rdb, 7*24*time.Hour), eval,
		fraud.NewHTTPFreezer(cfg.LedgerAPIURL, cfg.ClientToken, 5*time.Second),
		fraud.EngineConfig{FreezeRetry: cfg.FreezeRetry, MinBackoff: 100 * time.Millisecond, MaxBackoff: 5 * time.Second})
	if err != nil {
		return err
	}
	log.InfoContext(ctx, "fraud engine consuming", "event", "app.lifecycle", "outcome", "success",
		"topic", cfg.EventsTopic, "group", cfg.Group)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = engine.Run(ctx)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if rdb.Ping(r.Context()).Err() != nil || consumer.Ping(r.Context()) != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	err = startup.Serve(ctx, log, srv, cfg.ShutdownTimeout)
	stop()
	wg.Wait() // finish the current event; an uncommitted batch is redelivered
	return err
}
