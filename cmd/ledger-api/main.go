// Command ledger-api serves the immutable ledger's internal HTTP API
// (spec/TRD §6).
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"auth-api/internal/api"
	"auth-api/internal/config"
	"auth-api/internal/logger"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/session"
	"auth-api/internal/startup"
)

func main() {
	// Used as the container healthcheck: the runtime image has no shell or curl.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(startup.Healthcheck("LEDGER_HTTP_ADDR", ":8081"))
	}

	cfg, err := config.LoadLedger()
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.ServiceName)
	slog.SetDefault(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}

	if err := run(cfg, log); err != nil {
		log.Error("server exited with error", "event", "app.lifecycle", "outcome", "failure", "reason", "startup_failed", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.LedgerConfig, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx = logger.WithContext(ctx, log)

	// Sized for two short transactions per request at ~300 TPS per replica.
	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 32)
	if err != nil {
		return err
	}
	defer db.Close()

	// Only the user routes need Redis (auth-api's sessions). Startup waits for
	// it, but readiness doesn't include it: a later Redis outage fails just the
	// user routes (503 from Auth) and leaves the internal write path serving.
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := startup.Retry(ctx, log, "redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() }); err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}

	ledgerSvc, err := service.NewLedgerService(repository.NewLedgerRepo(db, cfg.EventsTopic), service.LedgerConfig{
		Currencies:       cfg.Currencies,
		MaxAmount:        cfg.MaxAmount,
		ProcessTimeout:   cfg.ProcessTimeout,
		IdempotencyTTL:   cfg.IdempotencyTTL,
		OverdraftClients: cfg.OverdraftClients,
	})
	if err != nil {
		return err
	}
	if err := ledgerSvc.EnsureSystemLedgers(ctx); err != nil {
		return err
	}

	gin.SetMode(gin.ReleaseMode)
	router, err := api.NewLedgerRouter(api.LedgerDeps{
		Logger:         log,
		Ledger:         ledgerSvc,
		Clients:        api.NewClients(cfg.Clients),
		Sessions:       session.NewRedisStore(rdb),
		TrustedProxies: cfg.TrustedProxies,
		Ready:          db.Ping,
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	return startup.Serve(ctx, log, srv, cfg.ShutdownTimeout)
}
