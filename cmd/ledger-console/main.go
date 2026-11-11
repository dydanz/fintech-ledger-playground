// Command ledger-console serves the web console (spec/2026-11-10 Web
// Console Design): the built React app and its /console/api routes.
package main

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/startup"
)

func main() {
	// Used as the container healthcheck: the runtime image has no shell or curl.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(startup.Healthcheck("CONSOLE_HTTP_ADDR", ":8090"))
	}

	cfg, err := config.LoadConsole()
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

func run(cfg config.ConsoleConfig, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	web := os.DirFS(cfg.WebDir)
	if _, err := fs.Stat(web, "index.html"); err != nil {
		return fmt.Errorf("CONSOLE_WEB_DIR %s has no built app: %w", cfg.WebDir, err)
	}

	gin.SetMode(gin.ReleaseMode)
	router, err := console.NewRouter(console.Deps{Logger: log, Web: web, TrustedProxies: cfg.TrustedProxies,
		Auth:   console.Upstream{Name: "auth-api", BaseURL: cfg.AuthAPIURL},
		Ledger: console.Upstream{Name: "ledger-api", BaseURL: cfg.LedgerAPIURL},
		Demo:   console.DemoConfig{Enabled: cfg.DemoActions, OnboardingToken: cfg.OnboardingToken, PaymentsToken: cfg.PaymentsToken}})
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
