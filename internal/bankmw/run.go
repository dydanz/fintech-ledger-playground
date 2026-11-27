package bankmw

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/signing"
	"fintech-ledger-playground/internal/startup"
)

// Env is what a bank package gets to build itself.
type Env struct {
	Config  config.BankMiddlewareConfig
	Log     *slog.Logger
	Client  *http.Client // to the bank, with BMW_BANK_TIMEOUT
	Emitter *Emitter     // nil unless KAFKA_BROKERS is set (async banks)
}

// Built is a bank package's adapter and its own inbound routes.
type Built struct {
	Bank   Bank
	Routes func(*gin.Engine)
	Ready  func(ctx context.Context) error
}

// Main is every bank middleware's main: healthcheck, configuration, then
// serve until SIGTERM.
func Main(defaultName string, build func(Env) (Built, error)) {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(startup.Healthcheck("BMW_HTTP_ADDR", ":8086"))
	}
	cfg, err := config.LoadBankMiddleware(defaultName)
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.ServiceName)
	slog.SetDefault(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}
	gin.SetMode(gin.ReleaseMode)
	h, cleanup, err := Setup(cfg, log, build)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}
	defer cleanup()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: h, ReadHeaderTimeout: 5 * time.Second,
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	if err := startup.Serve(logger.WithContext(ctx, log), log, srv, cfg.ShutdownTimeout); err != nil {
		log.Error(cfg.ServiceName+" exited with error", "event", "app.lifecycle", "outcome", "failure", "reason", "serve_failed", "error", err)
		os.Exit(1)
	}
}

// Setup builds the handler: MOM's keys and the verifier, the emitter when
// configured, the bank package, and the router.
func Setup(cfg config.BankMiddlewareConfig, log *slog.Logger, build func(Env) (Built, error)) (http.Handler, func(), error) {
	keys, err := signing.ParsePublicKeys(cfg.PublicKeys)
	if err != nil {
		return nil, nil, fmt.Errorf("MOM_PUBLIC_KEYS: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil, errors.New("MOM_PUBLIC_KEYS: no keys")
	}
	v := signing.NewVerifier(keys, cfg.SignatureSkew, cfg.NonceCacheSize)
	v.OverCap = func(size int) {
		log.Warn("nonce cache past its soft cap; growing, no live nonce evicted", "event", "bank.nonce_cache", "outcome", "failure",
			"reason", "over_cap", "bank_middleware", cfg.ServiceName, "size", size, "cap", cfg.NonceCacheSize)
	}
	env := Env{Config: cfg, Log: log, Client: &http.Client{Timeout: cfg.BankTimeout}}
	cleanup := func() {}
	if len(cfg.KafkaBrokers) > 0 {
		p, err := kafka.NewProducer(cfg.KafkaBrokers, contract.TopicBankEvents)
		if err != nil {
			return nil, nil, fmt.Errorf("kafka: %w", err)
		}
		env.Emitter, cleanup = NewEmitter(cfg.ServiceName, p, time.Now), p.Close
	}
	built, err := build(env)
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	r, err := NewRouter(Deps{Name: cfg.ServiceName, Logger: log, Bank: built.Bank, Verifier: v, Ready: built.Ready,
		TrustedProxies: cfg.TrustedProxies, Routes: built.Routes})
	if err != nil {
		cleanup()
		return nil, nil, err
	}
	return r, cleanup, nil
}
