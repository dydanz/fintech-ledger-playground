package main

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
		os.Exit(startup.Healthcheck("HTTP_ADDR", ":8080"))
	}

	cfg, err := config.Load()
	log := logger.New(os.Stdout, cfg.LogLevel, cfg.ServiceName)
	slog.SetDefault(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}

	if err := run(cfg, log); err != nil {
		log.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

func run(cfg config.Config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 20)
	if err != nil {
		return err
	}
	defer db.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword, DB: cfg.RedisDB})
	defer rdb.Close()
	if err := startup.Retry(ctx, log, "redis", func(ctx context.Context) error { return rdb.Ping(ctx).Err() }); err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}

	users := repository.NewUserRepo(db)
	sessions := session.NewRedisStore(rdb)
	authSvc, err := service.NewAuthService(users, repository.NewTokenHistoryRepo(db), sessions, cfg.BcryptCost)
	if err != nil {
		return err
	}

	gin.SetMode(gin.ReleaseMode)
	router, err := api.NewRouter(api.Deps{
		Logger:         log,
		Auth:           authSvc,
		Users:          service.NewUserService(users),
		Sessions:       sessions,
		TrustedProxies: cfg.TrustedProxies,
		Ready: func(ctx context.Context) error {
			return errors.Join(db.Ping(ctx), rdb.Ping(ctx).Err())
		},
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
