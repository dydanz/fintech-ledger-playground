package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/service"
	"fintech-ledger-playground/internal/session"
	"fintech-ledger-playground/internal/startup"
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

	// `auth-api grant-role --email <email> --role ops|user --by <you>` (make grant-role):
	// the only way to change a role; no API route does it.
	if len(os.Args) > 1 && os.Args[1] == "grant-role" {
		if err := grantRole(cfg, log, os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "grant-role:", err)
			os.Exit(1)
		}
		return
	}

	if err := run(cfg, log); err != nil {
		log.Error("server exited with error", "error", err)
		os.Exit(1)
	}
}

// grantRole sets one user's role. The user.role_change event is logged by
// the service; the operator sees the outcome on stdout.
func grantRole(cfg config.Config, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("grant-role", flag.ContinueOnError)
	email := fs.String("email", "", "the user's email")
	role := fs.String("role", "ops", "ops or user")
	by := fs.String("by", "", "who is making the change (kept in user_role_changes)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *email == "" {
		return errors.New("--email is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := startup.ConnectPostgres(ctx, log, cfg.DatabaseURL, 2)
	if err != nil {
		return err
	}
	defer db.Close()
	u, err := service.NewRoleService(repository.NewUserRepo(db)).SetRole(logger.WithContext(ctx, log), *email, *role, *by)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return errors.New("no user with that email; register it first")
	case err != nil:
		return err
	}
	fmt.Printf("user %s now has role %s\n", u.ID, u.Role)
	return nil
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
