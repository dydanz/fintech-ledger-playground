// Package startup holds what every binary in cmd/ does before serving:
// connect to dependencies with retry, and answer the container healthcheck.
// It emits the dependency.connect events of the logging convention.
package startup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// RetryWindow bounds how long Retry keeps trying a dependency that is still
// starting, so containers can start in any order.
const RetryWindow = 30 * time.Second

// Retry calls fn with backoff for up to RetryWindow. Every failed attempt
// logs dependency.connect at warn (retrying); giving up logs it at error.
func Retry(ctx context.Context, log *slog.Logger, dependency string, fn func(context.Context) error) error {
	start := time.Now()
	delay := 250 * time.Millisecond
	deadline := start.Add(RetryWindow)
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := fn(attemptCtx)
		cancel()
		elapsed := float64(time.Since(start).Microseconds()) / 1000
		if err == nil {
			log.InfoContext(ctx, "dependency connected", "event", "dependency.connect", "outcome", "success",
				"dependency", dependency, "duration_ms", elapsed)
			return nil
		}
		if time.Now().After(deadline) {
			log.ErrorContext(ctx, "dependency unavailable, giving up", "event", "dependency.connect", "outcome", "failure",
				"reason", ConnectReason(err), "dependency", dependency, "duration_ms", elapsed, "error", err)
			return err
		}
		log.WarnContext(ctx, "dependency not ready, retrying", "event", "dependency.connect", "outcome", "failure",
			"reason", ConnectReason(err), "dependency", dependency, "retry_in", delay.String(), "error", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay = min(delay*2, 4*time.Second)
	}
}

// ConnectReason maps a connection error to the catalog's fixed reasons:
// timeout, refused, auth_failed or unknown.
func ConnectReason(err error) string {
	var pgErr *pgconn.PgError
	var netErr net.Error
	switch {
	case errors.As(err, &pgErr) && (pgErr.Code == "28P01" || pgErr.Code == "28000"):
		return "auth_failed"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case strings.Contains(strings.ToLower(err.Error()), "password authentication failed"):
		return "auth_failed"
	}
	return "unknown"
}

// ConnectPostgres opens a pool and waits for the server with Retry.
func ConnectPostgres(ctx context.Context, log *slog.Logger, url string, maxConns int32) (*pgxpool.Pool, error) {
	pcfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	pcfg.MaxConns = maxConns
	pcfg.MaxConnIdleTime = 5 * time.Minute
	db, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("create pg pool: %w", err)
	}
	if err := Retry(ctx, log, "postgres", db.Ping); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	return db, nil
}

// Healthcheck GETs /healthz on the address in env var addrEnv (default
// def) and returns a process exit code. The distroless images have no shell
// or curl, so each binary runs this as its `healthcheck` subcommand.
func Healthcheck(addrEnv, def string) int {
	addr := os.Getenv(addrEnv)
	if addr == "" {
		addr = def
	}
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return 1
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

// Serve runs srv until ctx is cancelled, then shuts it down gracefully,
// logging the app.lifecycle events.
func Serve(ctx context.Context, log *slog.Logger, srv *http.Server, shutdownTimeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		log.InfoContext(ctx, "http server listening", "event", "app.lifecycle", "outcome", "success", "addr", srv.Addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if !errors.Is(err, http.ErrServerClosed) {
			log.ErrorContext(ctx, "http server failed", "event", "app.lifecycle", "outcome", "failure",
				"reason", "startup_failed", "error", err)
			return err
		}
	case <-ctx.Done():
		log.Info("shutdown signal received", "event", "app.lifecycle", "outcome", "success")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown timed out", "event", "app.lifecycle", "outcome", "failure",
			"reason", "shutdown_timeout", "error", err)
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	log.Info("server stopped", "event", "app.lifecycle", "outcome", "success")
	return nil
}
