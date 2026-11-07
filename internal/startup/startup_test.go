package startup_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"auth-api/internal/logger"
	"auth-api/internal/startup"
)

func TestRetryLogsEachAttemptAndSuccess(t *testing.T) {
	buf := &bytes.Buffer{}
	log := logger.New(buf, slog.LevelDebug, "test")
	calls := 0
	err := startup.Retry(context.Background(), log, "postgres", func(context.Context) error {
		calls++
		if calls < 2 {
			return fmt.Errorf("dial: %w", syscall.ECONNREFUSED)
		}
		return nil
	})
	if err != nil || calls != 2 {
		t.Fatalf("err %v after %d calls", err, calls)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("want 2 log lines, got %q", lines)
	}
	var retry, ok map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &retry)
	_ = json.Unmarshal([]byte(lines[1]), &ok)
	if retry["event"] != "dependency.connect" || retry["level"] != "warn" || retry["reason"] != "refused" || retry["dependency"] != "postgres" {
		t.Errorf("retry record = %v", retry)
	}
	if ok["event"] != "dependency.connect" || ok["outcome"] != "success" || ok["duration_ms"] == nil {
		t.Errorf("success record = %v", ok)
	}
}

func TestRetryStopsWhenContextIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := startup.Retry(ctx, logger.New(&bytes.Buffer{}, slog.LevelDebug, "t"), "redis", func(context.Context) error {
		return errors.New("down")
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestConnectReason(t *testing.T) {
	for err, want := range map[error]string{
		fmt.Errorf("x: %w", syscall.ECONNREFUSED):                   "refused",
		fmt.Errorf("x: %w", context.DeadlineExceeded):               "timeout",
		&pgconn.PgError{Code: "28P01"}:                              "auth_failed",
		errors.New(`FATAL: password authentication failed for "x"`): "auth_failed",
		errors.New("something else"):                                "unknown",
	} {
		if got := startup.ConnectReason(err); got != want {
			t.Errorf("ConnectReason(%v) = %q, want %q", err, got, want)
		}
	}
}
