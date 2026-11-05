// Package logger builds the service's structured JSON logger and carries
// request-scoped loggers through context.Context.
package logger

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"time"
)

// New returns a JSON logger writing to w. Every record carries
// "timestamp", "level", "message" and "service" fields so Loki/Grafana can
// parse and filter them consistently.
func New(w io.Writer, level slog.Leveler, service string) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) > 0 {
				return a
			}
			switch a.Key {
			case slog.TimeKey:
				return slog.String("timestamp", a.Value.Time().UTC().Format(time.RFC3339Nano))
			case slog.LevelKey:
				return slog.String("level", strings.ToLower(a.Value.String()))
			case slog.MessageKey:
				a.Key = "message"
			}
			return a
		},
	})
	return slog.New(h).With(slog.String("service", service))
}

type ctxKey struct{}

// WithContext stores l in ctx.
func WithContext(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// FromContext returns the request-scoped logger, or slog.Default().
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}
