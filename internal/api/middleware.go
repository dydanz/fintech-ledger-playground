package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/session"
	"fintech-ledger-playground/internal/token"
)

const (
	ctxRequestID = "request_id"
	ctxTraceID   = "trace_id"
	ctxUserID    = "auth.user_id"
	ctxTokenHash = "auth.token_hash"

	maxBodyBytes = 1 << 20
)

var (
	requestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)
	// W3C traceparent: version-traceid-parentid-flags
	traceparentPattern = regexp.MustCompile(`^([0-9a-f]{2})-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$`)
)

// RequestContext assigns a request ID (honouring a sane inbound X-Request-ID)
// and W3C trace identifiers (honouring a valid inbound traceparent, otherwise
// generating new ones), echoes both as response headers, and attaches a
// request-scoped logger carrying those fields to the request context.
func RequestContext(base *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.GetHeader("X-Request-ID")
		if !requestIDPattern.MatchString(reqID) {
			reqID = randomHex(16)
		}
		c.Set(ctxRequestID, reqID)
		c.Header("X-Request-ID", reqID)

		traceID, spanID, flags, ok := parseTraceparent(c.GetHeader("traceparent"))
		if !ok {
			traceID, spanID, flags = randomHex(16), randomHex(8), "01"
		}
		c.Set(ctxTraceID, traceID)
		c.Header("traceparent", "00-"+traceID+"-"+spanID+"-"+flags)

		l := base.With(
			slog.String("request_id", reqID),
			slog.String("trace_id", traceID),
			slog.String("span_id", spanID),
		)
		c.Request = c.Request.WithContext(logger.WithContext(c.Request.Context(), l))
		c.Next()
	}
}

// parseTraceparent accepts a W3C traceparent header. Per the spec, version ff
// and all-zero trace or parent IDs are invalid.
func parseTraceparent(h string) (traceID, spanID, flags string, ok bool) {
	m := traceparentPattern.FindStringSubmatch(h)
	if m == nil || m[1] == "ff" || isAllZero(m[2]) || isAllZero(m[3]) {
		return "", "", "", false
	}
	return m[2], m[3], m[4], true
}

func isAllZero(s string) bool { return strings.Trim(s, "0") == "" }

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// AccessLog emits one structured record per request after it completes.
// The query string is deliberately not logged since it may carry secrets.
func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		status := c.Writer.Status()
		level := slog.LevelInfo
		switch {
		case status >= 500:
			level = slog.LevelError
		case status >= 400:
			level = slog.LevelWarn
		}

		attrs := []slog.Attr{
			slog.String("method", c.Request.Method),
			slog.String("path", c.Request.URL.Path),
			slog.String("route", c.FullPath()),
			slog.Int("status", status),
			slog.Float64("latency_ms", float64(time.Since(start).Microseconds())/1000),
			slog.String("client_ip", c.ClientIP()),
			slog.Int("response_bytes", max(c.Writer.Size(), 0)),
			slog.String("user_agent", c.Request.UserAgent()),
		}
		if uid := c.GetString(ctxUserID); uid != "" {
			attrs = append(attrs, slog.String("user_id", uid))
		}
		if cid := c.GetString(ctxClientID); cid != "" {
			attrs = append(attrs, slog.String("client_id", cid))
		}
		if len(c.Errors) > 0 {
			attrs = append(attrs, slog.String("error", c.Errors.String()))
		}

		ctx := c.Request.Context()
		logger.FromContext(ctx).LogAttrs(ctx, level, "http_request", attrs...)
	}
}

// Recovery converts panics into a logged 500 JSON response.
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if r := recover(); r != nil {
				if err, ok := r.(error); ok && errors.Is(err, http.ErrAbortHandler) {
					panic(r)
				}
				ctx := c.Request.Context()
				logger.FromContext(ctx).ErrorContext(ctx, "panic recovered",
					"panic", r, "stack", string(debug.Stack()))
				abortError(c, http.StatusInternalServerError, "internal_error", "internal server error")
			}
		}()
		c.Next()
	}
}

// BodyLimit caps request bodies.
func BodyLimit(n int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body != nil {
			c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, n)
		}
		c.Next()
	}
}

// SessionLookup is the read side of the session store used by Auth.
type SessionLookup interface {
	Get(ctx context.Context, tokenHash string) (session.Session, error)
}

// SetUserID records the authenticated user for the rest of the request: the
// access log and every later log line carry user_id. For services that
// authenticate a user some other way (ledger-console asks auth-api).
func SetUserID(c *gin.Context, userID string) {
	c.Set(ctxUserID, userID)
	ctx := c.Request.Context()
	c.Request = c.Request.WithContext(logger.WithContext(ctx, logger.FromContext(ctx).With("user_id", userID)))
}

// Auth validates "Authorization: Bearer <token>" against Redis and injects
// the authenticated user ID into the Gin context. PostgreSQL is never hit.
func Auth(store SessionLookup) gin.HandlerFunc {
	return func(c *gin.Context) {
		raw, ok := bearerToken(c.GetHeader("Authorization"))
		if !ok || !token.WellFormed(raw) {
			c.Header("WWW-Authenticate", `Bearer realm="auth-api"`)
			abortError(c, http.StatusUnauthorized, "unauthorized", "missing or malformed bearer token")
			return
		}

		hash := token.Hash(raw)
		sess, err := store.Get(c.Request.Context(), hash)
		switch {
		case errors.Is(err, session.ErrNotFound):
			c.Header("WWW-Authenticate", `Bearer realm="auth-api", error="invalid_token"`)
			abortError(c, http.StatusUnauthorized, "invalid_token", "token is invalid or expired")
			return
		case err != nil:
			_ = c.Error(err)
			abortError(c, http.StatusServiceUnavailable, "session_store_unavailable", "authentication temporarily unavailable")
			return
		}

		c.Set(ctxUserID, sess.UserID)
		c.Set(ctxTokenHash, hash)
		ctx := c.Request.Context()
		c.Request = c.Request.WithContext(logger.WithContext(ctx, logger.FromContext(ctx).With("user_id", sess.UserID)))
		c.Next()
	}
}

func bearerToken(h string) (string, bool) {
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	t := strings.TrimSpace(h[len(prefix):])
	return t, t != ""
}
