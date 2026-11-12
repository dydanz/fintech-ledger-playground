package console

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/logger"
)

// Access control (spec/2026-11-10 Web Console Design D§7.5). Each refusal is
// one console.access event (C1); a granted ops request is in the access log
// with the user's id.

// allowHosts serves only requests whose Host header is in the list (case
// insensitive). The health checks are exempt: they're served before this
// runs, and they reveal nothing.
func allowHosts(hosts []string) gin.HandlerFunc {
	allowed := map[string]bool{}
	for _, h := range hosts {
		allowed[strings.ToLower(strings.TrimSpace(h))] = true
	}
	return func(c *gin.Context) {
		if p := c.Request.URL.Path; p == "/healthz" || p == "/readyz" || allowed[strings.ToLower(c.Request.Host)] {
			c.Next()
			return
		}
		// The host the caller sent is not logged: it's caller-supplied text.
		deny(c, http.StatusForbidden, "host_not_allowed", "this console only answers on its own address", "host_not_allowed")
	}
}

// requireOps lets a request through only for a logged-in user with the ops
// role. The role is read from auth-api's profile with the caller's own token
// on every request, before the response cache, so a revoked role or an ended
// session takes effect at once.
func requireOps(auth *proxy) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !strings.HasPrefix(c.GetHeader("Authorization"), "Bearer ") {
			deny(c, http.StatusUnauthorized, "unauthorized", "log in with an ops account to see this", "missing_token")
			return
		}
		resp, ok := auth.call(c, http.MethodGet, "/api/v1/users/profile", userHeader(c), nil, "ops_access")
		if !ok {
			return // 503 answered, dependency.call logged
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBody))
		var u struct {
			ID   string `json:"id"`
			Role string `json:"role"`
		}
		switch {
		case resp.StatusCode == http.StatusUnauthorized:
			deny(c, http.StatusUnauthorized, "unauthorized", "your session ended; log in again", "invalid_token")
			return
		case resp.StatusCode != http.StatusOK || json.Unmarshal(body, &u) != nil || u.ID == "":
			ctx := c.Request.Context()
			logger.FromContext(ctx).ErrorContext(ctx, "ops access check failed", "event", "console.access", "outcome", "failure",
				"reason", "auth_error", "status", resp.StatusCode)
			api.AbortError(c, http.StatusServiceUnavailable, "upstream_unavailable", "auth-api couldn't check your access; try again")
			return
		}
		api.SetUserID(c, u.ID)
		if u.Role != "ops" {
			deny(c, http.StatusForbidden, "forbidden", "your account doesn't have ops access", "not_ops")
			return
		}
		c.Next()
	}
}

func deny(c *gin.Context, status int, code, msg, reason string) {
	ctx := c.Request.Context()
	logger.FromContext(ctx).WarnContext(ctx, "console access refused", "event", "console.access", "outcome", "denied", "reason", reason)
	api.AbortError(c, status, code, msg)
}
