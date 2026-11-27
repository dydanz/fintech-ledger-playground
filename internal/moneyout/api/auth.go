package api

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	coreapi "fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/token"
)

const maxTokenLen = 512

// requireClient admits a caller whose bearer token hashes to a known
// MOM_CLIENTS entry. Only hashes are held; the token is never logged.
// Rejections emit transfer.client_auth.
func requireClient(clients []config.MoneyOutClient) gin.HandlerFunc {
	byHash := make(map[string]string, len(clients))
	for _, c := range clients {
		byHash[c.TokenHash] = c.ID
	}
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		deny := func(msg, reason string) {
			logger.FromContext(ctx).WarnContext(ctx, "ops client rejected",
				"event", "transfer.client_auth", "outcome", "denied", "reason", reason)
			c.Header("WWW-Authenticate", `Bearer realm="money-out-middleware"`)
			coreapi.AbortError(c, http.StatusUnauthorized, "unauthorized", msg)
		}
		raw, ok := bearer(c.GetHeader("Authorization"))
		if !ok || len(raw) > maxTokenLen {
			deny("missing or malformed bearer token", "missing_token")
			return
		}
		id, ok := byHash[token.Hash(raw)]
		if !ok {
			deny("unknown client token", "invalid_token")
			return
		}
		c.Set(coreapi.ClientIDKey, id)
		c.Next()
	}
}

func bearer(h string) (string, bool) {
	const prefix = "bearer "
	if len(h) <= len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return "", false
	}
	t := strings.TrimSpace(h[len(prefix):])
	return t, t != ""
}
