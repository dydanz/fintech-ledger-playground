package api

import (
	"net/http"
	"slices"

	"github.com/gin-gonic/gin"

	"auth-api/internal/config"
	"auth-api/internal/logger"
	"auth-api/internal/token"
)

const (
	ctxClientID = "ledger.client_id"

	maxServiceTokenLen = 512
)

// Clients authenticates internal callers of ledger-api by bearer token
// (TRD §6.1). Only token hashes are held; the tokens themselves are never
// stored or logged.
type Clients struct {
	byHash map[string]config.ClientCredential
}

func NewClients(creds []config.ClientCredential) *Clients {
	c := &Clients{byHash: make(map[string]config.ClientCredential, len(creds))}
	for _, cred := range creds {
		c.byHash[cred.TokenHash] = cred
	}
	return c
}

// Require returns middleware that admits a caller whose token is known and
// whose grants include op. It sets the client id for the handler and the
// access log. Rejections emit ledger.client_auth (C1).
func (cl *Clients) Require(op string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		deny := func(status int, code, msg, reason string, attrs ...any) {
			logger.FromContext(ctx).WarnContext(ctx, "internal client rejected", append([]any{
				"event", "ledger.client_auth", "outcome", "denied", "reason", reason, "operation", op}, attrs...)...)
			if status == http.StatusUnauthorized {
				c.Header("WWW-Authenticate", `Bearer realm="ledger-api"`)
			}
			abortError(c, status, code, msg)
		}

		raw, ok := bearerToken(c.GetHeader("Authorization"))
		if !ok || len(raw) > maxServiceTokenLen {
			deny(http.StatusUnauthorized, "unauthorized", "missing or malformed bearer token", "missing_token")
			return
		}
		cred, ok := cl.byHash[token.Hash(raw)]
		if !ok {
			deny(http.StatusUnauthorized, "unauthorized", "unknown service token", "invalid_token")
			return
		}
		if !slices.Contains(cred.Operations, op) {
			deny(http.StatusForbidden, "forbidden", "client is not allowed to perform this operation", "forbidden_operation",
				"client_id", cred.ID)
			return
		}

		// AccessLog adds client_id to the request record; the service puts it
		// on its own events, so it isn't added to the request logger here.
		c.Set(ctxClientID, cred.ID)
		c.Next()
	}
}
