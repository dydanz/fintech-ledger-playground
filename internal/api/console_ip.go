package api

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/netip"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/logger"
)

// The headers ledger-console sends auth-api with every call (#61): the
// client address it saw, vouched for with the token both services share.
const (
	HeaderConsoleToken    = "X-Console-Token"
	HeaderConsoleClientIP = "X-Console-Client-IP"
)

const ctxClientIP = "client_ip"

// ConsoleClientIP makes the console's X-Console-Client-IP the request's
// client address (ClientIP), when X-Console-Token matches token. Without a
// match the headers are ignored and the peer address stands: a request
// that carries them anyway is logged as an auth.proxy denial (C1), without
// the header values. An empty token turns the headers off entirely.
func ConsoleClientIP(token string) gin.HandlerFunc {
	want := sha256.Sum256([]byte(token))
	return func(c *gin.Context) {
		got, hasToken := c.Request.Header[HeaderConsoleToken]
		addr := c.GetHeader(HeaderConsoleClientIP)
		if token == "" || (!hasToken && addr == "") {
			c.Next()
			return
		}
		reason := ""
		sum := sha256.Sum256([]byte(c.GetHeader(HeaderConsoleToken)))
		ip, err := netip.ParseAddr(addr)
		switch {
		case !hasToken || len(got) == 0:
			reason = "missing_token"
		case subtle.ConstantTimeCompare(sum[:], want[:]) != 1:
			reason = "invalid_token"
		case err != nil:
			reason = "invalid_client_ip"
		default:
			c.Set(ctxClientIP, ip.Unmap().String())
		}
		if reason != "" {
			ctx := c.Request.Context()
			logger.FromContext(ctx).WarnContext(ctx, "console client address ignored", "event", "auth.proxy",
				"outcome", "denied", "reason", reason)
		}
		c.Next()
	}
}

// ClientIP is the request's client address: the one the console vouched
// for, or gin's (the peer, or X-Forwarded-For from TRUSTED_PROXIES).
func ClientIP(c *gin.Context) string {
	if ip := c.GetString(ctxClientIP); ip != "" {
		return ip
	}
	return c.ClientIP()
}
