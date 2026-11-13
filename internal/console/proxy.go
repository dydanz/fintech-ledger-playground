package console

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/startup"
)

// Upstream is a service the console calls on the app's behalf.
type Upstream struct {
	Name    string        // the dependency name in logs, e.g. auth-api
	BaseURL string        // e.g. http://api:8080; "" leaves its routes out
	Timeout time.Duration // per call; 0 means DefaultUpstreamTimeout
	// ConsoleToken (CONSOLE_PROXY_TOKEN, auth-api only): sent with the
	// client's address on every call, so auth-api records the browser's
	// address rather than the console's (#61). Never logged.
	ConsoleToken string
}

const DefaultUpstreamTimeout = 10 * time.Second

// maxUpstreamBody bounds what the console relays back (auth-api answers are
// small JSON).
const maxUpstreamBody = 1 << 20

// proxyRoute maps one console route to one upstream route.
type proxyRoute struct {
	method, path, upstreamPath, operation string
	keepQuery                             bool // pass the query string on (paging)
}

// authRoutes pass through to auth-api (D§7.4). The user's own token, when
// the app sends one, goes upstream as is; the console adds none.
var authRoutes = []proxyRoute{
	{"POST", "/auth/register", "/api/v1/auth/register", "auth_register", false},
	{"POST", "/auth/login", "/api/v1/auth/login", "auth_login", false},
	{"POST", "/auth/logout", "/api/v1/auth/logout", "auth_logout", false},
	{"GET", "/profile", "/api/v1/users/profile", "profile_get", false},
	{"PUT", "/profile", "/api/v1/users/profile", "profile_update", false},
	{"PUT", "/password", "/api/v1/users/password", "password_change", false},
}

// meRoutes pass through to ledger-api's user routes (D§5.4): ledger-api
// resolves the account from the user's own session.
var meRoutes = []proxyRoute{
	{"GET", "/me/account", "/api/v1/ledger/account", "me_account", false},
	{"GET", "/me/transactions", "/api/v1/ledger/transactions", "me_transactions", true},
}

// forwardedHeaders are the only request headers sent upstream. Cookies and
// X-Forwarded-* never are.
var forwardedHeaders = []string{"Content-Type", "Authorization", "User-Agent", "Accept"}

// returnedHeaders are the upstream response headers relayed to the app.
var returnedHeaders = []string{"Content-Type", "Cache-Control"}

type proxy struct {
	up     Upstream
	client *http.Client
}

func newProxy(up Upstream) *proxy {
	if up.Timeout <= 0 {
		up.Timeout = DefaultUpstreamTimeout
	}
	up.BaseURL = strings.TrimRight(up.BaseURL, "/")
	return &proxy{up: up, client: &http.Client{Timeout: up.Timeout}}
}

// handle relays one request. The upstream answer, including its error
// envelope and status (a 401 means the user's session ended), comes back
// unchanged. Only an unreachable upstream is the console's to report: a 503
// and one dependency.call event (C4). A reachable upstream's own errors are
// logged by that service, not again here. Bodies are never logged.
func (p *proxy) handle(rt proxyRoute) gin.HandlerFunc {
	return func(c *gin.Context) {
		target := rt.upstreamPath
		if rt.keepQuery && c.Request.URL.RawQuery != "" {
			target += "?" + c.Request.URL.RawQuery
		}
		hdr := http.Header{}
		for _, h := range forwardedHeaders {
			if v := c.GetHeader(h); v != "" {
				hdr.Set(h, v)
			}
		}
		resp, ok := p.call(c, rt.method, target, hdr, c.Request.Body, rt.operation)
		if !ok {
			return
		}
		defer resp.Body.Close()
		for _, h := range returnedHeaders {
			if v := resp.Header.Get(h); v != "" {
				c.Header(h, v)
			}
		}
		c.Status(resp.StatusCode)
		_, _ = io.Copy(c.Writer, io.LimitReader(resp.Body, maxUpstreamBody))
	}
}

// call sends one request upstream with the given headers, plus the request
// id and trace (one trace across both services: the same request id and
// trace id, a new span). If the upstream can't be reached it logs one
// dependency.call, answers 503 and returns ok=false; the caller closes the
// response otherwise.
func (p *proxy) call(c *gin.Context, method, target string, hdr http.Header, body io.Reader, operation string) (*http.Response, bool) {
	ctx := c.Request.Context()
	req, err := http.NewRequestWithContext(ctx, method, p.up.BaseURL+target, body)
	if err != nil {
		_ = c.Error(err)
		api.AbortError(c, http.StatusInternalServerError, "internal_error", "internal server error")
		return nil, false
	}
	req.Header = hdr
	req.Header.Set("X-Request-ID", c.GetString("request_id"))
	if p.up.ConsoleToken != "" {
		req.Header.Set(api.HeaderConsoleToken, p.up.ConsoleToken)
		req.Header.Set(api.HeaderConsoleClientIP, c.ClientIP())
	}
	if traceID := c.GetString("trace_id"); traceID != "" {
		req.Header.Set("traceparent", "00-"+traceID+"-"+newSpanID()+"-01")
	}
	start := time.Now()
	resp, err := p.client.Do(req)
	if err != nil {
		logger.FromContext(ctx).WarnContext(ctx, "upstream call failed", "event", "dependency.call", "outcome", "failure",
			"reason", callReason(err), "dependency", p.up.Name, "operation", operation,
			"duration_ms", float64(time.Since(start).Microseconds())/1000, "error", err)
		api.AbortError(c, http.StatusServiceUnavailable, "upstream_unavailable", p.up.Name+" is temporarily unavailable; try again")
		return nil, false
	}
	return resp, true
}

// callReason maps a client error to the dependency.call reasons: timeout,
// refused or unknown (startup.ConnectReason's classes, minus auth). A name
// that doesn't resolve counts as refused: in compose, that's a stopped
// upstream container.
func callReason(err error) string {
	switch r := startup.ConnectReason(err); r {
	case "timeout", "refused":
		return r
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr.IsNotFound {
		return "refused"
	}
	return "unknown"
}

func newSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
