package console_test

import (
	"bytes"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

// #61: the console tells auth-api the browser's address, with the shared
// token, so a console login records it. The browser can't set either header.
func TestAuthCallsCarryTheClientAddress(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{"POST /api/v1/auth/login": {status: 200, body: `{}`}}}
	up := httptest.NewServer(f)
	t.Cleanup(up.Close)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{
		Logger: logger.New(logs, slog.LevelDebug, "ledger-console"),
		Web:    builtApp(),
		Auth:   console.Upstream{Name: "auth-api", BaseURL: up.URL, Timeout: time.Second, ConsoleToken: "shared-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/console/api/auth/login", strings.NewReader(`{}`))
	req.RemoteAddr = "198.51.100.23:51000"
	req.Header.Set(api.HeaderConsoleToken, "forged")
	req.Header.Set(api.HeaderConsoleClientIP, "203.0.113.99")
	r.ServeHTTP(httptest.NewRecorder(), req)

	got := f.last(t)
	if got.header.Get(api.HeaderConsoleToken) != "shared-secret" || got.header.Get(api.HeaderConsoleClientIP) != "198.51.100.23" {
		t.Fatalf("upstream headers: token %q, address %q; want the console's own token and the browser's 198.51.100.23",
			got.header.Get(api.HeaderConsoleToken), got.header.Get(api.HeaderConsoleClientIP))
	}
	if strings.Contains(logs.String(), "shared-secret") {
		t.Fatal("token logged")
	}
}

// Without a token configured, nothing is sent: auth-api would ignore it.
func TestNoConsoleTokenSendsNoClientAddress(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{"POST /api/v1/auth/login": {status: 200, body: `{}`}}}
	h, _, _ := newProxyServer(t, f, time.Second)
	send(t, h, "POST", "/console/api/auth/login", "", `{}`, map[string]string{api.HeaderConsoleClientIP: "203.0.113.99"})
	got := f.last(t)
	if got.header.Get(api.HeaderConsoleToken) != "" || got.header.Get(api.HeaderConsoleClientIP) != "" {
		t.Fatalf("headers sent without a token: %v", got.header)
	}
}
