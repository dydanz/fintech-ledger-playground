package console_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

// fakeAuth is auth-api: it records each request and answers with a canned
// response per path.
type fakeAuth struct {
	mu   sync.Mutex
	reqs []recorded
	resp map[string]canned
}

type recorded struct {
	method, path string
	header       http.Header
	body         string
}

type canned struct {
	status int
	body   string
	header map[string]string
	delay  time.Duration
}

func (f *fakeAuth) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.reqs = append(f.reqs, recorded{r.Method, r.URL.Path, r.Header.Clone(), string(b)})
	c, ok := f.resp[r.Method+" "+r.URL.Path]
	f.mu.Unlock()
	if !ok {
		c = canned{status: 404, body: `{"error":{"code":"not_found","message":"route not found"}}`}
	}
	time.Sleep(c.delay)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	for k, v := range c.header {
		w.Header().Set(k, v)
	}
	w.WriteHeader(c.status)
	_, _ = io.WriteString(w, c.body)
}

func (f *fakeAuth) last(t *testing.T) recorded {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reqs) == 0 {
		t.Fatal("auth-api received no request")
	}
	return f.reqs[len(f.reqs)-1]
}

func newProxyServer(t *testing.T, auth http.Handler, timeout time.Duration) (http.Handler, *bytes.Buffer, string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	up := httptest.NewServer(auth)
	t.Cleanup(up.Close)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{
		Logger: logger.New(logs, slog.LevelDebug, "ledger-console"),
		Web:    builtApp(),
		Auth:   console.Upstream{Name: "auth-api", BaseURL: up.URL, Timeout: timeout},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r, logs, up.URL
}

func send(t *testing.T, h http.Handler, method, path, bearer, body string, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "console-test/1.0")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// Each console route maps to one auth-api route, with the method, body and
// the allowed headers passed through.
func TestAuthRoutesMapToAuthAPI(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{}}
	h, _, _ := newProxyServer(t, f, time.Second)
	for _, c := range []struct{ method, path, upstream string }{
		{"POST", "/console/api/auth/register", "/api/v1/auth/register"},
		{"POST", "/console/api/auth/login", "/api/v1/auth/login"},
		{"POST", "/console/api/auth/logout", "/api/v1/auth/logout"},
		{"GET", "/console/api/profile", "/api/v1/users/profile"},
		{"PUT", "/console/api/profile", "/api/v1/users/profile"},
		{"PUT", "/console/api/password", "/api/v1/users/password"},
	} {
		send(t, h, c.method, c.path, "tok", `{"x":1}`, nil)
		got := f.last(t)
		if got.method != c.method || got.path != c.upstream {
			t.Fatalf("%s %s reached %s %s, want %s %s", c.method, c.path, got.method, got.path, c.method, c.upstream)
		}
	}
}

func TestLoginPassesThroughUnchanged(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{
		"POST /api/v1/auth/login": {status: 200, body: `{"access_token":"abc","token_type":"Bearer","expires_in":3600}`,
			header: map[string]string{"Cache-Control": "no-store"}},
	}}
	h, _, _ := newProxyServer(t, f, time.Second)
	body := `{"email":"a@example.com","password":"correct horse battery"}`
	w := send(t, h, "POST", "/console/api/auth/login", "", body, map[string]string{"Cookie": "session=leak", "X-Forwarded-For": "1.2.3.4"})

	if w.Code != 200 || w.Body.String() != `{"access_token":"abc","token_type":"Bearer","expires_in":3600}` {
		t.Fatalf("response: %d %s", w.Code, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" || !strings.HasPrefix(w.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("response headers: %v", w.Header())
	}
	got := f.last(t)
	if got.body != body || got.header.Get("Content-Type") != "application/json" || got.header.Get("User-Agent") != "console-test/1.0" {
		t.Fatalf("upstream request: %+v", got)
	}
	// Only allow-listed headers go upstream.
	if got.header.Get("Cookie") != "" || got.header.Get("X-Forwarded-For") != "" || got.header.Get("Authorization") != "" {
		t.Fatalf("headers leaked upstream: %v", got.header)
	}
	// One trace across both services: same request id, same trace id, new span.
	if got.header.Get("X-Request-ID") != w.Header().Get("X-Request-ID") {
		t.Fatalf("X-Request-ID upstream %q, console %q", got.header.Get("X-Request-ID"), w.Header().Get("X-Request-ID"))
	}
	up, down := strings.Split(got.header.Get("traceparent"), "-"), strings.Split(w.Header().Get("traceparent"), "-")
	if len(up) != 4 || len(down) != 4 || up[1] != down[1] || up[2] == down[2] {
		t.Fatalf("traceparent upstream %q, console %q: want the same trace, a new span", got.header.Get("traceparent"), w.Header().Get("traceparent"))
	}
}

// Errors come back exactly as auth-api sent them: the app shows the
// message, and treats only 401 as "session ended".
func TestUpstreamErrorsPassThroughUnchanged(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{
		"PUT /api/v1/users/password": {status: 403, body: `{"error":{"code":"invalid_credentials","message":"current password is incorrect"}}`},
		"GET /api/v1/users/profile":  {status: 401, body: `{"error":{"code":"invalid_or_expired","message":"token expired"}}`},
	}}
	h, _, _ := newProxyServer(t, f, time.Second)
	w := send(t, h, "PUT", "/console/api/password", "tok-1", `{"current_password":"x","new_password":"y"}`, nil)
	if w.Code != 403 || !strings.Contains(w.Body.String(), "current password is incorrect") {
		t.Fatalf("403 not passed through: %d %s", w.Code, w.Body)
	}
	if got := f.last(t).header.Get("Authorization"); got != "Bearer tok-1" {
		t.Fatalf("Authorization upstream = %q", got)
	}
	if w := send(t, h, "GET", "/console/api/profile", "expired", "", nil); w.Code != 401 || !strings.Contains(w.Body.String(), "invalid_or_expired") {
		t.Fatalf("401 not passed through: %d %s", w.Code, w.Body)
	}
}

// auth-api unreachable: a 503 the app can show, and one dependency.call
// event, without the request body.
func TestUpstreamDownIs503AndLogged(t *testing.T) {
	closed := httptest.NewServer(&fakeAuth{})
	closed.Close() // nothing listens here any more
	gin.SetMode(gin.TestMode)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{Logger: logger.New(logs, slog.LevelDebug, "ledger-console"), Web: builtApp(),
		Auth: console.Upstream{Name: "auth-api", BaseURL: closed.URL, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	w := send(t, r, "POST", "/console/api/auth/login", "", `{"email":"a@example.com","password":"correct horse battery"}`, nil)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), `"code":"upstream_unavailable"`) {
		t.Fatalf("auth-api down: %d %s", w.Code, w.Body)
	}
	ev := depEvent(t, logs)
	for k, want := range map[string]any{"level": "warn", "outcome": "failure", "reason": "refused", "dependency": "auth-api", "operation": "auth_login"} {
		if ev[k] != want {
			t.Fatalf("dependency.call %s = %v, want %v (%v)", k, ev[k], want, ev)
		}
	}
	if _, ok := ev["duration_ms"]; !ok {
		t.Fatalf("dependency.call has no duration_ms: %v", ev)
	}
	if strings.Contains(logs.String(), "correct horse battery") || strings.Contains(logs.String(), "a@example.com") {
		t.Fatalf("request body in the logs:\n%s", logs)
	}
}

func TestUpstreamTimeoutIs503AndLogged(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{"GET /api/v1/users/profile": {status: 200, body: `{}`, delay: 300 * time.Millisecond}}}
	h, logs, _ := newProxyServer(t, f, 50*time.Millisecond)
	w := send(t, h, "GET", "/console/api/profile", "tok", "", nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("slow auth-api: %d %s", w.Code, w.Body)
	}
	if ev := depEvent(t, logs); ev["reason"] != "timeout" || ev["operation"] != "profile_get" {
		t.Fatalf("dependency.call = %v", ev)
	}
}

// A 5xx from auth-api is passed through, not logged again: auth-api has
// logged it.
func TestUpstream5xxIsNotLoggedTwice(t *testing.T) {
	f := &fakeAuth{resp: map[string]canned{"POST /api/v1/auth/login": {status: 500, body: `{"error":{"code":"internal_error","message":"internal server error"}}`}}}
	h, logs, _ := newProxyServer(t, f, time.Second)
	if w := send(t, h, "POST", "/console/api/auth/login", "", `{}`, nil); w.Code != 500 {
		t.Fatalf("5xx: %d", w.Code)
	}
	if strings.Contains(logs.String(), `"event":"dependency.call"`) {
		t.Fatalf("a reachable auth-api's 5xx was logged as a dependency failure:\n%s", logs)
	}
}

func TestWrongMethodOnAConsoleRoute(t *testing.T) {
	h, _, _ := newProxyServer(t, &fakeAuth{resp: map[string]canned{}}, time.Second)
	if w := send(t, h, "DELETE", "/console/api/profile", "tok", "", nil); w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /console/api/profile: %d %s", w.Code, w.Body)
	}
}

func depEvent(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["event"] == "dependency.call" {
			found = append(found, rec)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d dependency.call events, want 1:\n%s", len(found), logs)
	}
	return found[0]
}
