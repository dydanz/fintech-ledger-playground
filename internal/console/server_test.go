package console_test

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

const indexHTML = `<!doctype html><html><head><title>console</title></head><body><div id="root"></div></body></html>`

// A built SPA as Vite writes it: index.html plus hashed assets.
func builtApp() fstest.MapFS {
	return fstest.MapFS{
		"index.html":              {Data: []byte(indexHTML)},
		"assets/index-3f9a1c.js":  {Data: []byte("console.log('app')")},
		"assets/index-77b2e0.css": {Data: []byte("body{}")},
		"favicon.svg":             {Data: []byte("<svg/>")},
	}
}

func newServer(t *testing.T, web fstest.MapFS) (http.Handler, *bytes.Buffer) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{Logger: logger.New(logs, slog.LevelDebug, "ledger-console"), Web: web})
	if err != nil {
		t.Fatal(err)
	}
	return r, logs
}

func get(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestServesTheAppAtRoot(t *testing.T) {
	h, _ := newServer(t, builtApp())
	w := get(t, h, "GET", "/")
	if w.Code != http.StatusOK || w.Body.String() != indexHTML {
		t.Fatalf("GET /: %d %q", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("Content-Type = %q", ct)
	}
	// index.html names the hashed assets, so it must never be cached stale.
	if cc := w.Header().Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("Cache-Control = %q, want no-cache", cc)
	}
	for k, want := range map[string]string{
		"Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'",
		"X-Content-Type-Options":  "nosniff",
		"Referrer-Policy":         "no-referrer",
	} {
		if got := w.Header().Get(k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
}

// Client-side routes (React Router) get the app shell, so a reload or a
// shared link to /transactions/<id> works.
func TestClientRoutesGetTheAppShell(t *testing.T) {
	h, _ := newServer(t, builtApp())
	for _, p := range []string{"/accounts", "/transactions/01a1c4fb-6f14-74d2-8a6a-6a0f0ffdff16", "/me", "/login?next=%2Fme"} {
		if w := get(t, h, "GET", p); w.Code != http.StatusOK || w.Body.String() != indexHTML {
			t.Fatalf("GET %s: %d %q, want the app shell", p, w.Code, w.Body)
		}
	}
	if w := get(t, h, "HEAD", "/accounts"); w.Code != http.StatusOK {
		t.Fatalf("HEAD /accounts: %d", w.Code)
	}
}

// Hashed assets never change, so they can be cached for a year.
func TestServesAssetsWithLongCache(t *testing.T) {
	h, _ := newServer(t, builtApp())
	w := get(t, h, "GET", "/assets/index-3f9a1c.js")
	if w.Code != http.StatusOK || w.Body.String() != "console.log('app')" {
		t.Fatalf("asset: %d %q", w.Code, w.Body)
	}
	if ct := w.Header().Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("Content-Type = %q", ct)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "public, max-age=31536000, immutable" {
		t.Fatalf("Cache-Control = %q", cc)
	}
	if w := get(t, h, "GET", "/favicon.svg"); w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("favicon: %d, Cache-Control %q", w.Code, w.Header().Get("Cache-Control"))
	}
}

// A missing asset is a real 404, so a broken build isn't hidden behind the
// app shell (the browser would run index.html as JavaScript).
func TestMissingAssetIs404(t *testing.T) {
	h, _ := newServer(t, builtApp())
	for _, p := range []string{"/assets/gone.js", "/assets/", "/favicon.ico"} {
		w := get(t, h, "GET", p)
		if w.Code != http.StatusNotFound || strings.Contains(w.Body.String(), "<html") {
			t.Fatalf("GET %s: %d %q, want a 404 that isn't the app", p, w.Code, w.Body)
		}
	}
}

// The API namespace is JSON only: an unknown API route is never the app.
func TestUnknownAPIRouteIsJSON(t *testing.T) {
	h, _ := newServer(t, builtApp())
	for _, p := range []string{"/console/api/nope", "/console/api/"} {
		w := get(t, h, "GET", p)
		if w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), `"code":"not_found"`) {
			t.Fatalf("GET %s: %d %s", p, w.Code, w.Body)
		}
	}
}

func TestPathsCannotEscapeTheWebRoot(t *testing.T) {
	h, _ := newServer(t, builtApp())
	for _, p := range []string{"/../../etc/passwd", "/assets/../../etc/passwd", "/%2e%2e/%2e%2e/etc/passwd"} {
		w := get(t, h, "GET", p)
		if strings.Contains(w.Body.String(), "root:") {
			t.Fatalf("GET %s leaked a file outside the web root", p)
		}
		if w.Code != http.StatusOK && w.Code != http.StatusNotFound {
			t.Fatalf("GET %s: %d", p, w.Code)
		}
	}
}

func TestOnlyGetAndHeadReachTheApp(t *testing.T) {
	h, _ := newServer(t, builtApp())
	if w := get(t, h, "POST", "/accounts"); w.Code != http.StatusNotFound {
		t.Fatalf("POST /accounts: %d, want 404", w.Code)
	}
}

func TestHealthAndReadiness(t *testing.T) {
	h, _ := newServer(t, builtApp())
	if w := get(t, h, "GET", "/healthz"); w.Code != http.StatusOK {
		t.Fatalf("healthz: %d", w.Code)
	}
	if w := get(t, h, "GET", "/readyz"); w.Code != http.StatusOK {
		t.Fatalf("readyz with a built app: %d %s", w.Code, w.Body)
	}
	empty, _ := newServer(t, fstest.MapFS{})
	if w := get(t, empty, "GET", "/readyz"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz without index.html: %d, want 503", w.Code)
	}
}

func TestRequestsAreAccessLogged(t *testing.T) {
	h, logs := newServer(t, builtApp())
	get(t, h, "GET", "/accounts")
	if !strings.Contains(logs.String(), `"message":"http_request"`) || !strings.Contains(logs.String(), `"service":"ledger-console"`) {
		t.Fatalf("no access log line:\n%s", logs)
	}
}
