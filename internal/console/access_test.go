package console_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

// accessEvents returns the console.access events in the log.
func accessEvents(logs *bytes.Buffer) []map[string]any {
	var out []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["event"] == "console.access" {
			out = append(out, m)
		}
	}
	return out
}

// The ops routes need a logged-in user with the ops role (D§7.5). Every
// refusal is one console.access event; a granted request is in the access
// log with the user's id.
func TestOpsRoutesNeedTheOpsRole(t *testing.T) {
	for _, c := range []struct {
		name   string
		bearer string
		status int
		code   string
		reason string
	}{
		{"no token", "", 401, "unauthorized", "missing_token"},
		{"ended session", "expired-token", 401, "unauthorized", "invalid_token"},
		{"plain user", userTok, 403, "forbidden", "not_ops"},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newOpsEnv(t)
			for _, path := range []string{"/console/api/ops/monitor", "/console/api/ops/transactions", "/console/api/ops/transactions/" + txRecovered,
				"/console/api/ops/accounts", "/console/api/ops/accounts/a1"} {
				code, m := e.getAs(t, c.bearer, path)
				if code != c.status || dig(m, "error", "code") != c.code {
					t.Fatalf("%s: %d %v, want %d %s", path, code, m, c.status, c.code)
				}
			}
			if n := e.ledger.hit("/internal/v1/stats") + e.ledger.hit("/internal/v1/transactions") + e.ledger.hit("/internal/v1/accounts"); n != 0 {
				t.Fatalf("ledger-api read %d times for a refused caller", n)
			}
			evs := accessEvents(e.logs)
			if len(evs) != 5 {
				t.Fatalf("%d console.access events for 5 refusals:\n%s", len(evs), e.logs)
			}
			ev := evs[0]
			if ev["outcome"] != "denied" || ev["reason"] != c.reason || ev["level"] != "warn" {
				t.Fatalf("event = %v", ev)
			}
			if c.reason == "not_ops" && ev["user_id"] != "u-user" {
				t.Fatalf("not_ops event without the user: %v", ev)
			}
			if strings.Contains(e.logs.String(), c.bearer+`"`) && c.bearer != "" {
				t.Fatalf("the token was logged:\n%s", e.logs)
			}
		})
	}
}

func TestOpsUserIsAllowedAndAttributed(t *testing.T) {
	e := newOpsEnv(t)
	if code, m := e.get(t, "/console/api/ops/monitor"); code != 200 {
		t.Fatalf("ops user: %d %v", code, m)
	}
	if evs := accessEvents(e.logs); len(evs) != 0 {
		t.Fatalf("console.access events for a granted request: %v", evs)
	}
	var access map[string]any
	for _, l := range strings.Split(strings.TrimSpace(e.logs.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["message"] == "http_request" && m["path"] == "/console/api/ops/monitor" {
			access = m
		}
	}
	if access == nil || access["user_id"] != "u-ops" {
		t.Fatalf("access log line = %v, want user_id u-ops", access)
	}
}

// The role is checked on every request, before the 2 s cache: a cached
// answer is never served to someone who may not see it.
func TestOpsCacheDoesNotBypassTheRoleCheck(t *testing.T) {
	e := newOpsEnv(t)
	if code, _ := e.get(t, "/console/api/ops/monitor"); code != 200 {
		t.Fatal("ops user refused")
	}
	if code, _ := e.getAs(t, userTok, "/console/api/ops/monitor"); code != 403 {
		t.Fatalf("plain user got %d from the cache, want 403", code)
	}
}

func TestOpsWithoutAuthAPIIs503(t *testing.T) {
	gin.SetMode(gin.TestMode)
	closed := httptest.NewServer(&fakeProfiles{})
	closed.Close()
	led := httptest.NewServer(&fakeLedger{hits: map[string]int{}, stats: statsBody})
	t.Cleanup(led.Close)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{Logger: logger.New(logs, slog.LevelDebug, "ledger-console"), Web: builtApp(),
		Auth: console.Upstream{Name: "auth-api", BaseURL: closed.URL}, Ledger: console.Upstream{Name: "ledger-api", BaseURL: led.URL},
		Ops: console.OpsConfig{ReadToken: readTok}})
	if err != nil {
		t.Fatal(err)
	}
	w := send(t, r, "GET", "/console/api/ops/monitor", opsTok, "", nil)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "upstream_unavailable") || !strings.Contains(logs.String(), `"operation":"ops_access"`) {
		t.Fatalf("auth-api down: %d %s\n%s", w.Code, w.Body, logs)
	}
}

// Only the allowed Host headers are served (a DNS-rebinding page would send
// its own host). The health checks are exempt.
func TestAllowedHosts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{Logger: logger.New(logs, slog.LevelDebug, "ledger-console"), Web: builtApp(),
		AllowedHosts: []string{"localhost:8090", "127.0.0.1:8090"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		host, path string
		status     int
	}{
		{"localhost:8090", "/", 200},
		{"127.0.0.1:8090", "/accounts", 200},
		{"LOCALHOST:8090", "/", 200},
		{"evil.example:8090", "/", 403},
		{"evil.example", "/console/api/ops/monitor", 403},
		{"localhost:9999", "/", 403},
		{"evil.example", "/healthz", 200},
	} {
		req := httptest.NewRequest("GET", c.path, nil)
		req.Host = c.host
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != c.status {
			t.Fatalf("Host %s %s: %d, want %d", c.host, c.path, w.Code, c.status)
		}
		if c.status == 403 && !strings.Contains(w.Body.String(), "host_not_allowed") {
			t.Fatalf("Host %s: body %s", c.host, w.Body)
		}
	}
	evs := accessEvents(logs)
	if len(evs) != 3 || evs[0]["reason"] != "host_not_allowed" || evs[0]["outcome"] != "denied" {
		t.Fatalf("events = %v", evs)
	}
	if strings.Contains(logs.String(), "evil.example") {
		t.Fatalf("the caller-supplied host was logged:\n%s", logs)
	}
}
