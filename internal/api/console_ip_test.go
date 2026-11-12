package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"fintech-ledger-playground/internal/api"
)

const consoleToken = "test-console-proxy-token"

// loginVia logs in with the given extra headers, from RemoteAddr 10.9.0.2
// (the console container, as auth-api sees it), and returns the IP stored
// in auth_token_history.
func loginVia(t *testing.T, h *harness, hdr map[string]string) string {
	t.Helper()
	if w := h.do("POST", "/api/v1/auth/register", "", map[string]string{"email": "ip@example.com", "password": "password-123"}); w.Code != http.StatusCreated {
		t.Fatalf("register: %d %s", w.Code, w.Body)
	}
	h.logs.Reset()
	b, _ := json.Marshal(map[string]string{"email": "ip@example.com", "password": "password-123"})
	req := httptest.NewRequest("POST", "/api/v1/auth/login", bytes.NewReader(b))
	req.RemoteAddr = "10.9.0.2:41000"
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.srv.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("login: %d %s", w.Code, w.Body)
	}
	h.history.mu.Lock()
	defer h.history.mu.Unlock()
	return h.history.rows[len(h.history.rows)-1].rec.LoginIP.String()
}

func withConsoleToken(d *api.Deps) { d.ConsoleToken = consoleToken }

func accessLogIP(t *testing.T, h *harness) string {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["message"] == "http_request" {
			return rec["client_ip"].(string)
		}
	}
	t.Fatalf("no access log line in %s", h.logs)
	return ""
}

// #61: a login through the console records the browser's address, which the
// console vouches for with the shared token; the access log shows it too.
func TestConsoleForwardedClientIP(t *testing.T) {
	h := newHarnessWith(t, withConsoleToken)
	got := loginVia(t, h, map[string]string{api.HeaderConsoleToken: consoleToken, api.HeaderConsoleClientIP: "203.0.113.7"})
	if got != "203.0.113.7" {
		t.Fatalf("login IP = %s, want the browser's 203.0.113.7", got)
	}
	if ip := accessLogIP(t, h); ip != "203.0.113.7" {
		t.Fatalf("access log client_ip = %s", ip)
	}
	if strings.Contains(h.logs.String(), consoleToken) {
		t.Fatal("console token logged")
	}
}

func TestConsoleForwardedClientIPIsIgnoredWithoutTheToken(t *testing.T) {
	for name, tc := range map[string]struct {
		configured bool
		hdr        map[string]string
		reason     string // the auth.proxy denial logged; "" for none
	}{
		"no header at all":          {true, nil, ""},
		"wrong token":               {true, map[string]string{api.HeaderConsoleToken: "guess", api.HeaderConsoleClientIP: "203.0.113.7"}, "invalid_token"},
		"address without a token":   {true, map[string]string{api.HeaderConsoleClientIP: "203.0.113.7"}, "missing_token"},
		"token, malformed address":  {true, map[string]string{api.HeaderConsoleToken: consoleToken, api.HeaderConsoleClientIP: "not-an-ip"}, "invalid_client_ip"},
		"auth-api has no token set": {false, map[string]string{api.HeaderConsoleToken: "", api.HeaderConsoleClientIP: "203.0.113.7"}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			var h *harness
			if tc.configured {
				h = newHarnessWith(t, withConsoleToken)
			} else {
				h = newHarness(t)
			}
			if got := loginVia(t, h, tc.hdr); got != "10.9.0.2" {
				t.Fatalf("login IP = %s, want the peer 10.9.0.2", got)
			}
			var denials []map[string]any
			for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
				var rec map[string]any
				if json.Unmarshal([]byte(line), &rec) == nil && rec["event"] == "auth.proxy" {
					denials = append(denials, rec)
				}
			}
			if tc.reason == "" {
				if len(denials) != 0 {
					t.Fatalf("unexpected auth.proxy events: %v", denials)
				}
				return
			}
			if len(denials) != 1 || denials[0]["outcome"] != "denied" || denials[0]["reason"] != tc.reason || denials[0]["level"] != "warn" {
				t.Fatalf("want one auth.proxy denied %s, got %v", tc.reason, denials)
			}
			if strings.Contains(h.logs.String(), "guess") || strings.Contains(h.logs.String(), "203.0.113.7") {
				t.Fatal("the rejected token or address was logged")
			}
		})
	}
}
