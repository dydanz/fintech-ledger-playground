package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"

	"auth-api/internal/api"
	"auth-api/internal/logger"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/session"
	"auth-api/internal/token"
)

// --- in-memory fakes for the PostgreSQL repositories ---

type fakeUsers struct {
	mu   sync.Mutex
	byID map[string]*repository.User
	seq  int
}

func (f *fakeUsers) Create(_ context.Context, email, hash string) (repository.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.byID {
		if strings.EqualFold(u.Email, email) {
			return repository.User{}, repository.ErrEmailTaken
		}
	}
	f.seq++
	now := time.Now()
	u := &repository.User{ID: fmt.Sprintf("user-%d", f.seq), Email: email, PasswordHash: hash, CreatedAt: now, UpdatedAt: now}
	f.byID[u.ID] = u
	return *u, nil
}

func (f *fakeUsers) GetByEmail(_ context.Context, email string) (repository.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, u := range f.byID {
		if strings.EqualFold(u.Email, email) {
			return *u, nil
		}
	}
	return repository.User{}, repository.ErrNotFound
}

func (f *fakeUsers) GetByID(_ context.Context, id string) (repository.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if u, ok := f.byID[id]; ok {
		return *u, nil
	}
	return repository.User{}, repository.ErrNotFound
}

func (f *fakeUsers) UpdateProfile(_ context.Context, id string, p repository.ProfileUpdate) (repository.User, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	u, ok := f.byID[id]
	if !ok {
		return repository.User{}, repository.ErrNotFound
	}
	if p.FullName != nil {
		u.FullName = *p.FullName
	}
	if p.Bio != nil {
		u.Bio = *p.Bio
	}
	u.UpdatedAt = time.Now()
	return *u, nil
}

type historyRow struct {
	rec       repository.TokenRecord
	revokedAt *time.Time
}

type fakeHistory struct {
	mu   sync.Mutex
	rows []*historyRow
}

func (f *fakeHistory) Insert(_ context.Context, rec repository.TokenRecord) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, &historyRow{rec: rec})
	return nil
}

func (f *fakeHistory) Revoke(_ context.Context, hash string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.rows {
		if r.rec.TokenHash == hash && r.revokedAt == nil {
			now := time.Now()
			r.revokedAt = &now
			return true, nil
		}
	}
	return false, nil
}

// --- harness ---

type harness struct {
	t       *testing.T
	srv     http.Handler
	mr      *miniredis.Miniredis
	history *fakeHistory
	logs    *bytes.Buffer
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	gin.SetMode(gin.TestMode)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	users := &fakeUsers{byID: map[string]*repository.User{}}
	history := &fakeHistory{}
	sessions := session.NewRedisStore(rdb)

	authSvc, err := service.NewAuthService(users, history, sessions, bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}

	logs := &bytes.Buffer{}
	r, err := api.NewRouter(api.Deps{
		Logger:   logger.New(logs, slog.LevelDebug, "auth-api"),
		Auth:     authSvc,
		Users:    service.NewUserService(users),
		Sessions: sessions,
		Ready:    func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{t: t, srv: r, mr: mr, history: history, logs: logs}
}

func (h *harness) do(method, path, bearer string, body any) *httptest.ResponseRecorder {
	h.t.Helper()
	var rdr io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rdr = bytes.NewReader(b)
	}
	req := httptest.NewRequest(method, path, rdr)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "api-test/1.0")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	w := httptest.NewRecorder()
	h.srv.ServeHTTP(w, req)
	return w
}

func (h *harness) registerAndLogin(email, pw string) string {
	h.t.Helper()
	if w := h.do("POST", "/api/v1/auth/register", "", map[string]string{"email": email, "password": pw}); w.Code != http.StatusCreated {
		h.t.Fatalf("register: %d %s", w.Code, w.Body)
	}
	w := h.do("POST", "/api/v1/auth/login", "", map[string]string{"email": email, "password": pw})
	if w.Code != http.StatusOK {
		h.t.Fatalf("login: %d %s", w.Code, w.Body)
	}
	var res struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		h.t.Fatal(err)
	}
	if res.ExpiresIn != 3600 || res.TokenType != "Bearer" || !token.WellFormed(res.AccessToken) {
		h.t.Fatalf("unexpected login response: %+v", res)
	}
	return res.AccessToken
}

// --- tests ---

func TestFullFlow(t *testing.T) {
	h := newHarness(t)
	tok := h.registerAndLogin("Alice@Example.com", "correct horse battery")

	// Redis holds the session under the hashed key with a 1h TTL.
	key := session.KeyPrefix + token.Hash(tok)
	if !h.mr.Exists(key) {
		t.Fatalf("expected redis key %s", key)
	}
	if ttl := h.mr.TTL(key); ttl != time.Hour {
		t.Fatalf("ttl = %v, want 1h", ttl)
	}
	if h.mr.Exists(session.KeyPrefix + tok) {
		t.Fatal("raw token must not be used as a redis key")
	}

	// Audit trail stores only the hash plus request metadata.
	if len(h.history.rows) != 1 {
		t.Fatalf("history rows = %d", len(h.history.rows))
	}
	rec := h.history.rows[0].rec
	if rec.TokenHash != token.Hash(tok) || rec.UserAgent != "api-test/1.0" || !rec.LoginIP.IsValid() {
		t.Fatalf("bad audit record: %+v", rec)
	}

	// Profile update on a protected route.
	w := h.do("PUT", "/api/v1/users/profile", tok, map[string]string{"full_name": "  Alice Liddell ", "bio": "Down the rabbit hole"})
	if w.Code != http.StatusOK {
		t.Fatalf("update profile: %d %s", w.Code, w.Body)
	}
	var prof map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &prof)
	if prof["full_name"] != "Alice Liddell" || prof["email"] != "alice@example.com" {
		t.Fatalf("profile = %v", prof)
	}
	if _, leaked := prof["password_hash"]; leaked {
		t.Fatal("password hash leaked")
	}

	// Logout kills the Redis session and stamps the audit record.
	if w := h.do("POST", "/api/v1/auth/logout", tok, nil); w.Code != http.StatusNoContent {
		t.Fatalf("logout: %d %s", w.Code, w.Body)
	}
	if h.mr.Exists(key) {
		t.Fatal("session key survived logout")
	}
	if h.history.rows[0].revokedAt == nil {
		t.Fatal("audit record not revoked")
	}

	// Token no longer works.
	if w := h.do("PUT", "/api/v1/users/profile", tok, map[string]string{"bio": "x"}); w.Code != http.StatusUnauthorized {
		t.Fatalf("after logout: %d", w.Code)
	}
}

func TestTokenExpiresAfterOneHour(t *testing.T) {
	h := newHarness(t)
	tok := h.registerAndLogin("bob@example.com", "password123")

	h.mr.FastForward(59 * time.Minute)
	if w := h.do("GET", "/api/v1/users/profile", tok, nil); w.Code != http.StatusOK {
		t.Fatalf("before expiry: %d", w.Code)
	}
	h.mr.FastForward(2 * time.Minute)
	if w := h.do("GET", "/api/v1/users/profile", tok, nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("after expiry: %d", w.Code)
	}
}

func TestSessionWithoutTTLIsRejected(t *testing.T) {
	h := newHarness(t)
	tok := h.registerAndLogin("carol@example.com", "password123")
	key := session.KeyPrefix + token.Hash(tok)
	val, _ := h.mr.Get(key)
	_ = h.mr.Set(key, val) // re-set without TTL
	if h.mr.TTL(key) != 0 {
		t.Fatal("expected key without TTL")
	}

	if w := h.do("GET", "/api/v1/users/profile", tok, nil); w.Code != http.StatusUnauthorized {
		t.Fatalf("persisted key accepted: %d", w.Code)
	}
}

func TestAuthMiddlewareRejections(t *testing.T) {
	h := newHarness(t)
	valid, _ := token.Generate()

	cases := map[string]string{
		"missing":   "",
		"wrong":     "Basic abc",
		"malformed": "Bearer not-a-token",
		"unknown":   "Bearer " + valid,
	}
	for name, header := range cases {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/api/v1/auth/logout", nil)
			if header != "" {
				req.Header.Set("Authorization", header)
			}
			w := httptest.NewRecorder()
			h.srv.ServeHTTP(w, req)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("code = %d", w.Code)
			}
			if w.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("missing WWW-Authenticate")
			}
		})
	}
}

func TestRedisDownReturns503(t *testing.T) {
	h := newHarness(t)
	tok := h.registerAndLogin("dave@example.com", "password123")
	h.mr.SetError("connection refused")

	if w := h.do("GET", "/api/v1/users/profile", tok, nil); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("code = %d", w.Code)
	}
}

func TestLoginFailedRedisRevokesAuditRow(t *testing.T) {
	h := newHarness(t)
	if w := h.do("POST", "/api/v1/auth/register", "", map[string]string{"email": "erin@example.com", "password": "password123"}); w.Code != http.StatusCreated {
		t.Fatal(w.Body)
	}
	h.mr.SetError("boom")
	w := h.do("POST", "/api/v1/auth/login", "", map[string]string{"email": "erin@example.com", "password": "password123"})
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("code = %d", w.Code)
	}
	if len(h.history.rows) != 1 || h.history.rows[0].revokedAt == nil {
		t.Fatal("orphaned audit row should be revoked")
	}
}

func TestRegisterAndLoginErrors(t *testing.T) {
	h := newHarness(t)
	reg := func(email, pw string) int {
		return h.do("POST", "/api/v1/auth/register", "", map[string]string{"email": email, "password": pw}).Code
	}
	if c := reg("frank@example.com", "password123"); c != http.StatusCreated {
		t.Fatalf("register = %d", c)
	}
	if c := reg("FRANK@example.com", "password123"); c != http.StatusConflict {
		t.Fatalf("duplicate = %d", c)
	}
	if c := reg("not-an-email", "password123"); c != http.StatusBadRequest {
		t.Fatalf("bad email = %d", c)
	}
	if c := reg("g@example.com", "short"); c != http.StatusBadRequest {
		t.Fatalf("short pw = %d", c)
	}
	if c := reg("g@example.com", strings.Repeat("a", 73)); c != http.StatusBadRequest {
		t.Fatalf("long pw = %d", c)
	}

	login := func(email, pw string) *httptest.ResponseRecorder {
		return h.do("POST", "/api/v1/auth/login", "", map[string]string{"email": email, "password": pw})
	}
	wrongPw, unknown := login("frank@example.com", "wrongpass1"), login("nobody@example.com", "password123")
	if wrongPw.Code != http.StatusUnauthorized || unknown.Code != http.StatusUnauthorized {
		t.Fatalf("codes = %d, %d", wrongPw.Code, unknown.Code)
	}
	// Same body for both: no account enumeration.
	strip := func(b []byte) string {
		var m struct {
			Error struct{ Code, Message string }
		}
		_ = json.Unmarshal(b, &m)
		return m.Error.Code + m.Error.Message
	}
	if strip(wrongPw.Body.Bytes()) != strip(unknown.Body.Bytes()) {
		t.Fatal("login errors differ between unknown email and wrong password")
	}
}

func TestProfileValidation(t *testing.T) {
	h := newHarness(t)
	tok := h.registerAndLogin("gina@example.com", "password123")

	if w := h.do("PUT", "/api/v1/users/profile", tok, map[string]string{}); w.Code != http.StatusBadRequest {
		t.Fatalf("empty = %d", w.Code)
	}
	if w := h.do("PUT", "/api/v1/users/profile", tok, map[string]string{"bio": strings.Repeat("é", 501)}); w.Code != http.StatusBadRequest {
		t.Fatalf("long bio = %d", w.Code)
	}
}

func TestStructuredAccessLog(t *testing.T) {
	h := newHarness(t)
	req := httptest.NewRequest("GET", "/healthz?secret=abc", nil)
	req.Header.Set("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	h.srv.ServeHTTP(httptest.NewRecorder(), req)

	var rec map[string]any
	line := strings.TrimSpace(h.logs.String())
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("log line is not JSON: %q", line)
	}
	for _, k := range []string{"timestamp", "level", "message", "service", "request_id", "trace_id", "span_id", "method", "path", "status", "latency_ms", "client_ip"} {
		if _, ok := rec[k]; !ok {
			t.Errorf("missing log field %q in %v", k, rec)
		}
	}
	if rec["service"] != "auth-api" || rec["level"] != "info" || rec["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("unexpected log record: %v", rec)
	}
	if strings.Contains(line, "secret=abc") {
		t.Fatal("query string must not be logged")
	}
}

func TestTraceparentHonouredOrGenerated(t *testing.T) {
	const valid = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	cases := []struct {
		name, inbound, want string // want "" means a freshly generated traceparent
	}{
		{"valid inbound is echoed", valid, valid},
		{"missing is generated", "", ""},
		{"malformed is replaced", "not-a-traceparent", ""},
		{"uppercase hex is replaced", strings.ToUpper(valid), ""},
		{"version ff is replaced", "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", ""},
		{"all-zero trace id is replaced", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", ""},
		{"all-zero parent id is replaced", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", ""},
	}
	pattern := regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-01$`)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t)
			req := httptest.NewRequest("GET", "/healthz", nil)
			if tc.inbound != "" {
				req.Header.Set("traceparent", tc.inbound)
			}
			w := httptest.NewRecorder()
			h.srv.ServeHTTP(w, req)

			got := w.Header().Get("traceparent")
			m := pattern.FindStringSubmatch(got)
			switch {
			case m == nil:
				t.Fatalf("response traceparent %q is not a valid sampled traceparent", got)
			case tc.want != "" && got != tc.want:
				t.Fatalf("response traceparent = %q, want %q", got, tc.want)
			case tc.want == "" && got == tc.inbound:
				t.Fatalf("invalid inbound traceparent %q was echoed", tc.inbound)
			}

			var rec map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(h.logs.Bytes()), &rec); err != nil {
				t.Fatalf("log line is not JSON: %q", h.logs.String())
			}
			if rec["trace_id"] != m[1] || rec["span_id"] != m[2] {
				t.Fatalf("log trace_id/span_id = %v/%v, header = %q", rec["trace_id"], rec["span_id"], got)
			}
		})
	}
}

func TestGeneratedTraceIDsAreUnique(t *testing.T) {
	h := newHarness(t)
	seen := map[string]bool{}
	for range 50 {
		w := httptest.NewRecorder()
		h.srv.ServeHTTP(w, httptest.NewRequest("GET", "/healthz", nil))
		tp := w.Header().Get("traceparent")
		if seen[tp] {
			t.Fatalf("duplicate generated traceparent %q", tp)
		}
		seen[tp] = true
	}
}
