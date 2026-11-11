package console_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

const (
	onboardingTok = "dev-onboarding-token"
	paymentsTok   = "dev-payments-token"
	userTok       = "user-session-token"
	acctA         = "01a1c4f9-0000-7000-8000-00000000000a"
	userA         = "01a1c4f9-0000-7000-8000-0000000000aa"
)

type meEnv struct {
	h      http.Handler
	logs   *bytes.Buffer
	auth   *fakeAuth
	ledger *fakeAuth // the same fake: canned answers per path
}

func newMeEnv(t *testing.T, demo bool) *meEnv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	auth := &fakeAuth{resp: map[string]canned{
		"GET /api/v1/users/profile": {status: 200, body: `{"id":"` + userA + `","email":"a@example.com"}`},
	}}
	ledger := &fakeAuth{resp: map[string]canned{
		"GET /api/v1/ledger/account":                       {status: 200, body: `{"account":{"id":"` + acctA + `","user_id":"` + userA + `","status":"ACTIVE","ledgers":[{"currency":"IDR","balance":100}]}}`},
		"GET /api/v1/ledger/transactions":                  {status: 200, body: `{"transactions":[]}`},
		"POST /internal/v1/accounts":                       {status: 201, body: `{"account":{"id":"` + acctA + `"}}`},
		"POST /internal/v1/accounts/" + acctA + "/credits": {status: 201, body: `{"transaction":{"id":"tx-1","status":"SETTLED","amount":500,"currency":"IDR"}}`},
		"POST /internal/v1/accounts/" + acctA + "/debits":  {status: 422, body: `{"error":{"code":"insufficient_funds","message":"insufficient funds","transaction_id":"tx-2"}}`},
	}}
	authSrv, ledgerSrv := httptest.NewServer(auth), httptest.NewServer(ledger)
	t.Cleanup(authSrv.Close)
	t.Cleanup(ledgerSrv.Close)
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{
		Logger: logger.New(logs, slog.LevelDebug, "ledger-console"),
		Web:    builtApp(),
		Auth:   console.Upstream{Name: "auth-api", BaseURL: authSrv.URL, Timeout: time.Second},
		Ledger: console.Upstream{Name: "ledger-api", BaseURL: ledgerSrv.URL, Timeout: time.Second},
		Demo:   console.DemoConfig{Enabled: demo, OnboardingToken: onboardingTok, PaymentsToken: paymentsTok},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &meEnv{h: r, logs: logs, auth: auth, ledger: ledger}
}

func (e *meEnv) do(t *testing.T, method, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	hdr := map[string]string{}
	if key != "" {
		hdr["Idempotency-Key"] = key
	}
	return send(t, e.h, method, path, userTok, body, hdr)
}

func (f *fakeAuth) find(method, path string) []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, r := range f.reqs {
		if r.method == method && r.path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeAuth) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.reqs)
}

func demoEvent(t *testing.T, logs *bytes.Buffer) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["event"] == "console.demo_action" {
			found = append(found, rec)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d console.demo_action events, want 1:\n%s", len(found), logs)
	}
	return found[0]
}

// Reading the account is a pass-through to ledger-api's user routes with the
// user's own token: ledger-api resolves the account from the session.
func TestMyAccountReadsWithTheUserToken(t *testing.T) {
	e := newMeEnv(t, true)
	w := e.do(t, "GET", "/console/api/me/account", "", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), acctA) {
		t.Fatalf("me/account: %d %s", w.Code, w.Body)
	}
	if got := e.ledger.last(t); got.path != "/api/v1/ledger/account" || got.header.Get("Authorization") != "Bearer "+userTok {
		t.Fatalf("upstream: %s %s auth %q", got.method, got.path, got.header.Get("Authorization"))
	}
	e.do(t, "GET", "/console/api/me/transactions?limit=5&cursor=abc&currency=IDR", "", "")
	got := e.ledger.find("GET", "/api/v1/ledger/transactions")
	if len(got) != 1 {
		t.Fatalf("transactions not passed through")
	}
}

func TestMyTransactionsKeepsTheQuery(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"query":"` + r.URL.RawQuery + `"}`))
	}))
	defer up.Close()
	r, _ := console.NewRouter(console.Deps{Logger: logger.New(&bytes.Buffer{}, slog.LevelDebug, "ledger-console"), Web: builtApp(),
		Ledger: console.Upstream{Name: "ledger-api", BaseURL: up.URL}})
	w := send(t, r, "GET", "/console/api/me/transactions?limit=5&cursor=abc", userTok, "", nil)
	if !strings.Contains(w.Body.String(), "limit=5&cursor=abc") {
		t.Fatalf("query not forwarded: %s", w.Body)
	}
}

// Opening uses the user id from the user's own session (auth-api profile)
// and the console's onboarding token. The user's token never reaches the
// internal route, and the key makes a second open return the same account.
func TestOpenUsesTheSessionUserAndTheOnboardingToken(t *testing.T) {
	e := newMeEnv(t, true)
	w := e.do(t, "POST", "/console/api/me/open", "", "")
	if w.Code != 201 {
		t.Fatalf("open: %d %s", w.Code, w.Body)
	}
	if p := e.auth.find("GET", "/api/v1/users/profile"); len(p) != 1 || p[0].header.Get("Authorization") != "Bearer "+userTok {
		t.Fatalf("profile lookup = %+v", p)
	}
	open := e.ledger.find("POST", "/internal/v1/accounts")
	if len(open) != 1 {
		t.Fatal("no open call")
	}
	if open[0].body != `{"user_id":"`+userA+`"}` || open[0].header.Get("Authorization") != "Bearer "+onboardingTok ||
		open[0].header.Get("Idempotency-Key") != "console-open-"+userA {
		t.Fatalf("open call = %+v", open[0])
	}
	ev := demoEvent(t, e.logs)
	if ev["action"] != "open" || ev["outcome"] != "success" || ev["user_id"] != userA || ev["account_id"] != acctA {
		t.Fatalf("event = %v", ev)
	}
}

// A credit goes to the caller's own account only: resolved from the user's
// session, never from the request.
func TestCreditGoesToTheCallersOwnAccount(t *testing.T) {
	e := newMeEnv(t, true)
	w := e.do(t, "POST", "/console/api/me/credit", "click-1", `{"amount":500}`)
	if w.Code != 201 || !strings.Contains(w.Body.String(), "tx-1") {
		t.Fatalf("credit: %d %s", w.Code, w.Body)
	}
	c := e.ledger.find("POST", "/internal/v1/accounts/"+acctA+"/credits")
	if len(c) != 1 {
		t.Fatal("no credit call")
	}
	if c[0].body != `{"amount":500,"currency":"IDR"}` || c[0].header.Get("Authorization") != "Bearer "+paymentsTok ||
		c[0].header.Get("Idempotency-Key") != "console:"+acctA+":click-1" {
		t.Fatalf("credit call = %+v", c[0])
	}
	ev := demoEvent(t, e.logs)
	for k, want := range map[string]any{"level": "info", "action": "credit", "outcome": "success", "account_id": acctA,
		"user_id": userA, "transaction_id": "tx-1", "amount": float64(500), "currency": "IDR"} {
		if ev[k] != want {
			t.Fatalf("event %s = %v, want %v (%v)", k, ev[k], want, ev)
		}
	}
	if strings.Contains(e.logs.String(), paymentsTok) || strings.Contains(e.logs.String(), userTok) {
		t.Fatalf("a token is in the logs:\n%s", e.logs)
	}

	// The request can't name another account: unknown fields are refused.
	if w := e.do(t, "POST", "/console/api/me/credit", "click-2", `{"amount":5,"account_id":"someone-else"}`); w.Code != 400 {
		t.Fatalf("account_id in the body: %d %s", w.Code, w.Body)
	}
}

// A business refusal passes through, and is logged as denied with its code.
func TestDebitRefusalPassesThroughAsDenied(t *testing.T) {
	e := newMeEnv(t, true)
	w := e.do(t, "POST", "/console/api/me/debit", "click-1", `{"amount":999}`)
	if w.Code != 422 || !strings.Contains(w.Body.String(), "insufficient_funds") {
		t.Fatalf("debit: %d %s", w.Code, w.Body)
	}
	ev := demoEvent(t, e.logs)
	if ev["level"] != "warn" || ev["outcome"] != "denied" || ev["reason"] != "insufficient_funds" || ev["transaction_id"] != "tx-2" {
		t.Fatalf("event = %v", ev)
	}
}

func TestDemoAmountRules(t *testing.T) {
	e := newMeEnv(t, true)
	for i, body := range []string{`{"amount":0}`, `{"amount":-1}`, `{"amount":1000001}`, `{"amount":1.5}`, `{"amount":"5"}`, `{}`, `not json`} {
		if w := e.do(t, "POST", "/console/api/me/credit", "bad-"+string(rune('a'+i)), body); w.Code != 400 || !strings.Contains(w.Body.String(), `"invalid_input"`) {
			t.Fatalf("%s: %d %s, want 400 invalid_input", body, w.Code, w.Body)
		}
	}
	if n := len(e.ledger.find("POST", "/internal/v1/accounts/"+acctA+"/credits")); n != 0 {
		t.Fatalf("%d invalid credits reached ledger-api", n)
	}
	for _, body := range []string{`{"amount":1}`, `{"amount":1000000}`} {
		if w := e.do(t, "POST", "/console/api/me/credit", "ok-"+body[10:11], body); w.Code != 201 {
			t.Fatalf("%s: %d %s, want 201", body, w.Code, w.Body)
		}
	}
}

func TestDemoNeedsAWellFormedIdempotencyKey(t *testing.T) {
	e := newMeEnv(t, true)
	for _, key := range []string{"", "has space", strings.Repeat("k", 101)} {
		if w := e.do(t, "POST", "/console/api/me/credit", key, `{"amount":5}`); w.Code != 400 {
			t.Fatalf("key %q: %d %s, want 400", key, w.Code, w.Body)
		}
	}
}

func TestDemoActionsCanBeTurnedOff(t *testing.T) {
	e := newMeEnv(t, false)
	for _, p := range []string{"/console/api/me/open", "/console/api/me/credit", "/console/api/me/debit"} {
		w := e.do(t, "POST", p, "k-1", `{"amount":5}`)
		if w.Code != 403 || !strings.Contains(w.Body.String(), `"demo_actions_disabled"`) {
			t.Fatalf("%s with demo actions off: %d %s", p, w.Code, w.Body)
		}
	}
	if e.ledger.count() != 0 || e.auth.count() != 0 {
		t.Fatal("upstreams were called with demo actions off")
	}
	// Reading still works.
	if w := e.do(t, "GET", "/console/api/me/account", "", ""); w.Code != 200 {
		t.Fatalf("me/account with demo actions off: %d", w.Code)
	}
}

// An ended session stops the action before any write.
func TestDemoWithAnEndedSession(t *testing.T) {
	e := newMeEnv(t, true)
	e.ledger.resp["GET /api/v1/ledger/account"] = canned{status: 401, body: `{"error":{"code":"invalid_or_expired","message":"token expired"}}`}
	if w := e.do(t, "POST", "/console/api/me/credit", "k-1", `{"amount":5}`); w.Code != 401 {
		t.Fatalf("credit with an ended session: %d %s", w.Code, w.Body)
	}
	e.auth.resp["GET /api/v1/users/profile"] = canned{status: 401, body: `{"error":{"code":"invalid_or_expired","message":"token expired"}}`}
	if w := e.do(t, "POST", "/console/api/me/open", "", ""); w.Code != 401 {
		t.Fatalf("open with an ended session: %d %s", w.Code, w.Body)
	}
	if len(e.ledger.find("POST", "/internal/v1/accounts")) != 0 || len(e.ledger.find("POST", "/internal/v1/accounts/"+acctA+"/credits")) != 0 {
		t.Fatal("a write was attempted with an ended session")
	}
}

func TestDemoWithoutAnAccount(t *testing.T) {
	e := newMeEnv(t, true)
	e.ledger.resp["GET /api/v1/ledger/account"] = canned{status: 404, body: `{"error":{"code":"account_not_found","message":"account not found"}}`}
	if w := e.do(t, "POST", "/console/api/me/credit", "k-1", `{"amount":5}`); w.Code != 404 || !strings.Contains(w.Body.String(), "account_not_found") {
		t.Fatalf("credit without an account: %d %s", w.Code, w.Body)
	}
}

func TestDemoWhenLedgerAPIIsDown(t *testing.T) {
	e := newMeEnv(t, true)
	closed := httptest.NewServer(&fakeAuth{})
	closed.Close()
	authSrv := httptest.NewServer(e.auth)
	defer authSrv.Close()
	logs := &bytes.Buffer{}
	r, err := console.NewRouter(console.Deps{Logger: logger.New(logs, slog.LevelDebug, "ledger-console"), Web: builtApp(),
		Auth:   console.Upstream{Name: "auth-api", BaseURL: authSrv.URL},
		Ledger: console.Upstream{Name: "ledger-api", BaseURL: closed.URL},
		Demo:   console.DemoConfig{Enabled: true, OnboardingToken: onboardingTok, PaymentsToken: paymentsTok}})
	if err != nil {
		t.Fatal(err)
	}
	w := send(t, r, "POST", "/console/api/me/open", userTok, "", nil)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "upstream_unavailable") {
		t.Fatalf("ledger-api down: %d %s", w.Code, w.Body)
	}
	if ev := depEvent(t, logs); ev["dependency"] != "ledger-api" || ev["operation"] != "demo_open" || ev["reason"] != "refused" {
		t.Fatalf("dependency.call = %v", ev)
	}
}
