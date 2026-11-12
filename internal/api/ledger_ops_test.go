//go:build integration

package api_test

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/outbox"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/service"
	"fintech-ledger-playground/internal/session"
	"fintech-ledger-playground/internal/testutil/pgtest"
	"fintech-ledger-playground/internal/token"
)

// A client without the read operation, for the 403 checks.
const freezeOnlyToken = "dev-freeze-only-token-0123456789"

type opsHarness struct {
	*ledgerHarness
	acct string
}

// newOpsHarness is the ledger harness with the ops read routes wired, and
// one open account.
func newOpsHarness(t *testing.T) *opsHarness {
	t.Helper()
	gin.SetMode(gin.TestMode)
	pool := pgtest.New(t)
	repo := repository.NewLedgerRepo(pool, outbox.DefaultTopic, nil)
	svc, err := service.NewLedgerService(repo, service.LedgerConfig{Currencies: []string{"IDR", "USD"}, MaxAmount: 1_000_000_000_000,
		ProcessTimeout: 2 * time.Second, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.EnsureSystemLedgers(context.Background()); err != nil {
		t.Fatal(err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { rdb.Close() })
	clients := append(testClients(), config.ClientCredential{ID: "fraud-engine", TokenHash: token.Hash(freezeOnlyToken), Operations: []string{"freeze"}})
	r, err := api.NewLedgerRouter(api.LedgerDeps{Logger: logger.New(&discard{}, slog.LevelDebug, "ledger-api"), Ledger: svc,
		Ops: service.NewOpsReader(repo), Clients: api.NewClients(clients), Sessions: session.NewRedisStore(rdb),
		Ready: func(context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	var user string
	if err := pool.QueryRow(context.Background(), `INSERT INTO users (email, password_hash) VALUES ('ops@x.io', 'x') RETURNING id`).Scan(&user); err != nil {
		t.Fatal(err)
	}
	h := &opsHarness{ledgerHarness: &ledgerHarness{t: t, srv: r, pool: pool, userA: user}}
	h.acct = h.openAccount()
	return h
}

type discard struct{}

func (*discard) Write(p []byte) (int, error) { return len(p), nil }

func (h *opsHarness) credit(key string, amount int, extra string) string {
	h.t.Helper()
	w := h.do("POST", "/internal/v1/accounts/"+h.acct+"/credits", payToken, key, fmt.Sprintf(`{"amount":%d,"currency":"IDR"%s}`, amount, extra))
	if w.Code != http.StatusCreated {
		h.t.Fatalf("credit %s: %d %s", key, w.Code, w.Body)
	}
	return field(h.t, w, "transaction", "id").(string)
}

func (h *opsHarness) void(key, id string) {
	h.t.Helper()
	if w := h.do("POST", "/internal/v1/transactions/"+id+"/void", opsToken, key, `{"reason":"test"}`); w.Code != http.StatusCreated {
		h.t.Fatalf("void %s: %d %s", id, w.Code, w.Body)
	}
}

// page fetches one page of the cross-account feed: ids, and the next cursor.
func (h *opsHarness) page(query string) ([]string, string) {
	h.t.Helper()
	w := h.do("GET", "/internal/v1/transactions?"+query, opsToken, "", "")
	if w.Code != http.StatusOK {
		h.t.Fatalf("list transactions?%s: %d %s", query, w.Code, w.Body)
	}
	var ids []string
	for _, tx := range field(h.t, w, "transactions").([]any) {
		ids = append(ids, tx.(map[string]any)["id"].(string))
	}
	next, _ := field(h.t, w, "next_cursor").(string)
	return ids, next
}

func TestOpsTransactionsPagesWithoutRepeats(t *testing.T) {
	h := newOpsHarness(t)
	want := map[string]bool{}
	for i := range 25 {
		want[h.credit(fmt.Sprintf("c-%d", i), 100+i, "")] = true
	}
	seen := map[string]bool{}
	cursor, pages := "", 0
	for {
		q := "limit=10"
		if cursor != "" {
			q += "&cursor=" + url.QueryEscape(cursor)
		}
		ids, next := h.page(q)
		pages++
		for _, id := range ids {
			if seen[id] {
				t.Fatalf("transaction %s repeated on page %d", id, pages)
			}
			seen[id] = true
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if pages != 3 || len(seen) != 25 {
		t.Fatalf("%d pages, %d unique transactions; want 3 and 25", pages, len(seen))
	}
	for id := range want {
		if !seen[id] {
			t.Fatalf("transaction %s missing from the pages", id)
		}
	}
}

// The feed is ordered by last change. A row that changes after its page was
// read isn't repeated on the next page, and a row that changes before its
// page is read isn't lost: it moves to the top, where the next poll of the
// first page finds it.
func TestOpsTransactionsRowThatChangesIsNotRepeatedOrLost(t *testing.T) {
	h := newOpsHarness(t)
	var ids []string // oldest first
	for i := range 10 {
		ids = append(ids, h.credit(fmt.Sprintf("c-%d", i), 100+i, ""))
	}
	first, cursor := h.page("limit=5") // the 5 newest: ids[9]..ids[5]
	if first[0] != ids[9] || cursor == "" {
		t.Fatalf("first page = %v, want newest first", first)
	}

	h.void("v-seen", ids[9])   // seen on page 1, changes now
	h.void("v-unseen", ids[2]) // not yet seen, changes before page 2 is read

	second, _ := h.page("limit=5&cursor=" + url.QueryEscape(cursor))
	for _, id := range second {
		for _, f := range first {
			if id == f {
				t.Fatalf("%s from page 1 repeated on page 2", id)
			}
		}
		if id == ids[2] {
			t.Fatalf("changed row %s still on page 2: the order isn't by last change", id)
		}
	}
	top, _ := h.page("limit=5")
	found := map[string]bool{}
	for _, id := range top {
		found[id] = true
	}
	if !found[ids[9]] || !found[ids[2]] {
		t.Fatalf("the next first page %v misses a changed row (%s, %s)", top, ids[9], ids[2])
	}
}

func TestOpsTransactionsFilters(t *testing.T) {
	h := newOpsHarness(t)
	a := h.credit("c-1", 100, "")
	h.credit("c-2", 200, "")
	h.void("v-1", a)
	ids, _ := h.page("type=VOID")
	if len(ids) != 1 {
		t.Fatalf("type=VOID: %d rows, want 1", len(ids))
	}
	ids, _ = h.page("status=VOIDED")
	if len(ids) != 1 || ids[0] != a {
		t.Fatalf("status=VOIDED: %v, want [%s]", ids, a)
	}
	ids, _ = h.page("since=" + url.QueryEscape(time.Now().Add(time.Hour).UTC().Format(time.RFC3339)))
	if len(ids) != 0 {
		t.Fatalf("since in the future: %d rows, want 0", len(ids))
	}
}

func TestOpsInputValidation(t *testing.T) {
	h := newOpsHarness(t)
	for _, q := range []string{"limit=0", "limit=101", "limit=x", "cursor=not-a-cursor", "status=LOST", "type=REFUND", "since=yesterday"} {
		if w := h.do("GET", "/internal/v1/transactions?"+q, opsToken, "", ""); w.Code != http.StatusBadRequest {
			t.Fatalf("?%s: %d, want 400", q, w.Code)
		}
	}
	for _, q := range []string{"limit=0", "cursor=bm90LWEtdXVpZA", "cursor=***"} { // base64 of "not-a-uuid"; not base64
		if w := h.do("GET", "/internal/v1/accounts?"+q, opsToken, "", ""); w.Code != http.StatusBadRequest {
			t.Fatalf("accounts?%s: %d, want 400", q, w.Code)
		}
	}
	if w := h.do("GET", "/internal/v1/stats?since=never", opsToken, "", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("stats?since=never: %d, want 400", w.Code)
	}
	for _, p := range []string{"/internal/v1/transactions/00000000-0000-7000-8000-000000000000/detail", "/internal/v1/transactions/nope/detail",
		"/internal/v1/accounts/00000000-0000-7000-8000-000000000000/events"} {
		if w := h.do("GET", p, opsToken, "", ""); w.Code != http.StatusNotFound {
			t.Fatalf("%s: %d, want 404", p, w.Code)
		}
	}
}

// The detail has the event stream, the double entry and the outbox rows,
// and never the caller's free text (reference, metadata).
func TestOpsDetailOmitsFreeText(t *testing.T) {
	h := newOpsHarness(t)
	id := h.credit("c-1", 180, `,"reference":"INV-secret-123","metadata":{"note":"private-note"}`)

	w := h.do("GET", "/internal/v1/transactions/"+id+"/detail", opsToken, "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("detail: %d %s", w.Code, w.Body)
	}
	for _, secret := range []string{"INV-secret-123", "private-note", `"reference"`, `"metadata"`} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("detail contains %s:\n%s", secret, w.Body)
		}
	}
	var types []string
	for _, e := range field(t, w, "events").([]any) {
		types = append(types, e.(map[string]any)["type"].(string))
	}
	if strings.Join(types, ",") != "TransactionRequested,TransactionProcessing,TransactionSettled" {
		t.Fatalf("event types = %v", types)
	}
	ev0 := field(t, w, "events").([]any)[0].(map[string]any)
	if ev0["payload"].(map[string]any)["requested_by"] != "payments" || ev0["occurred_at"] == nil || ev0["version"] != float64(1) {
		t.Fatalf("first event = %v", ev0)
	}
	lines := field(t, w, "journal").([]any)
	var sum float64
	kinds := map[string]bool{}
	for _, l := range lines {
		m := l.(map[string]any)
		sum += m["amount"].(float64)
		kinds[m["ledger_kind"].(string)] = true
	}
	if len(lines) != 2 || sum != 0 || !kinds["USER"] || !kinds["SYSTEM"] {
		t.Fatalf("journal = %v, want a balanced USER and SYSTEM pair", lines)
	}
	if out := field(t, w, "outbox").([]any); len(out) != 3 {
		t.Fatalf("outbox rows = %v, want one per event", out)
	}
	if field(t, w, "transaction", "id") != id || field(t, w, "transaction", "requested_by") != "payments" {
		t.Fatalf("transaction = %v", field(t, w, "transaction"))
	}

	list := h.do("GET", "/internal/v1/transactions", opsToken, "", "")
	if strings.Contains(list.Body.String(), "INV-secret-123") || strings.Contains(list.Body.String(), "private-note") {
		t.Fatalf("list contains free text:\n%s", list.Body)
	}
}

// An account's transactions page newest first, hold only that account's
// rows, and carry no caller free text.
func TestOpsAccountTransactions(t *testing.T) {
	h := newOpsHarness(t)
	var other string
	if err := h.pool.QueryRow(context.Background(), `INSERT INTO users (email, password_hash) VALUES ('ops2@x.io', 'x') RETURNING id`).Scan(&other); err != nil {
		t.Fatal(err)
	}
	w := h.do("POST", "/internal/v1/accounts", opsToken, "open-other", `{"user_id":"`+other+`"}`)
	if w.Code != http.StatusCreated {
		t.Fatalf("open other: %d %s", w.Code, w.Body)
	}
	otherAcct := field(t, w, "account", "id").(string)
	if w := h.do("POST", "/internal/v1/accounts/"+otherAcct+"/credits", payToken, "o-1", `{"amount":5,"currency":"IDR"}`); w.Code != http.StatusCreated {
		t.Fatalf("credit other: %d %s", w.Code, w.Body)
	}
	var want []string
	for i := range 12 {
		want = append(want, h.credit(fmt.Sprintf("a-%d", i), 100+i, `,"reference":"INV-secret-123","metadata":{"note":"private-note"}`))
	}
	var got []string
	cursor, pages := "", 0
	for {
		q := "limit=5"
		if cursor != "" {
			q += "&cursor=" + url.QueryEscape(cursor)
		}
		w := h.do("GET", "/internal/v1/accounts/"+h.acct+"/transactions?"+q, opsToken, "", "")
		if w.Code != http.StatusOK {
			t.Fatalf("page %d: %d %s", pages, w.Code, w.Body)
		}
		for _, secret := range []string{"INV-secret-123", "private-note", `"reference"`, `"metadata"`} {
			if strings.Contains(w.Body.String(), secret) {
				t.Fatalf("page contains %s:\n%s", secret, w.Body)
			}
		}
		pages++
		for _, tx := range field(t, w, "transactions").([]any) {
			m := tx.(map[string]any)
			if m["account_id"] != h.acct || m["requested_by"] != "payments" {
				t.Fatalf("row = %v", m)
			}
			got = append(got, m["id"].(string))
		}
		next, _ := field(t, w, "next_cursor").(string)
		if next == "" {
			break
		}
		cursor = next
	}
	if pages != 3 || len(got) != 12 {
		t.Fatalf("%d pages, %d rows; want 3 and 12", pages, len(got))
	}
	for i, id := range got { // newest first
		if id != want[len(want)-1-i] {
			t.Fatalf("row %d = %s, want %s (newest first)", i, id, want[len(want)-1-i])
		}
	}
	base := "/internal/v1/accounts/" + h.acct + "/transactions?"
	for _, q := range []string{"limit=0", "limit=101", "cursor=not-a-cursor"} {
		if w := h.do("GET", base+q, opsToken, "", ""); w.Code != http.StatusBadRequest {
			t.Fatalf("?%s: %d, want 400", q, w.Code)
		}
	}
	for _, id := range []string{"00000000-0000-7000-8000-000000000000", "nope"} {
		if w := h.do("GET", "/internal/v1/accounts/"+id+"/transactions", opsToken, "", ""); w.Code != http.StatusNotFound || field(t, w, "error", "code") != "account_not_found" {
			t.Fatalf("account %s: %d %s, want 404 account_not_found", id, w.Code, w.Body)
		}
	}
}

// Freeze and unfreeze reasons are free text an operator typed: the events
// keep the actor and the rule, never the reason.
func TestOpsAccountEventsOmitFreezeReason(t *testing.T) {
	h := newOpsHarness(t)
	if w := h.do("POST", "/internal/v1/accounts/"+h.acct+"/freeze", opsToken, "f-1", `{"reason":"customer called about card 4111","rule":"topup_velocity"}`); w.Code != http.StatusOK {
		t.Fatalf("freeze: %d %s", w.Code, w.Body)
	}
	if w := h.do("POST", "/internal/v1/accounts/"+h.acct+"/unfreeze", opsToken, "u-1", `{"reason":"verified by phone 0812"}`); w.Code != http.StatusOK {
		t.Fatalf("unfreeze: %d %s", w.Code, w.Body)
	}
	w := h.do("GET", "/internal/v1/accounts/"+h.acct+"/events", opsToken, "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("account events: %d %s", w.Code, w.Body)
	}
	for _, secret := range []string{"4111", "0812", "customer called", "verified by"} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatalf("account events contain %q:\n%s", secret, w.Body)
		}
	}
	var types []string
	for _, e := range field(t, w, "events").([]any) {
		m := e.(map[string]any)
		types = append(types, m["type"].(string))
		if m["type"] == "AccountFrozen" {
			p := m["payload"].(map[string]any)
			if p["actor"] != "ops" || p["rule"] != "topup_velocity" {
				t.Fatalf("AccountFrozen payload = %v", p)
			}
		}
	}
	if strings.Join(types, ",") != "AccountOpened,LedgerOpened,AccountFrozen,AccountUnfrozen" {
		t.Fatalf("event types = %v", types)
	}
}

func TestOpsAccountsList(t *testing.T) {
	h := newOpsHarness(t)
	for i := range 3 {
		var u string
		if err := h.pool.QueryRow(context.Background(), `INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`, fmt.Sprintf("u%d@x.io", i)).Scan(&u); err != nil {
			t.Fatal(err)
		}
		if w := h.do("POST", "/internal/v1/accounts", opsToken, fmt.Sprintf("open-%d", i), `{"user_id":"`+u+`"}`); w.Code != http.StatusCreated {
			t.Fatalf("open: %d %s", w.Code, w.Body)
		}
	}
	w := h.do("GET", "/internal/v1/accounts?limit=3", opsToken, "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("list: %d %s", w.Code, w.Body)
	}
	first := field(t, w, "accounts").([]any)
	next, _ := field(t, w, "next_cursor").(string)
	if len(first) != 3 || next == "" {
		t.Fatalf("first page = %d accounts, cursor %q; want 3 and a cursor", len(first), next)
	}
	if ledgers := first[0].(map[string]any)["ledgers"].([]any); len(ledgers) != 1 {
		t.Fatalf("account ledgers = %v, want the IDR ledger", ledgers)
	}
	w = h.do("GET", "/internal/v1/accounts?limit=3&cursor="+url.QueryEscape(next), opsToken, "", "")
	if rest := field(t, w, "accounts").([]any); len(rest) != 1 {
		t.Fatalf("second page = %d accounts, want 1", len(rest))
	}
	// With user_id the route behaves as before: one account.
	w = h.do("GET", "/internal/v1/accounts?user_id="+h.userA, opsToken, "", "")
	if w.Code != http.StatusOK || field(t, w, "account", "id") != h.acct {
		t.Fatalf("by user_id: %d %s", w.Code, w.Body)
	}
}

func TestOpsStats(t *testing.T) {
	h := newOpsHarness(t)
	a := h.credit("c-1", 100, "")
	h.credit("c-2", 200, "")
	h.void("v-1", a)
	if w := h.do("POST", "/internal/v1/accounts/"+h.acct+"/debits", payToken, "d-1", `{"amount":999999,"currency":"IDR"}`); w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("overdraw: %d %s", w.Code, w.Body)
	}

	w := h.do("GET", "/internal/v1/stats?since="+url.QueryEscape(time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)), opsToken, "", "")
	if w.Code != http.StatusOK {
		t.Fatalf("stats: %d %s", w.Code, w.Body)
	}
	counts := map[string]float64{}
	for _, c := range field(t, w, "transactions").([]any) {
		m := c.(map[string]any)
		counts[fmt.Sprintf("%s %s %s", m["type"], m["status"], m["failure_reason"])] = m["count"].(float64)
	}
	want := map[string]float64{"CREDIT SETTLED ": 1, "CREDIT VOIDED ": 1, "VOID SETTLED ": 1, "DEBIT FAILED insufficient_funds": 1}
	for k, n := range want {
		if counts[k] != n {
			t.Fatalf("count %q = %v, want %v (all: %v)", k, counts[k], n, counts)
		}
	}
	if field(t, w, "pending", "count") != float64(0) {
		t.Fatalf("pending = %v, want 0", field(t, w, "pending"))
	}
	// No relay runs in this test, so every event is still in the outbox.
	if n := field(t, w, "outbox", "pending").(float64); n < 10 || field(t, w, "outbox", "oldest_age_ms").(float64) <= 0 {
		t.Fatalf("outbox = %v", field(t, w, "outbox"))
	}
}

func TestOpsRoutesRequireRead(t *testing.T) {
	h := newOpsHarness(t)
	id := h.credit("c-1", 100, "")
	for _, p := range []string{"/internal/v1/accounts", "/internal/v1/transactions", "/internal/v1/transactions/" + id + "/detail",
		"/internal/v1/accounts/" + h.acct + "/events", "/internal/v1/accounts/" + h.acct + "/transactions", "/internal/v1/stats"} {
		if w := h.do("GET", p, freezeOnlyToken, "", ""); w.Code != http.StatusForbidden {
			t.Fatalf("%s with a freeze-only client: %d, want 403", p, w.Code)
		}
		if w := h.do("GET", p, "", "", ""); w.Code != http.StatusUnauthorized {
			t.Fatalf("%s without a token: %d, want 401", p, w.Code)
		}
	}
}
