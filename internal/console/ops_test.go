package console_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

const readTok = "dev-console-token"

var opsNow = time.Date(2026, 11, 11, 7, 15, 0, 0, time.UTC)

const (
	txRecovered = "01a1c4fb-0000-7000-8000-00000000000r"
	txNormal    = "01a1c4fb-0000-7000-8000-00000000000n"
	txRefused   = "01a1c4fb-0000-7000-8000-00000000000f"
	txVoided    = "01a1c4fb-0000-7000-8000-00000000000v"
	txVoid      = "01a1c4fb-0000-7000-8000-0000000000v2"
	traceID     = "4bf92f3577b34da6a3ce929d0e0e4736"
)

// fakeLedger answers ledger-api's ops reads and counts them per path.
type fakeLedger struct {
	mu    sync.Mutex
	hits  map[string]int
	auth  []string
	down  bool
	stats string
	// lastQuery is the query of the last account-transactions read.
	lastQuery string
}

func (f *fakeLedger) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.hits[r.URL.Path]++
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	f.mu.Unlock()
	if f.down {
		http.Error(w, "down", http.StatusBadGateway)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	tx := func(id, status, reason string, created, settled time.Time) string {
		s := fmt.Sprintf(`{"id":%q,"type":"DEBIT","status":%q,"failure_reason":%q,"account_id":"a1","amount":500,"currency":"IDR","requested_by":"payments","trace_id":%q,"created_at":%q,"updated_at":%q`,
			id, status, reason, traceID, created.Format(time.RFC3339Nano), settled.Format(time.RFC3339Nano))
		if status == "SETTLED" {
			s += fmt.Sprintf(`,"settled_at":%q`, settled.Format(time.RFC3339Nano))
		}
		return s + "}"
	}
	c := opsNow.Add(-4 * time.Minute)
	switch {
	case r.URL.Path == "/internal/v1/stats":
		_, _ = w.Write([]byte(f.stats))
	case r.URL.Path == "/internal/v1/transactions":
		_, _ = fmt.Fprintf(w, `{"transactions":[%s,%s,%s]}`,
			tx(txRecovered, "SETTLED", "", c, c.Add(47*time.Second)), tx(txNormal, "SETTLED", "", c, c.Add(3*time.Millisecond)),
			tx(txRefused, "FAILED", "insufficient_funds", c, c))
	case r.URL.Path == "/internal/v1/transactions/"+txRecovered+"/detail":
		_, _ = fmt.Fprintf(w, `{"transaction":%s,"events":[
			{"type":"TransactionRequested","version":1,"payload":{"type":"DEBIT","amount":500,"requested_by":"payments"},"occurred_at":%q},
			{"type":"TransactionProcessing","version":2,"payload":{},"occurred_at":%q},
			{"type":"TransactionSettled","version":3,"payload":{"balance_after":99500},"occurred_at":%q}],
			"journal":[{"ledger_kind":"USER","amount":-500},{"ledger_kind":"SYSTEM","amount":500}],
			"outbox":[{"event_id":"e1","published_at":%q},{"event_id":"e2","published_at":%q},{"event_id":"e3","published_at":%q}]}`,
			tx(txRecovered, "SETTLED", "", c, c.Add(47*time.Second)), c.Format(time.RFC3339), c.Add(47*time.Second).Format(time.RFC3339),
			c.Add(47*time.Second).Format(time.RFC3339), c.Add(48*time.Second).Format(time.RFC3339), c.Add(48*time.Second).Format(time.RFC3339),
			c.Add(48*time.Second).Format(time.RFC3339))
	case r.URL.Path == "/internal/v1/transactions/"+txVoided+"/detail":
		_, _ = fmt.Fprintf(w, `{"transaction":%s,"events":[
			{"type":"TransactionRequested","version":1,"payload":{"requested_by":"payments"},"occurred_at":%q},
			{"type":"TransactionVoided","version":4,"payload":{"void_transaction_id":%q},"occurred_at":%q}],"journal":[],"outbox":[]}`,
			tx(txVoided, "VOIDED", "", c, c), c.Format(time.RFC3339), txVoid, c.Add(time.Minute).Format(time.RFC3339))
	case r.URL.Path == "/internal/v1/accounts/a1/transactions":
		f.mu.Lock()
		f.lastQuery = r.URL.RawQuery
		f.mu.Unlock()
		_, _ = fmt.Fprintf(w, `{"transactions":[%s,%s],"next_cursor":"more"}`,
			tx(txRecovered, "SETTLED", "", c, c.Add(47*time.Second)), tx(txRefused, "FAILED", "insufficient_funds", c, c))
	case r.URL.Path == "/internal/v1/accounts":
		_, _ = w.Write([]byte(`{"accounts":[{"id":"a1","status":"ACTIVE","ledgers":[]}],"next_cursor":"x"}`))
	case r.URL.Path == "/internal/v1/accounts/a1":
		_, _ = w.Write([]byte(`{"account":{"id":"a1","status":"FROZEN"}}`))
	case r.URL.Path == "/internal/v1/accounts/a1/events":
		_, _ = w.Write([]byte(`{"events":[{"type":"AccountFrozen","payload":{"actor":"fraud-engine","rule":"topup_velocity"}}]}`))
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":"transaction_not_found","message":"transaction not found"}}`))
	}
}

func (f *fakeLedger) query() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastQuery
}

func (f *fakeLedger) hit(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.hits[path]
}

const statsBody = `{"since":"2026-11-11T07:05:00Z","transactions":[
	{"type":"CREDIT","status":"SETTLED","failure_reason":"","count":40},
	{"type":"DEBIT","status":"SETTLED","failure_reason":"","count":55},
	{"type":"DEBIT","status":"FAILED","failure_reason":"insufficient_funds","count":4},
	{"type":"CREDIT","status":"VOIDED","failure_reason":"","count":1}],
	"pending":{"count":3,"oldest_age_ms":72000},"outbox":{"pending":2,"oldest_age_ms":30}}`

// fakeLokiServer answers the console's queries with a database outage at
// 07:11:15–07:12:00 (refusals, then writes back), one recovery, a leader
// takeover, a clean reconcile and a fraud freeze.
func fakeLokiServer(t *testing.T, up *atomic.Bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "loki down", http.StatusServiceUnavailable)
			return
		}
		q := r.URL.Query().Get("query")
		pt := func(hms string, v int) string {
			tt, _ := time.Parse("15:04:05", hms)
			ts := time.Date(2026, 11, 11, tt.Hour(), tt.Minute(), tt.Second(), 0, time.UTC)
			return fmt.Sprintf(`[%d,"%d"]`, ts.Unix(), v)
		}
		line := func(hms, body string, labels string) string {
			tt, _ := time.Parse("15:04:05", hms)
			ts := time.Date(2026, 11, 11, tt.Hour(), tt.Minute(), tt.Second(), 0, time.UTC)
			b, _ := json.Marshal(body)
			return fmt.Sprintf(`{"stream":{%s},"values":[["%d",%s]]}`, labels, ts.UnixNano(), b)
		}
		var res string
		switch {
		case strings.Contains(q, "outbox.publish|dependency.call|ledger.recover"):
			res = `{"resultType":"matrix","result":[{"metric":{"event":"ledger.recover","outcome":"success"},"values":[` + pt("07:12:15", 1) + `]}]}`
		case strings.Contains(q, "unwrap lag_ms"):
			res = `{"resultType":"matrix","result":[{"metric":{},"values":[` + pt("07:12:00", 30) + `]}]}`
		case strings.Contains(q, `message="http_request"`):
			res = `{"resultType":"matrix","result":[{"metric":{"status":"503"},"values":[` + pt("07:11:30", 260) + `,` + pt("07:11:45", 300) + `,` +
				pt("07:12:00", 300) + `,` + pt("07:12:15", 236) + `]},{"metric":{"status":"201"},"values":[` + pt("07:11:15", 700) + `,` + pt("07:12:30", 700) + `]}]}`
		case strings.Contains(q, `event="ledger.recover"`):
			// The sweeper's run is what processes and settles the transaction
			// (created 07:11:00, settled 07:11:47), so it comes just before.
			res = `{"resultType":"streams","result":[` + line("07:11:46", `{"event":"ledger.recover","outcome":"success","transaction_id":"`+txRecovered+`","age_ms":47000}`, `"service":"ledger-worker"`) + `]}`
		case strings.Contains(q, `event="app.leader"`):
			res = `{"resultType":"streams","result":[` + line("07:12:02", `{"event":"app.leader","state":"acquired","job":"sweeper"}`, `"service":"ledger-worker","container":"c23-ledger-worker-1"`) + `]}`
		case strings.Contains(q, `event="ledger.reconcile"`):
			res = `{"resultType":"streams","result":[` + line("07:12:40", `{"event":"ledger.reconcile","outcome":"success","mode":"incremental","mismatches":0}`, `"service":"ledger-worker"`) + `]}`
		case strings.Contains(q, `event="fraud.decision"`):
			res = `{"resultType":"streams","result":[]}`
		default:
			t.Errorf("unexpected loki query %s", q)
			res = `{"resultType":"matrix","result":[]}`
		}
		_, _ = w.Write([]byte(`{"status":"success","data":` + res + `}`))
	}))
}

type opsEnv struct {
	h      http.Handler
	ledger *fakeLedger
	lokiUp *atomic.Bool
	now    *time.Time
	logs   *bytes.Buffer
}

func newOpsEnv(t *testing.T) *opsEnv { t.Helper(); return newOpsEnvWith(t, nil) }

func newOpsEnvWith(t *testing.T, edit func(*console.OpsConfig)) *opsEnv {
	t.Helper()
	gin.SetMode(gin.TestMode)
	led := &fakeLedger{hits: map[string]int{}, stats: statsBody}
	ledSrv := httptest.NewServer(led)
	t.Cleanup(ledSrv.Close)
	up := &atomic.Bool{}
	up.Store(true)
	lokiSrv := fakeLokiServer(t, up)
	t.Cleanup(lokiSrv.Close)
	now := opsNow
	logs := &bytes.Buffer{}
	opsCfg := console.OpsConfig{ReadToken: readTok, Loki: console.NewLoki(lokiSrv.URL, time.Second), Now: func() time.Time { return now },
		GrafanaURL: "http://grafana.test:3000"}
	if edit != nil {
		edit(&opsCfg)
	}
	r, err := console.NewRouter(console.Deps{Logger: logger.New(logs, slog.LevelDebug, "ledger-console"), Web: builtApp(),
		Ledger: console.Upstream{Name: "ledger-api", BaseURL: ledSrv.URL},
		Ops:    opsCfg})
	if err != nil {
		t.Fatal(err)
	}
	return &opsEnv{h: r, ledger: led, lokiUp: up, now: &now, logs: logs}
}

func (e *opsEnv) get(t *testing.T, path string) (int, map[string]any) {
	t.Helper()
	w := send(t, e.h, "GET", path, "", "", nil)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

func dig(m any, path ...any) any {
	for _, p := range path {
		switch k := p.(type) {
		case string:
			mm, _ := m.(map[string]any)
			m = mm[k]
		case int:
			s, _ := m.([]any)
			if k >= len(s) {
				return nil
			}
			m = s[k]
		}
	}
	return m
}

func TestMonitorMergesTheLedgerAndTheLogs(t *testing.T) {
	e := newOpsEnv(t)
	code, m := e.get(t, "/console/api/ops/monitor")
	if code != 200 {
		t.Fatalf("monitor: %d %v", code, m)
	}
	if dig(m, "signals", "available") != true {
		t.Fatalf("signals = %v", dig(m, "signals"))
	}
	// Tiles: 100 transactions changed in the window, 1,096 refusals.
	if dig(m, "tiles", "transactions", "count") != float64(100) || dig(m, "tiles", "needed_recovery", "refused") != float64(1096) ||
		dig(m, "tiles", "needed_recovery", "recovered") != float64(1) || dig(m, "tiles", "reconciler", "status") != "clean" ||
		dig(m, "tiles", "outbox", "pending") != float64(2) ||
		dig(m, "tiles", "needed_recovery", "still_pending") != float64(3) || dig(m, "tiles", "needed_recovery", "oldest_pending_ms") != float64(72000) {
		t.Fatalf("tiles = %v", dig(m, "tiles"))
	}
	if r := dig(m, "tiles", "handled_correctly", "ratio").(float64); r > 0.1 { // 1,096 refused of 1,196 attempts
		t.Fatalf("handled_correctly ratio = %v", r)
	}
	// The feed: the recovered transaction is labelled from the logs.
	labels := map[string]string{}
	for _, it := range dig(m, "feed").([]any) {
		labels[dig(it, "transaction", "id").(string)] = dig(it, "label").(string)
	}
	if labels[txRecovered] != "recovered" || labels[txNormal] != "normal" || labels[txRefused] != "rejected" {
		t.Fatalf("feed labels = %v", labels)
	}
	if !strings.HasPrefix(dig(m, "feed", 1, "what").(string), "Settled in 3 ms") {
		t.Fatalf("normal what = %v", dig(m, "feed", 1, "what"))
	}
	// One resolved database incident.
	inc := dig(m, "incidents").([]any)
	if len(inc) != 1 || dig(inc[0], "kind") != "database" || dig(inc[0], "active") != false || dig(inc[0], "count") != float64(1096) {
		t.Fatalf("incidents = %v", inc)
	}
	// Self-healing: the recovery, the takeover and the database coming back.
	var healing []string
	for _, h := range dig(m, "self_healing").([]any) {
		healing = append(healing, dig(h, "text").(string))
	}
	joined := strings.Join(healing, " | ")
	for _, want := range []string{"Recovery job completed transaction …000r, stuck for 47 s", "c23-ledger-worker-1 took leadership of 1 background job", "The database is back"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("self_healing lacks %q: %s", want, joined)
		}
	}
	// Reads use the console's read token only.
	for _, a := range e.ledger.auth {
		if a != "Bearer "+readTok {
			t.Fatalf("ledger-api read with %q", a)
		}
	}
}

// Without Loki the Monitor still shows the ledger's data, says the signals
// are unavailable, and claims no incident or recovery it can't see.
func TestMonitorWithoutLoki(t *testing.T) {
	e := newOpsEnv(t)
	e.lokiUp.Store(false)
	code, m := e.get(t, "/console/api/ops/monitor")
	if code != 200 {
		t.Fatalf("monitor without loki: %d %v", code, m)
	}
	if dig(m, "signals", "available") != false || len(dig(m, "incidents").([]any)) != 0 || len(dig(m, "self_healing").([]any)) != 0 ||
		dig(m, "tiles", "reconciler", "status") != "unknown" || dig(m, "tiles", "transactions", "count") != float64(100) {
		t.Fatalf("monitor without loki = %v", m)
	}
	if lbl := dig(m, "feed", 0, "label"); lbl != "normal" {
		t.Fatalf("without the logs the recovery can't be claimed; label = %v", lbl)
	}
	if !strings.Contains(e.logs.String(), `"dependency":"loki"`) {
		t.Fatalf("loki failure not logged:\n%s", e.logs)
	}
}

func TestMonitorWithoutTheLedgerIs503(t *testing.T) {
	e := newOpsEnv(t)
	e.ledger.down = true
	w := send(t, e.h, "GET", "/console/api/ops/monitor", "", "", nil)
	if w.Code != 502 && w.Code != 503 {
		t.Fatalf("ledger-api failing: %d %s", w.Code, w.Body)
	}
}

// Many open tabs cost one set of upstream calls per 2 s.
func TestOpsResponsesAreCachedFor2s(t *testing.T) {
	e := newOpsEnv(t)
	for range 5 {
		e.get(t, "/console/api/ops/monitor")
	}
	if n := e.ledger.hit("/internal/v1/stats"); n != 1 {
		t.Fatalf("stats read %d times for 5 requests within 2 s, want 1", n)
	}
	*e.now = e.now.Add(2500 * time.Millisecond)
	e.get(t, "/console/api/ops/monitor")
	if n := e.ledger.hit("/internal/v1/stats"); n != 2 {
		t.Fatalf("stats read %d times after the TTL, want 2", n)
	}
}

func TestTransactionTimeline(t *testing.T) {
	e := newOpsEnv(t)
	code, m := e.get(t, "/console/api/ops/transactions/"+txRecovered)
	if code != 200 {
		t.Fatalf("detail: %d %v", code, m)
	}
	if dig(m, "label", "label") != "recovered" {
		t.Fatalf("label = %v", dig(m, "label"))
	}
	var titles []string
	for _, s := range dig(m, "timeline").([]any) {
		titles = append(titles, dig(s, "title").(string)+" ["+dig(s, "source").(string)+"]")
	}
	want := []string{"Request received and saved [ledger]", "Recovery job picked it up [logs]", "Processing started [ledger]",
		"Settled: money moved [ledger]", "Events published to Kafka [ledger]", "Verified by the reconciler [logs]"}
	if strings.Join(titles, " | ") != strings.Join(want, " | ") {
		t.Fatalf("timeline =\n  %s\nwant\n  %s", strings.Join(titles, " | "), strings.Join(want, " | "))
	}
	if n := len(dig(m, "journal").([]any)); n != 2 {
		t.Fatalf("journal lines = %d", n)
	}
	// The trace id links to Grafana Explore, over the transaction's time.
	explore, _ := dig(m, "explore_url").(string)
	u, err := url.Parse(explore)
	if err != nil || !strings.HasPrefix(explore, "http://grafana.test:3000/explore?") {
		t.Fatalf("explore_url = %q", explore)
	}
	panes := u.Query().Get("panes")
	if !strings.Contains(panes, `|= \"`+traceID+`\"`) || !strings.Contains(panes, `"uid":"loki"`) {
		t.Fatalf("explore panes = %s", panes)
	}
	if dig(m, "reversed_by") != nil {
		t.Fatalf("reversed_by on a transaction never voided: %v", dig(m, "reversed_by"))
	}
	if code, _ := e.get(t, "/console/api/ops/transactions/01a1c4fb-0000-7000-8000-000000000000"); code != 404 {
		t.Fatalf("unknown transaction: %d", code)
	}
}

// A voided original names the void that reversed it, from its
// TransactionVoided event, so the screen can link the two.
func TestVoidedTransactionNamesItsVoid(t *testing.T) {
	e := newOpsEnv(t)
	code, m := e.get(t, "/console/api/ops/transactions/"+txVoided)
	if code != 200 || dig(m, "reversed_by") != txVoid || dig(m, "label", "label") != "reversed" {
		t.Fatalf("voided detail: %d reversed_by=%v label=%v", code, dig(m, "reversed_by"), dig(m, "label"))
	}
}

// Without a Grafana URL configured there's no Explore link.
func TestNoExploreLinkWithoutGrafana(t *testing.T) {
	e := newOpsEnvWith(t, func(c *console.OpsConfig) { c.GrafanaURL = "" })
	if code, m := e.get(t, "/console/api/ops/transactions/"+txRecovered); code != 200 || dig(m, "explore_url") != nil {
		t.Fatalf("detail: %d explore_url=%v", code, dig(m, "explore_url"))
	}
}

func TestTransactionsListIsLabelled(t *testing.T) {
	e := newOpsEnv(t)
	code, m := e.get(t, "/console/api/ops/transactions?limit=3&status=SETTLED")
	if code != 200 || dig(m, "transactions", 0, "label") != "recovered" || dig(m, "signals", "available") != true {
		t.Fatalf("list: %d %v", code, m)
	}
}

func TestAccounts(t *testing.T) {
	e := newOpsEnv(t)
	if code, m := e.get(t, "/console/api/ops/accounts?limit=1"); code != 200 || dig(m, "next_cursor") != "x" {
		t.Fatalf("accounts: %d %v", code, m)
	}
	code, m := e.get(t, "/console/api/ops/accounts/a1")
	if code != 200 || dig(m, "account", "status") != "FROZEN" || dig(m, "events", 0, "payload", "rule") != "topup_velocity" {
		t.Fatalf("account: %d %v", code, m)
	}
	// Its transactions, labelled like the feed, and the cursor for more.
	if dig(m, "transactions", 0, "label") != "recovered" || dig(m, "transactions", 1, "label") != "rejected" ||
		dig(m, "transactions", 0, "transaction", "id") != txRecovered || dig(m, "next_cursor") != "more" || dig(m, "signals", "available") != true {
		t.Fatalf("account transactions: %v next=%v", dig(m, "transactions"), dig(m, "next_cursor"))
	}
	if q := e.ledger.query(); q != "limit=20" {
		t.Fatalf("first page query = %q, want limit=20", q)
	}
	if code, _ := e.get(t, "/console/api/ops/accounts/a1?cursor=more"); code != 200 || e.ledger.query() != "cursor=more&limit=20" {
		t.Fatalf("next page: %d query %q", code, e.ledger.query())
	}
}
