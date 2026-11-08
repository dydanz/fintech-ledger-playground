//go:build integration

package fraud_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"auth-api/internal/api"
	"auth-api/internal/config"
	"auth-api/internal/fraud"
	"auth-api/internal/kafka"
	"auth-api/internal/logger"
	"auth-api/internal/outbox"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/session"
	"auth-api/internal/testutil/kafkatest"
	"auth-api/internal/testutil/pgtest"
	"auth-api/internal/token"
)

const (
	payTok   = "e2e-payments-token-0123456789"
	opsTok   = "e2e-ops-token-0123456789"
	fraudTok = "e2e-fraud-token-0123456789"
)

// stack is ledger-api (httptest), the outbox relay and Kafka, wired for real.
type stack struct {
	t       *testing.T
	pool    *pgxpool.Pool
	brokers []string
	topic   string
	ledger  *httptest.Server
	rdb     *redis.Client
	logs    *syncBuf
	ctx     context.Context
	stop    context.CancelFunc
}

func newStack(t *testing.T) *stack {
	t.Helper()
	if os.Getenv("TEST_KAFKA_BROKERS") == "" {
		t.Skip("TEST_KAFKA_BROKERS not set")
	}
	brokers, topic := kafkatest.NewTopic(t, 12)
	pool := pgtest.New(t)
	logs := &syncBuf{}
	ctx, stop := context.WithCancel(logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "fraud-e2e")))
	t.Cleanup(stop)

	svc, err := service.NewLedgerService(repository.NewLedgerRepo(pool, topic, nil), service.LedgerConfig{Currencies: []string{"IDR"},
		MaxAmount: 1_000_000_000, ProcessTimeout: 5 * time.Second, IdempotencyTTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.EnsureSystemLedgers(ctx); err != nil {
		t.Fatal(err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	gin.SetMode(gin.TestMode)
	router, err := api.NewLedgerRouter(api.LedgerDeps{Logger: logger.New(logs, slog.LevelDebug, "ledger-api"), Ledger: svc,
		Sessions: session.NewRedisStore(rdb), Ready: func(context.Context) error { return nil },
		Clients: api.NewClients([]config.ClientCredential{
			{ID: "payments", TokenHash: token.Hash(payTok), Operations: []string{"credit", "debit", "read"}},
			{ID: "ops", TokenHash: token.Hash(opsTok), Operations: []string{"account_open", "read"}},
			{ID: "fraud-engine", TokenHash: token.Hash(fraudTok), Operations: []string{"freeze"}},
		})})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(router)
	t.Cleanup(srv.Close)

	prod, err := kafka.NewProducer(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	relay, err := outbox.NewRelay(repository.NewOutboxRepo(pool), prod, outbox.RelayConfig{PollInterval: 50 * time.Millisecond,
		BatchSize: 500, LagEvery: time.Hour, Retention: time.Hour, PurgeBatch: 100, MaxBackoff: time.Second,
		EscalateAfter: time.Minute, PublishTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { relay.Run(ctx); close(done) }()
	t.Cleanup(func() { stop(); <-done })
	return &stack{t: t, pool: pool, brokers: brokers, topic: topic, ledger: srv, rdb: rdb, logs: logs, ctx: ctx, stop: stop}
}

// engine starts a fraud engine in group, freezing through ledgerURL.
func (s *stack) engine(group, ledgerURL string, retry time.Duration) (stop func()) {
	s.t.Helper()
	c, err := kafka.NewConsumer(s.brokers, group, s.topic)
	if err != nil {
		s.t.Fatal(err)
	}
	ev, _ := fraud.NewEvaluator(fraud.NewRedisWindowStore(s.rdb), cfg())
	e, err := fraud.NewEngine(c, fraud.NewRedisSeenStore(s.rdb, time.Hour), ev, fraud.NewHTTPFreezer(ledgerURL, fraudTok, 2*time.Second),
		fraud.EngineConfig{FreezeRetry: retry, MinBackoff: 20 * time.Millisecond, MaxBackoff: 200 * time.Millisecond})
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(s.ctx)
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }()
	return func() { cancel(); <-done; c.Close() }
}

func (s *stack) do(method, path, tok, key, body, traceID string) (int, map[string]any) {
	s.t.Helper()
	req, _ := http.NewRequest(method, s.ledger.URL+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tok)
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if traceID != "" {
		req.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func (s *stack) account(email string) string {
	s.t.Helper()
	var uid string
	if err := s.pool.QueryRow(s.ctx, `INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`, email).Scan(&uid); err != nil {
		s.t.Fatal(err)
	}
	code, body := s.do("POST", "/internal/v1/accounts", opsTok, "open-"+uid, `{"user_id":"`+uid+`"}`, "")
	if code != 201 {
		s.t.Fatalf("open: %d %v", code, body)
	}
	return body["account"].(map[string]any)["id"].(string)
}

func (s *stack) status(acct string) string {
	var st string
	s.pool.QueryRow(s.ctx, `SELECT status FROM ledger.accounts WHERE id = $1`, acct).Scan(&st)
	return st
}

func traceFor(i int) string { return fmt.Sprintf("%032x", 0xabc0000+i) }

// TRD §14 P4 exit: 4 top-ups in 5 minutes → frozen within 2 s p99 of the
// triggering commit, then a debit is account_frozen; one trace_id from the
// triggering credit to the AccountFrozen event.
func TestFourTopupsFreezeWithinTwoSeconds(t *testing.T) {
	s := newStack(t)
	defer s.engine("fraud-e2e-"+s.topic, s.ledger.URL, 5*time.Second)()

	var lat []time.Duration
	for n := range 10 {
		acct := s.account(fmt.Sprintf("fraud-%d@x.io", n))
		var committed time.Time
		for i := range 4 {
			code, body := s.do("POST", "/internal/v1/accounts/"+acct+"/credits", payTok, fmt.Sprintf("c-%d-%d", n, i),
				`{"amount":100,"currency":"IDR"}`, traceFor(n*10+i))
			if code != 201 {
				t.Fatalf("credit: %d %v", code, body)
			}
			committed = time.Now() // the 201 follows the commit
			if i < 3 && s.status(acct) != "ACTIVE" {
				t.Fatalf("frozen after only %d top-ups", i+1)
			}
		}
		deadline := committed.Add(5 * time.Second)
		for s.status(acct) != "FROZEN" && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if s.status(acct) != "FROZEN" {
			t.Fatalf("account %d not frozen 5s after the 4th top-up", n)
		}
		lat = append(lat, time.Since(committed))

		code, body := s.do("POST", "/internal/v1/accounts/"+acct+"/debits", payTok, fmt.Sprintf("d-%d", n), `{"amount":1,"currency":"IDR"}`, "")
		if code != 422 || body["error"].(map[string]any)["code"] != "account_frozen" {
			t.Fatalf("debit after the freeze: %d %v", code, body)
		}
		var traceID, actor, rule string
		if err := s.pool.QueryRow(s.ctx, `SELECT trace_id, payload->>'actor', payload->>'rule' FROM ledger.events
			WHERE stream_id = $1 AND event_type = 'AccountFrozen'`, acct).Scan(&traceID, &actor, &rule); err != nil {
			t.Fatal(err)
		}
		if traceID != traceFor(n*10+3) || actor != "fraud-engine" || rule != "topup_velocity" {
			t.Fatalf("AccountFrozen trace=%s actor=%s rule=%s, want the 4th credit's trace", traceID, actor, rule)
		}
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("commit → freeze latency over %d accounts: p50 %v, max %v", len(lat), lat[len(lat)/2], lat[len(lat)-1])
	if lat[len(lat)-1] > 2*time.Second {
		t.Fatalf("max latency %v > 2s", lat[len(lat)-1])
	}
	if !strings.Contains(s.logs.String(), `"trace_id":"`+traceFor(3)) {
		t.Error("the engine did not log with the triggering trace_id")
	}
}

// ledger-api down: the freeze is retried and reported, and the batch is not
// committed, so a restarted engine re-evaluates the events and freezes.
func TestLedgerDownIsRetriedAndNotCommitted(t *testing.T) {
	s := newStack(t)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // nothing listens here now
	group := "fraud-down-" + s.topic

	acct := s.account("down@x.io")
	for i := range 4 {
		if code, body := s.do("POST", "/internal/v1/accounts/"+acct+"/credits", payTok, fmt.Sprintf("dc-%d", i),
			`{"amount":100,"currency":"IDR"}`, ""); code != 201 {
			t.Fatalf("credit: %d %v", code, body)
		}
	}
	stopDown := s.engine(group, deadURL, 200*time.Millisecond)
	deadline := time.Now().Add(10 * time.Second)
	for !strings.Contains(s.logs.String(), `"reason":"freeze_call_failed"`) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	stopDown()
	if !strings.Contains(s.logs.String(), `"reason":"freeze_call_failed"`) {
		t.Fatalf("no freeze_call_failed:\n%s", s.logs)
	}
	if s.status(acct) != "ACTIVE" {
		t.Fatal("frozen with ledger-api down?")
	}

	// Same group, ledger-api reachable: the uncommitted events come back.
	defer s.engine(group, s.ledger.URL, 5*time.Second)()
	deadline = time.Now().Add(10 * time.Second)
	for s.status(acct) != "FROZEN" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if s.status(acct) != "FROZEN" {
		t.Fatal("not frozen after ledger-api came back: the events were committed while failing")
	}
}
