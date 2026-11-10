//go:build integration

package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"auth-api/internal/kafka"
	"auth-api/internal/logger"
	"auth-api/internal/outbox"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/testutil/kafkatest"
	"auth-api/internal/testutil/pgtest"
)

const trace = "4bf92f3577b34da6a3ce929d0e0e4736"

type env struct {
	t       *testing.T
	pool    *pgxpool.Pool
	brokers []string
	topic   string
	prod    *kafka.Producer
	repo    *repository.OutboxRepo
	logs    *syncWriter
	ctx     context.Context
}

func newEnv(t *testing.T) *env {
	t.Helper()
	brokers, topic := kafkatest.NewTopic(t, 12)
	pool := pgtest.New(t)
	prod, err := kafka.NewProducer(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prod.Close)
	logs := &syncWriter{w: &bytes.Buffer{}}
	ctx := logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "ledger-worker"))
	return &env{t: t, pool: pool, brokers: brokers, topic: topic, prod: prod, repo: repository.NewOutboxRepo(pool), logs: logs, ctx: ctx}
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

// String reads the logs under the lock, while a relay may still be writing.
func (s *syncWriter) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.w.String() }

func (e *env) cfg() outbox.RelayConfig {
	return outbox.RelayConfig{PollInterval: 20 * time.Millisecond, BatchSize: 50, LagEvery: time.Hour, Retention: time.Hour,
		PurgeBatch: 10, MaxBackoff: 50 * time.Millisecond, EscalateAfter: time.Hour, PublishTimeout: 5 * time.Second}
}

func (e *env) relay(store outbox.Store, pub outbox.Publisher, cfg outbox.RelayConfig) *outbox.Relay {
	e.t.Helper()
	r, err := outbox.NewRelay(store, pub, cfg)
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// rawInsert writes a synthetic outbox row (the ledger never writes these by
// hand; tests use them to control ids and commit order).
func (e *env) rawInsert(ctx context.Context, exec func(ctx context.Context, sql string, args ...any) error, key string) string {
	e.t.Helper()
	id := fmt.Sprintf("0192f3c4-0000-7000-8000-%012d", time.Now().UnixNano()%1_000_000_000_000)
	payload := fmt.Sprintf(`{"event_id":%q,"event_type":"Synthetic","stream_id":%q,"stream_version":1}`, id, key)
	if err := exec(ctx, `INSERT INTO ledger.outbox (event_id, topic, msg_key, payload, headers)
		VALUES ($1, $2, $3, $4, '{"event_type":"Synthetic","schema_version":"1"}')`, id, e.topic, key, payload); err != nil {
		e.t.Fatal(err)
	}
	time.Sleep(time.Microsecond)
	return id
}

func (e *env) poolExec(ctx context.Context, sql string, args ...any) error {
	_, err := e.pool.Exec(ctx, sql, args...)
	return err
}

func (e *env) unpublished() int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ledger.outbox WHERE published_at IS NULL`).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

// eventIDs reads want records and returns their envelope event_ids, in
// consumption order, plus the set.
func (e *env) eventIDs(want int) ([]string, map[string]int) {
	e.t.Helper()
	msgs := kafkatest.ReadN(e.t, e.brokers, e.topic, want, 20*time.Second)
	var ids []string
	seen := map[string]int{}
	for _, m := range msgs {
		var env struct {
			EventID string `json:"event_id"`
		}
		if err := json.Unmarshal(m.Value, &env); err != nil {
			e.t.Fatalf("value is not an envelope: %s", m.Value)
		}
		ids = append(ids, env.EventID)
		seen[env.EventID]++
	}
	return ids, seen
}

// Real ledger traffic: every event reaches Kafka once, as its envelope, with
// headers, keyed by account, and the outbox is marked published.
func TestRelayPublishesLedgerEvents(t *testing.T) {
	e := newEnv(t)
	svc, acct := e.ledgerAccount("relay@x.io")
	for i := range 3 {
		if r, err := svc.Credit(e.ctx, service.MoneyCommand{Request: req(fmt.Sprintf("c%d", i)), AccountID: acct, Amount: 10, Currency: "IDR"}); err != nil || r.Status != 201 {
			t.Fatalf("credit: %d %v", r.Status, err)
		}
	}
	var want int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ledger.outbox`).Scan(&want)

	if _, err := e.relay(e.repo, e.prod, e.cfg()).Drain(e.ctx); err != nil {
		t.Fatal(err)
	}
	if n := e.unpublished(); n != 0 {
		t.Fatalf("%d rows unpublished after drain", n)
	}
	msgs := kafkatest.ReadN(t, e.brokers, e.topic, want, 20*time.Second)
	if len(msgs) != want {
		t.Fatalf("consumed %d, want %d", len(msgs), want)
	}
	rows, _ := e.pool.Query(e.ctx, `SELECT event_id::text FROM ledger.outbox`)
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, m := range msgs {
		var env struct {
			EventID, EventType, AccountID, TraceID string
			SchemaVersion                          int `json:"schema_version"`
		}
		json.Unmarshal(m.Value, &struct {
			EventID       *string `json:"event_id"`
			EventType     *string `json:"event_type"`
			AccountID     *string `json:"account_id"`
			TraceID       *string `json:"trace_id"`
			SchemaVersion *int    `json:"schema_version"`
		}{&env.EventID, &env.EventType, &env.AccountID, &env.TraceID, &env.SchemaVersion})
		got[env.EventID] = true
		if m.Key != acct || env.AccountID != acct || env.TraceID != trace || env.SchemaVersion != 1 ||
			m.Headers["event_type"] != env.EventType || !strings.Contains(m.Headers["traceparent"], trace) {
			t.Fatalf("message %s: key=%s headers=%v value=%s", env.EventID, m.Key, m.Headers, m.Value)
		}
	}
	for _, id := range ids {
		if !got[id] {
			t.Fatalf("event %s never published", id)
		}
	}
}

// TRD §13.3 relay crash: the broker acknowledged a batch but marking it
// published failed. The batch is published again; deduplicating on
// event_id gives exactly the outbox.
type failMark struct {
	outbox.Store
	fails atomic.Int32
}

func (f *failMark) MarkPublished(ctx context.Context, ids []int64) error {
	if f.fails.Add(-1) >= 0 {
		return errors.New("connection reset")
	}
	return f.Store.MarkPublished(ctx, ids)
}

func TestRelayCrashRepublishesAndDedupes(t *testing.T) {
	e := newEnv(t)
	var want []string
	for i := range 5 {
		want = append(want, e.rawInsert(e.ctx, e.poolExec, fmt.Sprintf("acct-%d", i%2)))
	}
	store := &failMark{Store: e.repo}
	store.fails.Store(1)
	r := e.relay(store, e.prod, e.cfg())
	if reason, err := r.Drain(e.ctx); err == nil || reason != "pg_error" {
		t.Fatalf("first drain: %q %v", reason, err)
	}
	if n := e.unpublished(); n != 5 {
		t.Fatalf("after the crash %d rows unpublished, want 5", n)
	}
	if _, err := r.Drain(e.ctx); err != nil {
		t.Fatal(err)
	}
	if n := e.unpublished(); n != 0 {
		t.Fatalf("%d unpublished after recovery", n)
	}
	ids, seen := e.eventIDs(10)
	if len(ids) != 10 {
		t.Fatalf("consumed %d, want 10 (5 published twice)", len(ids))
	}
	for _, id := range want {
		if seen[id] != 2 {
			t.Fatalf("event %s delivered %d times, want 2", id, seen[id])
		}
	}
	if len(seen) != 5 {
		t.Fatalf("deduplicated set has %d events, want 5", len(seen))
	}
}

// A row that commits after a higher id was already published is still
// relayed: the relay keeps no cursor (TRD §7.3).
func TestLateCommittingLowerIDIsPublished(t *testing.T) {
	e := newEnv(t)
	late, err := e.pool.Begin(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer late.Rollback(e.ctx)
	lateID := e.rawInsert(e.ctx, func(ctx context.Context, sql string, args ...any) error {
		_, err := late.Exec(ctx, sql, args...)
		return err
	}, "acct-late")
	earlyID := e.rawInsert(e.ctx, e.poolExec, "acct-early")

	var lowID, highID int64
	r := e.relay(e.repo, e.prod, e.cfg())
	if _, err := r.Drain(e.ctx); err != nil {
		t.Fatal(err)
	}
	if err := late.Commit(e.ctx); err != nil {
		t.Fatal(err)
	}
	e.pool.QueryRow(e.ctx, `SELECT id FROM ledger.outbox WHERE event_id = $1`, lateID).Scan(&lowID)
	e.pool.QueryRow(e.ctx, `SELECT id FROM ledger.outbox WHERE event_id = $1`, earlyID).Scan(&highID)
	if lowID >= highID {
		t.Fatalf("setup: late row id %d should be below %d", lowID, highID)
	}
	if n := e.unpublished(); n != 1 {
		t.Fatalf("after commit %d unpublished, want the late row", n)
	}
	if _, err := r.Drain(e.ctx); err != nil {
		t.Fatal(err)
	}
	_, seen := e.eventIDs(2)
	if seen[lateID] != 1 || seen[earlyID] != 1 {
		t.Fatalf("published %v", seen)
	}
}

// Per-ledger ordering under concurrent load, with the relay running: the
// account's TransactionSettled events arrive in ledger_sequence order, and
// every stream's versions arrive in order.
func TestPerLedgerOrderUnderConcurrentLoad(t *testing.T) {
	e := newEnv(t)
	svc, acct := e.ledgerAccount("order@x.io")
	ctx, stop := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { e.relay(e.repo, e.prod, e.cfg()).Run(ctx); close(done) }()

	const n = 40
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r, err := svc.Credit(e.ctx, service.MoneyCommand{Request: req(fmt.Sprintf("load-%d", i)), AccountID: acct, Amount: 1, Currency: "IDR"}); err != nil || r.Status != 201 {
				t.Errorf("credit %d: %d %v", i, r.Status, err)
			}
		}()
	}
	wg.Wait()
	var total int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ledger.outbox`).Scan(&total)
	deadline := time.Now().Add(10 * time.Second)
	for e.unpublished() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	stop()
	<-done

	msgs := kafkatest.ReadN(t, e.brokers, e.topic, total, 20*time.Second)
	if len(msgs) < total {
		t.Fatalf("consumed %d of %d", len(msgs), total)
	}
	var lastSeq int64
	lastVersion := map[string]int64{}
	seen := map[string]bool{}
	for _, m := range msgs {
		var env struct {
			EventID       string `json:"event_id"`
			EventType     string `json:"event_type"`
			StreamID      string `json:"stream_id"`
			StreamVersion int64  `json:"stream_version"`
			Payload       struct {
				LedgerSequence int64 `json:"ledger_sequence"`
			} `json:"payload"`
		}
		if err := json.Unmarshal(m.Value, &env); err != nil {
			t.Fatal(err)
		}
		if seen[env.EventID] { // a duplicate is allowed and ignored
			continue
		}
		seen[env.EventID] = true
		if env.StreamVersion <= lastVersion[env.StreamID] {
			t.Fatalf("stream %s: version %d after %d", env.StreamID, env.StreamVersion, lastVersion[env.StreamID])
		}
		lastVersion[env.StreamID] = env.StreamVersion
		if env.EventType == "TransactionSettled" {
			if env.Payload.LedgerSequence != lastSeq+1 {
				t.Fatalf("ledger_sequence %d after %d", env.Payload.LedgerSequence, lastSeq)
			}
			lastSeq = env.Payload.LedgerSequence
		}
	}
	if lastSeq != n {
		t.Fatalf("last ledger_sequence %d, want %d", lastSeq, n)
	}
}

// Kafka unavailable: writes keep landing in the outbox, the relay retries
// with backoff (outbox.publish warn, then error once failing long enough),
// and the outbox drains when Kafka is back.
type flakyPub struct {
	outbox.Publisher
	down atomic.Bool
	hits atomic.Int32
}

func (f *flakyPub) Publish(ctx context.Context, recs []kafka.Record) error {
	if f.down.Load() {
		f.hits.Add(1)
		return errors.New("dial tcp: connection refused")
	}
	return f.Publisher.Publish(ctx, recs)
}

func TestRelayRetriesWhileKafkaIsDownThenDrains(t *testing.T) {
	e := newEnv(t)
	pub := &flakyPub{Publisher: e.prod}
	pub.down.Store(true)
	cfg := e.cfg()
	cfg.EscalateAfter = 150 * time.Millisecond
	ctx, stop := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { e.relay(e.repo, pub, cfg).Run(ctx); close(done) }()
	defer func() { stop(); <-done }()

	for i := range 3 {
		e.rawInsert(e.ctx, e.poolExec, fmt.Sprintf("acct-%d", i))
	}
	time.Sleep(400 * time.Millisecond)
	if n := e.unpublished(); n != 3 {
		t.Fatalf("published while Kafka was down: %d unpublished", n)
	}
	if h := pub.hits.Load(); h < 3 || h > 20 {
		t.Fatalf("%d publish attempts in 400ms: want backoff (a few, not hundreds)", h)
	}
	logs := e.logs.String()
	for _, want := range []string{`"event":"outbox.publish"`, `"reason":"kafka_unavailable"`, `"level":"warn"`, `"level":"error"`} {
		if !strings.Contains(logs, want) {
			t.Errorf("logs missing %s:\n%s", want, logs)
		}
	}

	pub.down.Store(false)
	deadline := time.Now().Add(5 * time.Second)
	for e.unpublished() > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := e.unpublished(); n != 0 {
		t.Fatalf("%d unpublished after Kafka came back", n)
	}
}

// An unreachable broker makes franz-go retry forever; PublishTimeout turns
// that into a logged, retried failure instead of a silent stall.
type hangingPub struct{ calls atomic.Int32 }

func (h *hangingPub) Publish(ctx context.Context, _ []kafka.Record) error {
	h.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestStalledPublishIsLoggedAndRetried(t *testing.T) {
	e := newEnv(t)
	e.rawInsert(e.ctx, e.poolExec, "acct-stall")
	cfg := e.cfg()
	cfg.PublishTimeout = 100 * time.Millisecond
	pub := &hangingPub{}
	ctx, stop := context.WithTimeout(e.ctx, 600*time.Millisecond)
	defer stop()
	e.relay(e.repo, pub, cfg).Run(ctx)
	if c := pub.calls.Load(); c < 2 {
		t.Fatalf("%d publish attempts: a stalled batch must time out and be retried", c)
	}
	if !strings.Contains(e.logs.String(), `"reason":"kafka_unavailable"`) {
		t.Fatalf("stall not logged:\n%s", e.logs)
	}
	if e.unpublished() != 1 {
		t.Fatal("a timed-out batch was marked published")
	}
}

// stuckPub never returns until released, whatever ctx says: a publish that
// is slow or stuck in the client (spec/Chaos Runbook §6.1).
type stuckPub struct{ release chan struct{} }

func (s *stuckPub) Publish(context.Context, []kafka.Record) error {
	<-s.release
	return errors.New("released")
}

// outbox.lag is the relay's heartbeat, and the lag alert's input: it keeps
// coming, with the growing lag, while a publish is stuck.
func TestLagIsReportedWhilePublishIsStuck(t *testing.T) {
	e := newEnv(t)
	e.rawInsert(e.ctx, e.poolExec, "acct-stuck")
	cfg := e.cfg()
	cfg.LagEvery = 50 * time.Millisecond
	pub := &stuckPub{release: make(chan struct{})}
	ctx, stop := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { e.relay(e.repo, pub, cfg).Run(ctx); close(done) }()
	time.Sleep(500 * time.Millisecond)
	stop()
	close(pub.release)
	<-done
	if n := strings.Count(e.logs.String(), `"event":"outbox.lag"`); n < 5 {
		t.Fatalf("%d outbox.lag logs in 500ms at LagEvery=50ms while publish was stuck:\n%s", n, e.logs)
	}
	if !strings.Contains(e.logs.String(), `"pending":1`) {
		t.Fatalf("lag logs don't show the unpublished row:\n%s", e.logs)
	}
}

// NOTIFY wakes the relay well before its poll interval, including after a
// wait has already timed out once.
func TestNotifyWakesTheRelay(t *testing.T) {
	e := newEnv(t)
	cfg := e.cfg()
	cfg.PollInterval, cfg.MaxBackoff = 2*time.Second, 2*time.Second
	ctx, stop := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { e.relay(e.repo, e.prod, cfg).Run(ctx); close(done) }()
	defer func() { stop(); <-done }()

	time.Sleep(2500 * time.Millisecond) // one empty poll timed out
	e.rawInsert(e.ctx, e.poolExec, "acct-n")
	start := time.Now()
	if _, err := e.pool.Exec(e.ctx, `SELECT pg_notify($1, '')`, outbox.NotifyChannel); err != nil {
		t.Fatal(err)
	}
	for e.unpublished() > 0 && time.Since(start) < 1500*time.Millisecond {
		time.Sleep(10 * time.Millisecond)
	}
	if e.unpublished() != 0 {
		t.Fatalf("not published %v after NOTIFY (poll is 2s)", time.Since(start))
	}
}

func TestLagAndPurge(t *testing.T) {
	e := newEnv(t)
	cfg := e.cfg()
	cfg.LagEvery = time.Millisecond
	for i := range 4 {
		e.rawInsert(e.ctx, e.poolExec, fmt.Sprintf("acct-%d", i))
	}
	// Two old published rows, one recent published, one unpublished.
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.outbox SET published_at = now() - interval '2 hours'
		WHERE id IN (SELECT id FROM ledger.outbox ORDER BY id LIMIT 2)`); err != nil {
		t.Fatal(err)
	}
	e.pool.Exec(e.ctx, `UPDATE ledger.outbox SET published_at = now() WHERE id = (SELECT id FROM ledger.outbox ORDER BY id OFFSET 2 LIMIT 1)`)

	r := e.relay(e.repo, e.prod, cfg)
	if err := r.Purge(e.ctx); err != nil {
		t.Fatal(err)
	}
	var left int
	e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ledger.outbox`).Scan(&left)
	if left != 2 {
		t.Fatalf("%d rows left, want 2 (recent published + unpublished)", left)
	}
	if !strings.Contains(e.logs.String(), `"event":"outbox.purge"`) || !strings.Contains(e.logs.String(), `"deleted":2`) {
		t.Fatalf("purge log:\n%s", e.logs)
	}

	ctx, stop := context.WithTimeout(e.ctx, 150*time.Millisecond)
	defer stop()
	pub := &flakyPub{Publisher: e.prod}
	pub.down.Store(true) // keep the row pending so lag is non-zero
	e.relay(e.repo, pub, cfg).Run(ctx)
	if !strings.Contains(e.logs.String(), `"event":"outbox.lag"`) || !strings.Contains(e.logs.String(), `"pending":1`) {
		t.Fatalf("lag log:\n%s", e.logs)
	}
}

// --- helpers ---

func req(key string) service.Request {
	return service.Request{ClientID: "payments", IdempotencyKey: key, RequestID: "r-" + key, TraceID: trace}
}

func (e *env) ledgerAccount(email string) (*service.LedgerService, string) {
	e.t.Helper()
	svc, err := service.NewLedgerService(repository.NewLedgerRepo(e.pool, e.topic, nil), service.LedgerConfig{
		Currencies: []string{"IDR"}, MaxAmount: 1_000_000_000, ProcessTimeout: 5 * time.Second, IdempotencyTTL: time.Hour})
	if err != nil {
		e.t.Fatal(err)
	}
	var uid string
	if err := e.pool.QueryRow(e.ctx, `INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`, email).Scan(&uid); err != nil {
		e.t.Fatal(err)
	}
	r, err := svc.OpenAccount(e.ctx, service.OpenAccountCommand{Request: service.Request{ClientID: "onboarding",
		IdempotencyKey: "open", RequestID: "r", TraceID: trace}, UserID: uid})
	if err != nil {
		e.t.Fatal(err)
	}
	var body struct {
		Account struct {
			ID string `json:"id"`
		} `json:"account"`
	}
	json.Unmarshal(r.Body, &body)
	return svc, body.Account.ID
}

// Without any NOTIFY, the relay still wakes every PollInterval: it logs
// outbox.lag on schedule and picks up a row whose NOTIFY never came.
func TestRelayPollsWithoutNotify(t *testing.T) {
	e := newEnv(t)
	cfg := e.cfg()
	cfg.PollInterval, cfg.LagEvery = 50*time.Millisecond, 100*time.Millisecond
	ctx, stop := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { e.relay(e.repo, e.prod, cfg).Run(ctx); close(done) }()
	time.Sleep(200 * time.Millisecond)
	e.rawInsert(e.ctx, e.poolExec, "acct-poll") // no pg_notify
	time.Sleep(800 * time.Millisecond)
	stop()
	<-done
	if n := e.unpublished(); n != 0 {
		t.Fatalf("row without NOTIFY not polled: %d unpublished", n)
	}
	if n := strings.Count(e.logs.String(), `"event":"outbox.lag"`); n < 5 {
		t.Fatalf("%d outbox.lag logs in 1s at LagEvery=100ms: the loop is not waking on its own", n)
	}
}
