//go:build integration

package service_test

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"auth-api/internal/idempotency"
	"auth-api/internal/repository"
	"auth-api/internal/service"
)

// newCachedEnv is newSvcEnv with the Redis idempotency cache on miniredis.
func newCachedEnv(t *testing.T) (*svcEnv, *miniredis.Miniredis, idempotency.Cache) {
	t.Helper()
	e := newSvcEnv(t, 2*time.Second)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { rdb.Close() })
	cache := idempotency.NewRedisCache(rdb)
	cfg := testConfig()
	cfg.Cache = cache
	svc, err := service.NewLedgerService(e.store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = svc
	return e, mr, cache
}

func cacheKey(client, rawKey string) string {
	return idempotency.KeyPrefix + client + ":" + idempotency.HashKey(rawKey)
}

func TestCachedReplayDoesNotTouchPostgres(t *testing.T) {
	e, mr, cache := newCachedEnv(t)
	acct := e.openAccount("cache@x.io")
	first, err := e.money(e.svc.Credit, "cr-cache", acct, 1000, "IDR")
	if err != nil || first.Status != http.StatusCreated {
		t.Fatalf("credit: %d %s %v", first.Status, first.Body, err)
	}
	if !mr.Exists(cacheKey("payments", "cr-cache")) {
		t.Fatal("completed response was not cached")
	}

	// Same cache, a store that fails the test if touched.
	cfg := testConfig()
	cfg.Cache = cache
	offline, _ := service.NewLedgerService(untouchable{t}, cfg)
	e.resetLogs()
	r, err := offline.Credit(e.ctx, service.MoneyCommand{Request: req("payments", "cr-cache"), AccountID: acct, Amount: 1000,
		Currency: "IDR", Reference: "SECRET-REF", Metadata: []byte(`{"channel":"secret-meta"}`)})
	if err != nil || !r.Replayed || r.Status != first.Status || !bytes.Equal(r.Body, first.Body) {
		t.Fatalf("cached replay: %d %s %v", r.Status, r.Body, err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"outcome": "success", "source": "cache"})
}

func TestCacheExpiresWithThePostgresRow(t *testing.T) {
	e, mr, _ := newCachedEnv(t)
	acct := e.openAccount("ttl@x.io")
	if _, err := e.money(e.svc.Credit, "cr-ttl", acct, 10, "IDR"); err != nil {
		t.Fatal(err)
	}
	var left time.Duration
	if err := e.pool.QueryRow(e.ctx, `SELECT expires_at - now() FROM ledger.idempotency_keys WHERE key_hash = $1`,
		idempotency.HashKey("cr-ttl")).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL(cacheKey("payments", "cr-ttl")); ttl <= 0 || ttl > left+time.Second || ttl < left-5*time.Second {
		t.Fatalf("cache TTL %v, Postgres row expires in %v", ttl, left)
	}
}

func TestPostgresReplayBackfillsTheCache(t *testing.T) {
	e, mr, _ := newCachedEnv(t)
	acct := e.openAccount("backfill@x.io")
	first, _ := e.money(e.svc.Debit, "db-nsf", acct, 5000, "IDR") // a stored 422 is replayed like a success
	if first.Status != http.StatusUnprocessableEntity {
		t.Fatalf("debit: %d %s", first.Status, first.Body)
	}
	mr.FlushAll()

	e.resetLogs()
	r, err := e.money(e.svc.Debit, "db-nsf", acct, 5000, "IDR")
	if err != nil || !r.Replayed || !bytes.Equal(r.Body, first.Body) {
		t.Fatalf("pg replay: %d %s %v", r.Status, r.Body, err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"source": "postgres"})
	if !mr.Exists(cacheKey("payments", "db-nsf")) {
		t.Fatal("Postgres replay did not backfill the cache")
	}

	e.resetLogs()
	if r, _ = e.money(e.svc.Debit, "db-nsf", acct, 5000, "IDR"); !bytes.Equal(r.Body, first.Body) {
		t.Fatalf("cache replay body differs: %s", r.Body)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"source": "cache"})
}

func TestCacheMismatchFallsThroughToPostgres(t *testing.T) {
	e, _, _ := newCachedEnv(t)
	acct := e.openAccount("mismatch@x.io")
	if _, err := e.money(e.svc.Credit, "cr-mm", acct, 10, "IDR"); err != nil {
		t.Fatal(err)
	}
	// Same key, different amount: the cached entry's fingerprint doesn't
	// match, so Postgres decides, and it says 422.
	if _, err := e.money(e.svc.Credit, "cr-mm", acct, 11, "IDR"); !errors.Is(err, service.ErrIdempotencyKeyReused) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyReused", err)
	}
}

func TestRedisDownWritesStillCorrect(t *testing.T) {
	e, mr, _ := newCachedEnv(t)
	acct := e.openAccount("down@x.io")
	mr.Close()

	e.resetLogs()
	first, err := e.money(e.svc.Credit, "cr-down", acct, 700, "IDR")
	if err != nil || first.Status != http.StatusCreated {
		t.Fatalf("credit with Redis down: %d %s %v", first.Status, first.Body, err)
	}
	var warns int
	for _, rec := range records(t, e.logs) {
		if rec["event"] == "dependency.call" {
			warns++
			if rec["level"] != "warn" || rec["dependency"] != "redis" || rec["outcome"] != "failure" || rec["reason"] == "" {
				t.Errorf("dependency.call = %v", rec)
			}
		}
	}
	if warns != 2 { // the fast-path get and the post-commit set
		t.Fatalf("want 2 dependency.call warnings, got %d:\n%s", warns, e.logs)
	}
	if strings.Contains(e.logs.String(), "cr-down") {
		t.Fatal("raw Idempotency-Key logged")
	}

	// Retry and key reuse are still decided correctly, by Postgres.
	r, err := e.money(e.svc.Credit, "cr-down", acct, 700, "IDR")
	if err != nil || !r.Replayed || !bytes.Equal(r.Body, first.Body) {
		t.Fatalf("replay with Redis down: %d %s %v", r.Status, r.Body, err)
	}
	if _, err := e.money(e.svc.Credit, "cr-down", acct, 1, "IDR"); !errors.Is(err, service.ErrIdempotencyKeyReused) {
		t.Fatalf("reuse with Redis down: %v", err)
	}
	if n := e.count(`SELECT count(*) FROM ledger.transactions WHERE account_id = $1`, acct); n != 1 {
		t.Fatalf("%d transactions, want 1", n)
	}
}

func TestPendingIsNotCachedUntilProcessCompletes(t *testing.T) {
	e, mr, _ := newCachedEnv(t)
	acct := e.openAccount("pending@x.io")
	e.store.failInTx(2, repository.ErrTimeout) // Tx1 commits, Tx2 fails
	r, err := e.money(e.svc.Credit, "cr-pend", acct, 100, "IDR")
	if err != nil || r.Status != http.StatusAccepted {
		t.Fatalf("want 202: %d %s %v", r.Status, r.Body, err)
	}
	if mr.Exists(cacheKey("payments", "cr-pend")) {
		t.Fatal("a 202 PENDING response was cached")
	}
	var txID string
	if err := e.pool.QueryRow(e.ctx, `SELECT id FROM ledger.transactions WHERE account_id = $1`, acct).Scan(&txID); err != nil {
		t.Fatal(err)
	}
	// What the sweeper will do: Process completes it and caches the result.
	done, err := e.svc.Process(e.ctx, service.ProcessInput{TransactionID: txID, ClientID: "payments",
		KeyHash: idempotency.HashKey("cr-pend"), RequestID: "sweeper"})
	if err != nil || done.Status != http.StatusCreated || !mr.Exists(cacheKey("payments", "cr-pend")) {
		t.Fatalf("process: %d %s %v cached=%v", done.Status, done.Body, err, mr.Exists(cacheKey("payments", "cr-pend")))
	}
}

func TestCachedVoidReplayDoesNotTouchPostgres(t *testing.T) {
	e, _, cache := newCachedEnv(t)
	acct := e.openAccount("cache-void@x.io")
	credit, _ := e.money(e.svc.Credit, "cr-v", acct, 100, "IDR")
	creditID := decode[txBody](t, credit).Transaction.ID
	first, err := e.void("v-cache", creditID, "r")
	if err != nil || first.Status != http.StatusCreated {
		t.Fatalf("void: %d %s %v", first.Status, first.Body, err)
	}

	cfg := testConfig()
	cfg.Cache = cache
	offline, _ := service.NewLedgerService(untouchable{t}, cfg)
	e.resetLogs()
	r, err := offline.Void(e.ctx, service.VoidCommand{Request: req("ops", "v-cache"), TransactionID: creditID, Reason: "r"})
	if err != nil || !r.Replayed || !bytes.Equal(r.Body, first.Body) {
		t.Fatalf("cached void replay: %d %s %v", r.Status, r.Body, err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"operation": "void", "source": "cache"})
}
