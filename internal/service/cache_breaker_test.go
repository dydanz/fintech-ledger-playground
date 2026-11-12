//go:build integration

package service_test

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"fintech-ledger-playground/internal/idempotency"
	"fintech-ledger-playground/internal/service"
)

// switchCache is a cache that can be taken down; it counts the calls that
// reach it.
type switchCache struct {
	down  atomic.Bool
	calls atomic.Int64
	m     map[string]idempotency.Entry
}

var errCacheDown = fmt.Errorf("dial tcp: connect: %w", syscall.ECONNREFUSED)

func (c *switchCache) Get(_ context.Context, client, key string) (idempotency.Entry, bool, error) {
	c.calls.Add(1)
	if c.down.Load() {
		return idempotency.Entry{}, false, errCacheDown
	}
	e, ok := c.m[client+key]
	return e, ok, nil
}

func (c *switchCache) Set(_ context.Context, client, key string, e idempotency.Entry) error {
	c.calls.Add(1)
	if c.down.Load() {
		return errCacheDown
	}
	c.m[client+key] = e
	return nil
}

func dependencyCalls(t *testing.T, e *svcEnv) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, r := range records(t, e.logs) {
		if r["event"] == "dependency.call" {
			out = append(out, r)
		}
	}
	return out
}

// #60: while Redis is down, writes skip it after the first failure instead
// of waiting out the cache timeout twice each, and the outage is logged once,
// not per write. A probe after CacheRetry finds it back.
func TestCacheBreaker(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	cache := &switchCache{m: map[string]idempotency.Entry{}}
	cfg := testConfig()
	cfg.Cache, cfg.CacheRetry = cache, 300*time.Millisecond
	svc, err := service.NewLedgerService(e.store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.svc = svc
	acct := e.openAccount("breaker@x.io") // uses the cache too
	e.resetLogs()
	cache.calls.Store(0)

	credit := func(key string) {
		t.Helper()
		if r, err := e.money(e.svc.Credit, key, acct, 10, "IDR"); err != nil || r.Status != http.StatusCreated {
			t.Fatalf("credit %s: %+v %v", key, r, err)
		}
	}

	cache.down.Store(true)
	for _, k := range []string{"b1", "b2", "b3", "b4", "b5"} {
		credit(k)
	}
	if n := cache.calls.Load(); n != 1 {
		t.Fatalf("calls to the down cache = %d, want 1 (the failure that opened the breaker)", n)
	}
	logged := dependencyCalls(t, e)
	if len(logged) != 1 || logged[0]["outcome"] != "failure" || logged[0]["operation"] != "idempotency_get" ||
		logged[0]["dependency"] != "redis" || logged[0]["reason"] != "refused" || logged[0]["level"] != "warn" {
		t.Fatalf("want one dependency.call failure, got %v", logged)
	}

	// After CacheRetry one write probes it; still down: no new line.
	time.Sleep(350 * time.Millisecond)
	credit("b6")
	if n := cache.calls.Load(); n != 2 {
		t.Fatalf("calls after the retry interval = %d, want 2 (one probe)", n)
	}
	if n := len(dependencyCalls(t, e)); n != 1 {
		t.Fatalf("a failed probe was logged again (%d lines)", n)
	}

	// Back: the next probe closes the breaker and says so, once.
	cache.down.Store(false)
	time.Sleep(350 * time.Millisecond)
	credit("b7")
	logged = dependencyCalls(t, e)
	if len(logged) != 2 {
		t.Fatalf("want the failure and one recovery line, got %v", logged)
	}
	back := logged[1]
	if back["outcome"] != "success" || back["operation"] != "idempotency_get" || back["level"] != "info" ||
		back["skipped"] != 10.0 || back["down_ms"] == nil {
		t.Fatalf("recovery line = %v, want success with skipped=10 and down_ms", back)
	}
	before := cache.calls.Load()
	credit("b8")
	if n := cache.calls.Load() - before; n != 2 {
		t.Fatalf("calls per write once closed = %d, want 2 (get and set)", n)
	}
	replay, err := e.money(e.svc.Credit, "b8", acct, 10, "IDR")
	if err != nil || !replay.Replayed {
		t.Fatalf("replay of b8: %+v %v", replay, err)
	}
	if n := len(dependencyCalls(t, e)); n != 2 {
		t.Fatalf("lines after recovery = %d, want 2", n)
	}
}
