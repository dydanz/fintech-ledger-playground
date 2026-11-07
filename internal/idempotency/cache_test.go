package idempotency_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"auth-api/internal/idempotency"
)

func newCache(t *testing.T) (*idempotency.RedisCache, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return idempotency.NewRedisCache(rdb), mr
}

func TestCacheRoundTripIsByteExact(t *testing.T) {
	c, mr := newCache(t)
	ctx := context.Background()
	// Key order, spacing and a non-UTF-8 byte must all survive unchanged.
	body := []byte("{\"z\":1,  \"a\":\"\xff\"}\n")
	e := idempotency.Entry{Fingerprint: "fp", Operation: "credit", Status: 201, Body: body, ExpiresAt: time.Now().Add(time.Hour)}
	if err := c.Set(ctx, "payments", "abc", e); err != nil {
		t.Fatal(err)
	}
	got, ok, err := c.Get(ctx, "payments", "abc")
	if err != nil || !ok || !bytes.Equal(got.Body, body) || got.Status != 201 || got.Fingerprint != "fp" || got.Operation != "credit" {
		t.Fatalf("Get = %+v %v %v", got, ok, err)
	}
	// Expires with the Postgres row, and is scoped per client.
	if ttl := mr.TTL(idempotency.KeyPrefix + "payments:abc"); ttl < 59*time.Minute || ttl > time.Hour {
		t.Fatalf("TTL = %v, want about 1h", ttl)
	}
	if _, ok, _ := c.Get(ctx, "ops", "abc"); ok {
		t.Fatal("another client's entry was returned")
	}
}

func TestCacheMissesAndErrors(t *testing.T) {
	c, mr := newCache(t)
	ctx := context.Background()
	if _, ok, err := c.Get(ctx, "payments", "none"); ok || err != nil {
		t.Fatalf("miss: %v %v", ok, err)
	}
	// An entry already past its expiry is not stored.
	if err := c.Set(ctx, "payments", "old", idempotency.Entry{Status: 201, Body: []byte("{}"), ExpiresAt: time.Now().Add(-time.Second)}); err != nil {
		t.Fatal(err)
	}
	if mr.Exists(idempotency.KeyPrefix + "payments:old") {
		t.Fatal("expired entry stored")
	}
	// Garbage is a miss, not an error: Postgres answers instead.
	mr.Set(idempotency.KeyPrefix+"payments:junk", "not json")
	if _, ok, err := c.Get(ctx, "payments", "junk"); ok || err != nil {
		t.Fatalf("junk: %v %v", ok, err)
	}
	mr.Close()
	if _, _, err := c.Get(ctx, "payments", "none"); err == nil {
		t.Fatal("Get with Redis down returned no error")
	}
	if err := c.Set(ctx, "payments", "x", idempotency.Entry{Status: 201, Body: []byte("{}"), ExpiresAt: time.Now().Add(time.Hour)}); err == nil {
		t.Fatal("Set with Redis down returned no error")
	}
}
