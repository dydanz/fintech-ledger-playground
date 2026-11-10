//go:build integration

package session_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/session"
)

// The Lua scripts against a real Redis (miniredis runs Lua too, but this
// checks the real server's semantics). Skipped unless TEST_REDIS_ADDR is set.
func TestSessionIndexAgainstRedis(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: os.Getenv("TEST_REDIS_PASSWORD")})
	t.Cleanup(func() { rdb.Close() })
	ctx := context.Background()
	s := session.NewRedisStore(rdb)

	user := "itest-" + time.Now().Format("150405.000000000")
	h1, h2 := user+"-h1", user+"-h2"
	t.Cleanup(func() { rdb.Del(ctx, session.KeyPrefix+h1, session.KeyPrefix+h2, session.UserIndexPrefix+user) })

	for _, h := range []string{h1, h2} {
		if err := s.Create(ctx, h, user, time.Minute); err != nil {
			t.Fatal(err)
		}
	}
	if ttl := rdb.PTTL(ctx, session.UserIndexPrefix+user).Val(); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("index TTL = %v, want (0, 1m]", ttl)
	}
	if removed, err := s.Delete(ctx, h1); err != nil || !removed {
		t.Fatalf("Delete = %v, %v", removed, err)
	}
	if m := rdb.SMembers(ctx, session.UserIndexPrefix+user).Val(); len(m) != 1 || m[0] != h2 {
		t.Fatalf("index after Delete = %v, want [%s]", m, h2)
	}
	n, err := s.RevokeAll(ctx, user)
	if err != nil || n != 1 {
		t.Fatalf("RevokeAll = %d, %v, want 1", n, err)
	}
	if _, err := s.Get(ctx, h2); !errors.Is(err, session.ErrNotFound) {
		t.Fatalf("session alive after RevokeAll: %v", err)
	}
	if rdb.Exists(ctx, session.UserIndexPrefix+user).Val() != 0 {
		t.Fatal("index left behind")
	}
}
