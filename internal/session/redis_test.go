package session_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/session"
)

func newStore(t *testing.T) (*session.RedisStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return session.NewRedisStore(rdb), mr
}

// Every session is indexed under its user, so all of a user's sessions can
// be ended at once; the index expires with the newest session.
func TestCreateIndexesTheSessionUnderItsUser(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	if err := s.Create(ctx, "h1", "user-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, "h2", "user-1", time.Hour); err != nil {
		t.Fatal(err)
	}
	members, err := mr.Members(session.UserIndexPrefix + "user-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) != 2 {
		t.Fatalf("index members = %v, want h1 and h2", members)
	}
	if ttl := mr.TTL(session.UserIndexPrefix + "user-1"); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("index TTL = %v, want (0, 1h]", ttl)
	}
}

func TestDeleteRemovesTheSessionFromItsUserIndex(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	_ = s.Create(ctx, "h1", "user-1", time.Hour)
	_ = s.Create(ctx, "h2", "user-1", time.Hour)

	removed, err := s.Delete(ctx, "h1")
	if err != nil || !removed {
		t.Fatalf("Delete = %v, %v", removed, err)
	}
	members, _ := mr.Members(session.UserIndexPrefix + "user-1")
	if len(members) != 1 || members[0] != "h2" {
		t.Fatalf("index after delete = %v, want [h2]", members)
	}
	if removed, _ := s.Delete(ctx, "h1"); removed {
		t.Fatal("second Delete reported a removal")
	}
}

// RevokeAll ends every session of one user, and no one else's.
func TestRevokeAllEndsEverySessionOfOneUser(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	_ = s.Create(ctx, "h1", "user-1", time.Hour)
	_ = s.Create(ctx, "h2", "user-1", time.Hour)
	_ = s.Create(ctx, "h3", "user-2", time.Hour)

	n, err := s.RevokeAll(ctx, "user-1")
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("RevokeAll ended %d sessions, want 2", n)
	}
	for _, h := range []string{"h1", "h2"} {
		if _, err := s.Get(ctx, h); !errors.Is(err, session.ErrNotFound) {
			t.Fatalf("session %s still valid after RevokeAll: %v", h, err)
		}
	}
	if mr.Exists(session.UserIndexPrefix + "user-1") {
		t.Fatal("user-1 index left behind")
	}
	if _, err := s.Get(ctx, "h3"); err != nil {
		t.Fatalf("another user's session was ended: %v", err)
	}
	if n, err := s.RevokeAll(ctx, "user-1"); err != nil || n != 0 {
		t.Fatalf("RevokeAll with nothing left = %d, %v", n, err)
	}
}

// A session that already expired leaves a stale member; RevokeAll still
// succeeds and counts only the sessions it ended.
func TestRevokeAllSkipsExpiredMembers(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	_ = s.Create(ctx, "h1", "user-1", time.Minute)
	_ = s.Create(ctx, "h2", "user-1", time.Hour)
	mr.FastForward(2 * time.Minute) // h1 expires; the index lives on

	n, err := s.RevokeAll(ctx, "user-1")
	if err != nil || n != 1 {
		t.Fatalf("RevokeAll = %d, %v, want 1", n, err)
	}
}

// If the index can't be written, the session must not exist either:
// otherwise a password change could miss it.
func TestCreateFailsClosedWhenTheIndexIsNotWritten(t *testing.T) {
	s, mr := newStore(t)
	ctx := context.Background()
	mr.Set(session.UserIndexPrefix+"user-1", "not-a-set") // SADD now fails with WRONGTYPE

	if err := s.Create(ctx, "h1", "user-1", time.Hour); err == nil {
		t.Fatal("Create succeeded although the index write failed")
	}
	if mr.Exists(session.KeyPrefix + "h1") {
		t.Fatal("session key left behind without an index entry")
	}
}
