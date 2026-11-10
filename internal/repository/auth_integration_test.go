//go:build integration

package repository_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"testing"
	"time"

	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/testutil/pgtest"
)

func TestUpdatePasswordHash(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	users := repository.NewUserRepo(pool)
	u, err := users.Create(ctx, "hash@example.com", "old-hash")
	if err != nil {
		t.Fatal(err)
	}
	if err := users.UpdatePasswordHash(ctx, u.ID, "new-hash"); err != nil {
		t.Fatal(err)
	}
	got, err := users.GetByID(ctx, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.PasswordHash != "new-hash" || !got.UpdatedAt.After(u.UpdatedAt) {
		t.Fatalf("hash = %q, updated_at %v (was %v)", got.PasswordHash, got.UpdatedAt, u.UpdatedAt)
	}
	if err := users.UpdatePasswordHash(ctx, "00000000-0000-0000-0000-000000000000", "x"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("unknown user: %v, want ErrNotFound", err)
	}
}

// RevokeAllForUser stamps every open, unexpired row of one user, once, and
// leaves expired rows and other users' rows alone. The append-only trigger
// allows exactly this update.
func TestRevokeAllForUser(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()
	users := repository.NewUserRepo(pool)
	history := repository.NewTokenHistoryRepo(pool)
	a, _ := users.Create(ctx, "a@example.com", "h")
	b, _ := users.Create(ctx, "b@example.com", "h")

	ip := netip.MustParseAddr("127.0.0.1")
	// token_hash must be a SHA-256 hex digest (auth_token_history_hash_format).
	hashOf := func(label string) string { sum := sha256.Sum256([]byte(label)); return hex.EncodeToString(sum[:]) }
	label := map[string]string{}
	insert := func(user, name string, expires time.Time) {
		t.Helper()
		label[hashOf(name)] = name
		if err := history.Insert(ctx, repository.TokenRecord{UserID: user, TokenHash: hashOf(name), LoginIP: ip, UserAgent: "t", ExpiresAt: expires}); err != nil {
			t.Fatal(err)
		}
	}
	hour := time.Now().Add(time.Hour)
	insert(a.ID, "a1", hour)
	insert(a.ID, "a2", hour)
	insert(a.ID, "a-expired", time.Now().Add(-time.Minute))
	insert(b.ID, "b1", hour)
	if _, err := history.Revoke(ctx, hashOf("a2")); err != nil { // already revoked by a logout
		t.Fatal(err)
	}

	n, err := history.RevokeAllForUser(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("RevokeAllForUser stamped %d rows, want 1 (a1)", n)
	}
	open := map[string]bool{}
	rows, err := pool.Query(ctx, `SELECT token_hash FROM auth_token_history WHERE revoked_at IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var h string
		_ = rows.Scan(&h)
		open[label[h]] = true
	}
	if len(open) != 2 || !open["a-expired"] || !open["b1"] {
		t.Fatalf("open rows after RevokeAllForUser = %v, want a-expired and b1", open)
	}
	if n, err := history.RevokeAllForUser(ctx, a.ID); err != nil || n != 0 {
		t.Fatalf("second RevokeAllForUser = %d, %v, want 0", n, err)
	}
}
