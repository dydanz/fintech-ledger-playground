//go:build integration

package pgtest_test

import (
	"context"
	"strings"
	"testing"

	"auth-api/internal/testutil/pgtest"
)

func TestNewAppliesMigrations(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()

	for _, table := range []string{"public.users", "public.auth_token_history"} {
		var reg *string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)::text", table).Scan(&reg); err != nil {
			t.Fatal(err)
		}
		if reg == nil {
			t.Errorf("table %s missing after migrations", table)
		}
	}
}

func TestDatabasesAreIsolated(t *testing.T) {
	ctx := context.Background()
	names := map[string]bool{}
	for range 2 {
		var name string
		if err := pgtest.New(t).QueryRow(ctx, "SELECT current_database()").Scan(&name); err != nil {
			t.Fatal(err)
		}
		names[name] = true
	}
	if len(names) != 2 {
		t.Fatalf("expected two distinct databases, got %v", names)
	}
}

// First real-database coverage of migration 001's append-only guard.
func TestTokenHistoryIsAppendOnly(t *testing.T) {
	pool := pgtest.New(t)
	ctx := context.Background()

	var userID string
	if err := pool.QueryRow(ctx,
		`INSERT INTO users (email, password_hash) VALUES ('a@example.com', 'x') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var rowID int64
	if err := pool.QueryRow(ctx,
		`INSERT INTO auth_token_history (user_id, token_hash, expires_at)
		 VALUES ($1, repeat('a', 64), now() + interval '1 hour') RETURNING id`, userID).Scan(&rowID); err != nil {
		t.Fatal(err)
	}

	mustFail := func(name, sql, want string) {
		t.Helper()
		_, err := pool.Exec(ctx, sql, rowID)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want it to contain %q", name, err, want)
		}
	}
	mustFail("delete", `DELETE FROM auth_token_history WHERE id = $1`, "append-only")
	mustFail("update other column", `UPDATE auth_token_history SET user_agent = 'x' WHERE id = $1`, "only allows setting revoked_at")

	if _, err := pool.Exec(ctx, `UPDATE auth_token_history SET revoked_at = now() WHERE id = $1`, rowID); err != nil {
		t.Fatalf("first revoke should succeed: %v", err)
	}
	mustFail("second revoke", `UPDATE auth_token_history SET revoked_at = now() WHERE id = $1`, "already revoked")

	if _, err := pool.Exec(ctx, `TRUNCATE auth_token_history`); err == nil || !strings.Contains(err.Error(), "append-only") {
		t.Errorf("truncate: err = %v, want append-only error", err)
	}
}
