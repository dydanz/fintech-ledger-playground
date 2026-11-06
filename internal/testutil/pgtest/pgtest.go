// Package pgtest gives integration tests a throwaway PostgreSQL database with
// every migrations/*.up.sql applied, so tests run against the real schema,
// triggers and constraints.
package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EnvURL names the variable holding a connection string for a role that may
// CREATE DATABASE. `make test-integration` points it at the compose Postgres.
const EnvURL = "TEST_DATABASE_URL"

// New creates a fresh database on the server at TEST_DATABASE_URL, applies all
// up migrations in filename order, and drops the database when the test ends.
// The test is skipped when TEST_DATABASE_URL is unset.
func New(t testing.TB) *pgxpool.Pool {
	t.Helper()
	adminURL := os.Getenv(EnvURL)
	if adminURL == "" {
		t.Skipf("%s not set; skipping integration test", EnvURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("pgtest: connect: %v", err)
	}
	name := pgx.Identifier{"test_" + randomHex(8)}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		admin.Close(ctx)
		t.Fatalf("pgtest: create database: %v", err)
	}
	t.Cleanup(func() {
		// t.Context() is already cancelled when cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("pgtest: drop database %s: %v", name, err)
		}
		admin.Close(ctx)
	})

	cfg, err := pgxpool.ParseConfig(adminURL)
	if err != nil {
		t.Fatalf("pgtest: parse %s: %v", EnvURL, err)
	}
	cfg.ConnConfig.Database = name[1 : len(name)-1] // strip the identifier quotes
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("pgtest: connect to test database: %v", err)
	}
	t.Cleanup(pool.Close) // runs before the DROP above (cleanups are LIFO)

	if err := migrate(ctx, pool); err != nil {
		t.Fatalf("pgtest: %v", err)
	}
	return pool
}

// migrate runs each up migration through the simple protocol, which allows
// the multi-statement files (BEGIN ... COMMIT) the repo uses.
func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	root, err := repoRoot()
	if err != nil {
		return err
	}
	files, err := filepath.Glob(filepath.Join(root, "migrations", "*.up.sql"))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return errors.New("no migrations found in " + filepath.Join(root, "migrations"))
	}
	sort.Strings(files)

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			return err
		}
		if _, err := conn.Conn().PgConn().Exec(ctx, string(sql)).ReadAll(); err != nil {
			return errors.New("apply " + filepath.Base(f) + ": " + err.Error())
		}
	}
	return nil
}

// repoRoot walks up from the test's working directory (its package dir) to
// the directory holding go.mod.
func repoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("go.mod not found above the test directory")
		}
		dir = parent
	}
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
