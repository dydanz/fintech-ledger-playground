package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/service"
)

// fakeRoles records SetRole calls and answers as told.
type fakeRoles struct {
	calls    int
	previous string
	err      error
}

func (f *fakeRoles) SetRole(_ context.Context, email, role string) (string, repository.User, error) {
	f.calls++
	if f.err != nil {
		return "", repository.User{}, f.err
	}
	return f.previous, repository.User{ID: "u-1", Email: email, Role: role}, nil
}

func roleCtx() (context.Context, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return logger.WithContext(context.Background(), logger.New(buf, slog.LevelDebug, "auth-api")), buf
}

func oneEvent(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	var found []map[string]any
	for _, l := range lines {
		var m map[string]any
		if json.Unmarshal([]byte(l), &m) == nil && m["event"] == "user.role_change" {
			found = append(found, m)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%d user.role_change events, want 1:\n%s", len(found), buf)
	}
	return found[0]
}

// Granting a role is an authorization change (C1): one event per call, with
// the user id and both roles, never the email.
func TestSetRole(t *testing.T) {
	for _, c := range []struct {
		name     string
		role     string
		store    *fakeRoles
		wantErr  error
		calls    int
		outcome  string
		reason   string
		level    string
		previous string
	}{
		{"grant ops", "ops", &fakeRoles{previous: "user"}, nil, 1, "success", "", "info", "user"},
		{"back to user", "user", &fakeRoles{previous: "ops"}, nil, 1, "success", "", "info", "ops"},
		{"unknown role", "admin", &fakeRoles{}, service.ErrInvalidRole, 0, "denied", "invalid_input", "warn", ""},
		{"unknown email", "ops", &fakeRoles{err: repository.ErrNotFound}, repository.ErrNotFound, 1, "denied", "not_found", "warn", ""},
		{"database down", "ops", &fakeRoles{err: errors.New("conn refused")}, nil, 1, "failure", "pg_error", "error", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, buf := roleCtx()
			u, err := service.NewRoleService(c.store).SetRole(ctx, "ops-person@example.com", c.role)
			switch {
			case c.outcome == "success" && (err != nil || u.Role != c.role):
				t.Fatalf("SetRole = %+v, %v", u, err)
			case c.wantErr != nil && !errors.Is(err, c.wantErr):
				t.Fatalf("err = %v, want %v", err, c.wantErr)
			case c.outcome == "failure" && err == nil:
				t.Fatal("no error when the store failed")
			}
			if c.store.calls != c.calls {
				t.Fatalf("store called %d times, want %d", c.store.calls, c.calls)
			}
			ev := oneEvent(t, buf)
			if ev["outcome"] != c.outcome || ev["level"] != c.level || (c.reason != "" && ev["reason"] != c.reason) {
				t.Fatalf("event = %v", ev)
			}
			if c.outcome == "success" && (ev["user_id"] != "u-1" || ev["role"] != c.role || ev["previous_role"] != c.previous) {
				t.Fatalf("success event = %v", ev)
			}
			if strings.Contains(buf.String(), "ops-person@example.com") {
				t.Fatalf("the email was logged:\n%s", buf)
			}
		})
	}
}
