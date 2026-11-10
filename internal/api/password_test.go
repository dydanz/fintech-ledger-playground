package api_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"fintech-ledger-playground/internal/session"
)

const (
	pwOld = "correct horse battery"
	pwNew = "staple-anew-2026"
)

func (h *harness) login(email, pw string) (*httpResult, string) {
	h.t.Helper()
	w := h.do("POST", "/api/v1/auth/login", "", map[string]string{"email": email, "password": pw})
	var res struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &res)
	return &httpResult{code: w.Code, body: w.Body.String()}, res.AccessToken
}

type httpResult struct {
	code int
	body string
}

func (h *harness) changePassword(bearer, current, next string) *httpResult {
	h.t.Helper()
	w := h.do("PUT", "/api/v1/users/password", bearer, map[string]string{"current_password": current, "new_password": next})
	return &httpResult{code: w.Code, body: w.Body.String()}
}

// passwordEvent returns the one auth.password_change log line, failing the
// test unless there is exactly one, and checks no secret is in any line.
func (h *harness) passwordEvent(secrets ...string) map[string]any {
	h.t.Helper()
	var found []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		for _, sec := range secrets {
			if sec != "" && strings.Contains(line, sec) {
				h.t.Fatalf("log line contains a secret:\n%s", line)
			}
		}
		var rec map[string]any
		if json.Unmarshal([]byte(line), &rec) == nil && rec["event"] == "auth.password_change" {
			found = append(found, rec)
		}
	}
	if len(found) != 1 {
		h.t.Fatalf("%d auth.password_change events, want 1:\n%s", len(found), h.logs)
	}
	return found[0]
}

func wantEvent(t *testing.T, rec map[string]any, level, outcome, reason string) {
	t.Helper()
	if rec["level"] != level || rec["outcome"] != outcome || (reason != "" && rec["reason"] != reason) {
		t.Fatalf("event = level %v outcome %v reason %v, want %s %s %s", rec["level"], rec["outcome"], rec["reason"], level, outcome, reason)
	}
	if rec["user_id"] == nil || rec["user_id"] == "" {
		t.Fatalf("event has no user_id: %v", rec)
	}
}

// A password change ends every session of the user, including the one that
// made the request, and revokes their audit rows.
func TestPasswordChangeEndsEverySession(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("pw@example.com", pwOld)
	_, b := h.login("pw@example.com", pwOld)

	if r := h.changePassword(a, pwOld, pwNew); r.code != http.StatusNoContent {
		t.Fatalf("change password: %d %s", r.code, r.body)
	}
	for name, tok := range map[string]string{"requesting": a, "other": b} {
		if w := h.do("GET", "/api/v1/users/profile", tok, nil); w.Code != http.StatusUnauthorized {
			t.Fatalf("%s session still valid after a password change: %d", name, w.Code)
		}
	}
	if r, _ := h.login("pw@example.com", pwOld); r.code != http.StatusUnauthorized {
		t.Fatalf("old password still works: %d", r.code)
	}
	if r, _ := h.login("pw@example.com", pwNew); r.code != http.StatusOK {
		t.Fatalf("new password refused: %d %s", r.code, r.body)
	}
	revoked := 0
	for _, row := range h.history.rows[:2] {
		if row.revokedAt != nil {
			revoked++
		}
	}
	if revoked != 2 {
		t.Fatalf("%d of the 2 earlier audit rows revoked, want 2", revoked)
	}
	rec := h.passwordEvent(pwOld, pwNew, a, b)
	wantEvent(t, rec, "info", "success", "")
	if rec["sessions_ended"] != float64(2) {
		t.Fatalf("sessions_ended = %v, want 2", rec["sessions_ended"])
	}
}

// A wrong current password is refused with 403 (the token itself is valid,
// so not 401) and changes nothing.
func TestPasswordChangeWrongCurrentChangesNothing(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("wrong@example.com", pwOld)

	r := h.changePassword(a, "not the password", pwNew)
	if r.code != http.StatusForbidden || !strings.Contains(r.body, `"invalid_credentials"`) {
		t.Fatalf("wrong current password: %d %s", r.code, r.body)
	}
	if w := h.do("GET", "/api/v1/users/profile", a, nil); w.Code != http.StatusOK {
		t.Fatalf("session ended by a refused change: %d", w.Code)
	}
	if r, _ := h.login("wrong@example.com", pwOld); r.code != http.StatusOK {
		t.Fatalf("old password no longer works: %d", r.code)
	}
	wantEvent(t, h.passwordEvent(pwOld, pwNew, "not the password", a), "warn", "denied", "bad_password")
}

func TestPasswordChangeRejectsInvalidNewPassword(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("short@example.com", pwOld)

	r := h.changePassword(a, pwOld, "short")
	if r.code != http.StatusBadRequest || !strings.Contains(r.body, `"invalid_request"`) {
		t.Fatalf("short new password: %d %s", r.code, r.body)
	}
	if w := h.do("GET", "/api/v1/users/profile", a, nil); w.Code != http.StatusOK {
		t.Fatalf("session ended by a refused change: %d", w.Code)
	}
	wantEvent(t, h.passwordEvent(pwOld, "short", a), "warn", "denied", "invalid_input")
}

func TestPasswordChangeNeedsBothFields(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("fields@example.com", pwOld)
	w := h.do("PUT", "/api/v1/users/password", a, map[string]string{"new_password": pwNew})
	if w.Code != http.StatusBadRequest {
		t.Fatalf("missing current_password: %d %s", w.Code, w.Body)
	}
}

func TestPasswordChangeRequiresAToken(t *testing.T) {
	h := newHarness(t)
	if r := h.changePassword("", pwOld, pwNew); r.code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", r.code)
	}
}

// If the sessions can't be ended, the password isn't changed either: a new
// password with old sessions still alive would defeat the point.
func TestPasswordChangeSessionStoreFailureKeepsThePassword(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("redis@example.com", pwOld)
	var uid string
	for id := range h.users.byID {
		uid = id
	}
	h.mr.Del(session.UserIndexPrefix + uid)
	h.mr.Set(session.UserIndexPrefix+uid, "not-a-set") // SMEMBERS now fails with WRONGTYPE

	if r := h.changePassword(a, pwOld, pwNew); r.code != http.StatusServiceUnavailable {
		t.Fatalf("session store failure: %d %s", r.code, r.body)
	}
	h.mr.Del(session.UserIndexPrefix + uid) // otherwise the next login fails closed on the same key
	if r, _ := h.login("redis@example.com", pwOld); r.code != http.StatusOK {
		t.Fatalf("password changed although the sessions weren't ended: %d", r.code)
	}
	rec := h.passwordEvent(pwOld, pwNew, a)
	wantEvent(t, rec, "error", "failure", "session_store_error")
}

// The sessions are already gone when the hash write fails: the user is
// logged out with the old password still valid, which is safe.
func TestPasswordChangeHashWriteFailure(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("pg@example.com", pwOld)
	h.users.failPwWrite = errors.New("connection reset")

	if r := h.changePassword(a, pwOld, pwNew); r.code != http.StatusInternalServerError {
		t.Fatalf("hash write failure: %d %s", r.code, r.body)
	}
	h.users.failPwWrite = nil
	if r, _ := h.login("pg@example.com", pwOld); r.code != http.StatusOK {
		t.Fatalf("old password should still work: %d", r.code)
	}
	wantEvent(t, h.passwordEvent(pwOld, pwNew, a), "error", "failure", "pg_error")
}

// The password changed and the sessions are gone, but the audit trail
// couldn't be stamped: the change stands (204) and the gap is logged.
func TestPasswordChangeAuditFailureIsPartial(t *testing.T) {
	h := newHarness(t)
	a := h.registerAndLogin("audit@example.com", pwOld)
	h.history.failRevokeAll = errors.New("connection reset")

	if r := h.changePassword(a, pwOld, pwNew); r.code != http.StatusNoContent {
		t.Fatalf("audit failure: %d %s", r.code, r.body)
	}
	if r, _ := h.login("audit@example.com", pwNew); r.code != http.StatusOK {
		t.Fatalf("new password refused: %d", r.code)
	}
	wantEvent(t, h.passwordEvent(pwOld, pwNew, a), "error", "partial", "audit_revoke_failed")
}
