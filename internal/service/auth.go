// Package service contains the authentication and profile business logic.
package service

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"
	"unicode/utf8"

	"golang.org/x/crypto/bcrypt"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/token"
)

// TokenTTL is the fixed validity window of an access token.
const TokenTTL = time.Hour

const (
	minPasswordLen = 8
	maxPasswordLen = 72 // bcrypt ignores bytes beyond 72
	maxUserAgent   = 512
)

var (
	ErrInvalidCredentials = errors.New("invalid email or password")
	ErrEmailTaken         = repository.ErrEmailTaken
	// ErrWrongPassword is a wrong current password on a password change. The
	// caller's token is valid, so it maps to 403, not 401.
	ErrWrongPassword = errors.New("current password is incorrect")
	// ErrSessionStore wraps a session store failure the caller can retry.
	ErrSessionStore = errors.New("session store unavailable")
)

// ValidationError is returned for bad client input.
type ValidationError struct{ Msg string }

func (e *ValidationError) Error() string { return e.Msg }

type UserStore interface {
	Create(ctx context.Context, email, passwordHash string) (repository.User, error)
	GetByEmail(ctx context.Context, email string) (repository.User, error)
	GetByID(ctx context.Context, id string) (repository.User, error)
	UpdateProfile(ctx context.Context, id string, p repository.ProfileUpdate) (repository.User, error)
	UpdatePasswordHash(ctx context.Context, id, hash string) error
}

type TokenHistory interface {
	Insert(ctx context.Context, rec repository.TokenRecord) error
	Revoke(ctx context.Context, tokenHash string) (bool, error)
	RevokeAllForUser(ctx context.Context, userID string) (int64, error)
}

type SessionStore interface {
	Create(ctx context.Context, tokenHash, userID string, ttl time.Duration) error
	Delete(ctx context.Context, tokenHash string) (bool, error)
	RevokeAll(ctx context.Context, userID string) (int, error)
}

type AuthService struct {
	users      UserStore
	history    TokenHistory
	sessions   SessionStore
	bcryptCost int
	ttl        time.Duration
	dummyHash  []byte
	now        func() time.Time
}

func NewAuthService(users UserStore, history TokenHistory, sessions SessionStore, bcryptCost int) (*AuthService, error) {
	// Compared against when the email is unknown, so a login for a missing
	// account costs the same as one with a wrong password (no user enumeration
	// via timing).
	dummy, err := bcrypt.GenerateFromPassword([]byte("dummy-password-for-timing"), bcryptCost)
	if err != nil {
		return nil, fmt.Errorf("init dummy hash: %w", err)
	}
	return &AuthService{
		users:      users,
		history:    history,
		sessions:   sessions,
		bcryptCost: bcryptCost,
		ttl:        TokenTTL,
		dummyHash:  dummy,
		now:        time.Now,
	}, nil
}

func normalizeEmail(email string) string { return strings.ToLower(strings.TrimSpace(email)) }

func validatePassword(pw string) error {
	if utf8.RuneCountInString(pw) < minPasswordLen {
		return &ValidationError{Msg: fmt.Sprintf("password must be at least %d characters", minPasswordLen)}
	}
	if len(pw) > maxPasswordLen {
		return &ValidationError{Msg: fmt.Sprintf("password must be at most %d bytes", maxPasswordLen)}
	}
	return nil
}

func (s *AuthService) Register(ctx context.Context, email, password string) (repository.User, error) {
	if err := validatePassword(password); err != nil {
		return repository.User{}, err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), s.bcryptCost)
	if err != nil {
		return repository.User{}, fmt.Errorf("hash password: %w", err)
	}
	u, err := s.users.Create(ctx, normalizeEmail(email), string(hash))
	if err != nil {
		return repository.User{}, err
	}
	logger.FromContext(ctx).InfoContext(ctx, "user registered", "user_id", u.ID)
	return u, nil
}

type LoginInput struct {
	Email     string
	Password  string
	ClientIP  string
	UserAgent string
}

type LoginResult struct {
	Token     string
	UserID    string
	ExpiresAt time.Time
	ExpiresIn time.Duration
}

// Login verifies credentials and issues a token. The audit row is written
// before the Redis session, so no usable token can exist without an audit
// record.
func (s *AuthService) Login(ctx context.Context, in LoginInput) (LoginResult, error) {
	log := logger.FromContext(ctx)

	u, err := s.users.GetByEmail(ctx, normalizeEmail(in.Email))
	switch {
	case errors.Is(err, repository.ErrNotFound):
		_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte(in.Password))
		log.WarnContext(ctx, "login failed", "reason", "unknown_email")
		return LoginResult{}, ErrInvalidCredentials
	case err != nil:
		return LoginResult{}, err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(in.Password)); err != nil {
		log.WarnContext(ctx, "login failed", "reason", "bad_password", "user_id", u.ID)
		return LoginResult{}, ErrInvalidCredentials
	}

	raw, err := token.Generate()
	if err != nil {
		return LoginResult{}, err
	}
	hash := token.Hash(raw)
	expiresAt := s.now().Add(s.ttl)

	ip, _ := netip.ParseAddr(in.ClientIP)
	if err := s.history.Insert(ctx, repository.TokenRecord{
		UserID:    u.ID,
		TokenHash: hash,
		LoginIP:   ip.Unmap(),
		UserAgent: truncate(in.UserAgent, maxUserAgent),
		ExpiresAt: expiresAt,
	}); err != nil {
		return LoginResult{}, err
	}

	if err := s.sessions.Create(ctx, hash, u.ID, s.ttl); err != nil {
		// The token was never handed out; mark the audit row revoked so the
		// trail doesn't show a live session that never existed.
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		if _, rerr := s.history.Revoke(cctx, hash); rerr != nil {
			log.ErrorContext(ctx, "failed to revoke orphaned audit record", "error", rerr, "token_hash", hash)
		}
		return LoginResult{}, err
	}

	log.InfoContext(ctx, "login succeeded", "user_id", u.ID)
	return LoginResult{Token: raw, UserID: u.ID, ExpiresAt: expiresAt, ExpiresIn: s.ttl}, nil
}

// Logout invalidates the session in Redis first (that is what makes the
// token unusable), then stamps revoked_at in the audit trail. If the audit
// update fails the token is already dead, so the failure is logged for
// reconciliation rather than surfaced to the client.
func (s *AuthService) Logout(ctx context.Context, tokenHash, userID string) error {
	log := logger.FromContext(ctx)

	if _, err := s.sessions.Delete(ctx, tokenHash); err != nil {
		return err
	}

	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	revoked, err := s.history.Revoke(cctx, tokenHash)
	switch {
	case err != nil:
		log.ErrorContext(ctx, "session deleted but audit revoke failed",
			"error", err, "user_id", userID, "token_hash", tokenHash)
	case !revoked:
		log.WarnContext(ctx, "no open audit record for token", "user_id", userID, "token_hash", tokenHash)
	default:
		log.InfoContext(ctx, "logout succeeded", "user_id", userID)
	}
	return nil
}

// ChangePassword replaces the user's password and ends every session of the
// user, including the one making the request, so the caller logs in again.
//
// Order matters. The new hash is computed first (bcrypt is the slow step),
// then the sessions are ended, then the hash is written. If the sessions
// can't be ended, the password stays as it was: a new password with the old
// sessions still alive would defeat the point. If the hash write fails after
// that, the user is logged out with the old password still valid, which is
// safe. Sessions created before the per-user index existed aren't in it and
// expire on their own within TokenTTL.
//
// It emits one auth.password_change event (C1).
func (s *AuthService) ChangePassword(ctx context.Context, userID, current, next string) error {
	log := logger.FromContext(ctx)
	const event = "auth.password_change"

	u, err := s.users.GetByID(ctx, userID)
	if err != nil {
		log.ErrorContext(ctx, "password change failed", "event", event, "outcome", "failure", "reason", "pg_error", "error", err)
		return err
	}
	if err := bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(current)); err != nil {
		log.WarnContext(ctx, "password change refused", "event", event, "outcome", "denied", "reason", "bad_password")
		return ErrWrongPassword
	}
	if err := validatePassword(next); err != nil {
		log.WarnContext(ctx, "password change refused", "event", event, "outcome", "denied", "reason", "invalid_input")
		return err
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(next), s.bcryptCost)
	if err != nil {
		log.ErrorContext(ctx, "password change failed", "event", event, "outcome", "failure", "reason", "hash_failed", "error", err)
		return fmt.Errorf("hash password: %w", err)
	}

	ended, err := s.sessions.RevokeAll(ctx, userID)
	if err != nil {
		log.ErrorContext(ctx, "password change failed", "event", event, "outcome", "failure", "reason", "session_store_error", "error", err)
		return errors.Join(ErrSessionStore, err)
	}
	if err := s.users.UpdatePasswordHash(ctx, userID, string(hash)); err != nil {
		log.ErrorContext(ctx, "password change failed", "event", event, "outcome", "failure", "reason", "pg_error", "error", err,
			"sessions_ended", ended)
		return err
	}

	cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if _, err := s.history.RevokeAllForUser(cctx, userID); err != nil {
		log.ErrorContext(ctx, "password changed but audit revoke failed", "event", event, "outcome", "partial",
			"reason", "audit_revoke_failed", "error", err, "sessions_ended", ended)
		return nil
	}
	log.InfoContext(ctx, "password changed", "event", event, "outcome", "success", "sessions_ended", ended)
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	s = s[:max]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}
