package repository

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TokenRecord is one row of the append-only auth_token_history audit trail.
type TokenRecord struct {
	UserID    string
	TokenHash string
	LoginIP   netip.Addr // zero value is stored as NULL
	UserAgent string
	ExpiresAt time.Time
}

type TokenHistoryRepo struct {
	db *pgxpool.Pool
}

func NewTokenHistoryRepo(db *pgxpool.Pool) *TokenHistoryRepo { return &TokenHistoryRepo{db: db} }

func (r *TokenHistoryRepo) Insert(ctx context.Context, rec TokenRecord) error {
	var ip any
	if rec.LoginIP.IsValid() {
		ip = rec.LoginIP
	}
	_, err := r.db.Exec(ctx,
		`INSERT INTO auth_token_history (user_id, token_hash, login_ip, user_agent, expires_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		rec.UserID, rec.TokenHash, ip, rec.UserAgent, rec.ExpiresAt)
	if err != nil {
		return fmt.Errorf("insert token history: %w", err)
	}
	return nil
}

// Revoke stamps revoked_at on the record for tokenHash. It reports whether
// a not-yet-revoked record was found.
func (r *TokenHistoryRepo) Revoke(ctx context.Context, tokenHash string) (bool, error) {
	tag, err := r.db.Exec(ctx,
		`UPDATE auth_token_history SET revoked_at = now()
		  WHERE token_hash = $1 AND revoked_at IS NULL`, tokenHash)
	if err != nil {
		return false, fmt.Errorf("revoke token history: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}
