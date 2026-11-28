package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"fintech-ledger-playground/internal/moneyout/breaker"
	"fintech-ledger-playground/internal/moneyout/contract"
)

const breakerColumns = `bank_middleware, rail, state, consecutive, calls, failures, window_start,
	coalesce(opened_at, 'epoch'), probes_in_flight, coalesce(probe_at, 'epoch'), version, updated_at`

func scanBreaker(row pgx.Row) (breaker.Row, error) {
	var r breaker.Row
	var state string
	err := row.Scan(&r.BankMiddleware, &r.Rail, &state, &r.Consecutive, &r.Calls, &r.Failures, &r.WindowStart, &r.OpenedAt,
		&r.ProbesInFlight, &r.ProbeAt, &r.Version, &r.UpdatedAt)
	r.State = breaker.State(state)
	if r.OpenedAt.Unix() == 0 {
		r.OpenedAt = time.Time{}
	}
	if r.ProbeAt.Unix() == 0 {
		r.ProbeAt = time.Time{}
	}
	return r, err
}

// LockBreaker reads a breaker FOR UPDATE, creating it CLOSED the first time.
func (x *Tx) LockBreaker(ctx context.Context, bmw, rail string) (breaker.Row, error) {
	if _, err := x.tx.Exec(ctx, `INSERT INTO breakers (bank_middleware, rail) VALUES ($1, $2) ON CONFLICT DO NOTHING`, bmw, rail); err != nil {
		return breaker.Row{}, fmt.Errorf("create breaker: %w", err)
	}
	r, err := scanBreaker(x.tx.QueryRow(ctx, `SELECT `+breakerColumns+` FROM breakers WHERE bank_middleware = $1 AND rail = $2 FOR UPDATE`, bmw, rail))
	if err != nil {
		return breaker.Row{}, fmt.Errorf("lock breaker: %w", err)
	}
	return r, nil
}

// SaveBreaker writes r, read at version r.Version, and journals change (if
// any) as BreakerChanged at the next version in the same transaction.
func (x *Tx) SaveBreaker(ctx context.Context, r breaker.Row, change *contract.Envelope) error {
	next := r.Version
	if change != nil {
		next++
		if change.StreamVersion != next {
			return ErrStreamVersion
		}
	}
	tag, err := x.tx.Exec(ctx, `UPDATE breakers SET state = $3, consecutive = $4, calls = $5, failures = $6, window_start = $7,
		opened_at = nullif($8, 'epoch'::timestamptz), probes_in_flight = $9, probe_at = nullif($10, 'epoch'::timestamptz), version = $11,
		updated_at = now() WHERE bank_middleware = $1 AND rail = $2 AND version = $12`,
		r.BankMiddleware, r.Rail, string(r.State), r.Consecutive, r.Calls, r.Failures, r.WindowStart, epoch(r.OpenedAt), r.ProbesInFlight,
		epoch(r.ProbeAt), next, r.Version)
	if err != nil {
		return fmt.Errorf("save breaker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionConflict
	}
	if change == nil {
		return nil
	}
	return x.Enqueue(ctx, contract.TopicJournal, *change)
}

func epoch(t time.Time) time.Time {
	if t.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return t
}

// ListBreakers returns every breaker row.
func (s *Store) ListBreakers(ctx context.Context) ([]breaker.Row, error) {
	rows, err := s.db.Query(ctx, `SELECT `+breakerColumns+` FROM breakers ORDER BY bank_middleware, rail`)
	if err != nil {
		return nil, fmt.Errorf("list breakers: %w", err)
	}
	defer rows.Close()
	var out []breaker.Row
	for rows.Next() {
		r, err := scanBreaker(rows)
		if err != nil {
			return nil, fmt.Errorf("list breakers: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SetAttemptProbe marks an attempt as a half-open probe.
func (x *Tx) SetAttemptProbe(ctx context.Context, transferID string, attemptNo int) error {
	if _, err := x.tx.Exec(ctx, `UPDATE attempts SET breaker_probe = true WHERE transfer_id = $1 AND attempt_no = $2`, transferID, attemptNo); err != nil {
		return fmt.Errorf("set probe: %w", err)
	}
	return nil
}
