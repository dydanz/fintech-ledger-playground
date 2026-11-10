package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"fintech-ledger-playground/internal/outbox"
)

// OutboxRepo is the relay's view of ledger.outbox (spec/TRD §7.3).
type OutboxRepo struct {
	db *pgxpool.Pool
}

func NewOutboxRepo(db *pgxpool.Pool) *OutboxRepo { return &OutboxRepo{db: db} }

// Unpublished returns up to limit unpublished rows in id order. It keeps no
// cursor: every call starts from the lowest unpublished id, so a row that
// committed after a higher id was published is still returned.
func (r *OutboxRepo) Unpublished(ctx context.Context, limit int) ([]outbox.Pending, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, event_id, topic, msg_key, payload::text, headers
		   FROM ledger.outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list unpublished outbox: %w", err)
	}
	defer rows.Close()
	var out []outbox.Pending
	for rows.Next() {
		var p outbox.Pending
		var payload string
		var headers []byte
		if err := rows.Scan(&p.ID, &p.EventID, &p.Topic, &p.Key, &payload, &headers); err != nil {
			return nil, fmt.Errorf("list unpublished outbox: %w", err)
		}
		p.Value = []byte(payload)
		if err := json.Unmarshal(headers, &p.Headers); err != nil {
			return nil, fmt.Errorf("outbox %d headers: %w", p.ID, err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// MarkPublished sets published_at on ids, after the broker acknowledged them.
func (r *OutboxRepo) MarkPublished(ctx context.Context, ids []int64) error {
	_, err := r.db.Exec(ctx, `UPDATE ledger.outbox SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`, ids)
	if err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

// Lag reports how many rows are unpublished and the age of the oldest.
func (r *OutboxRepo) Lag(ctx context.Context) (pending int64, oldest time.Duration, err error) {
	var ms float64
	err = r.db.QueryRow(ctx,
		`SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)) * 1000, 0)
		   FROM ledger.outbox WHERE published_at IS NULL`).Scan(&pending, &ms)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox lag: %w", err)
	}
	return pending, time.Duration(ms * float64(time.Millisecond)), nil
}

// Purge deletes, among the limit oldest rows, those published before
// cutoff, and reports how many. Walking the primary key from the oldest
// row keeps it cheap without an index on published_at; the caller stops
// when a batch deletes fewer than limit.
func (r *OutboxRepo) Purge(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := r.db.Exec(ctx,
		`WITH oldest AS (SELECT id, published_at FROM ledger.outbox ORDER BY id LIMIT $2)
		 DELETE FROM ledger.outbox o USING oldest
		  WHERE o.id = oldest.id AND oldest.published_at < $1`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("purge outbox: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Listen holds a connection LISTENing on the relay channel.
func (r *OutboxRepo) Listen(ctx context.Context) (outbox.Listener, error) {
	conn, err := r.db.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("listen: %w", err)
	}
	if _, err := conn.Exec(ctx, `LISTEN `+outbox.NotifyChannel); err != nil {
		conn.Release()
		return nil, fmt.Errorf("listen: %w", err)
	}
	return &pgListener{conn: conn}, nil
}

type pgListener struct{ conn *pgxpool.Conn }

// Wait returns when a notification arrives (true) or after d (false).
func (l *pgListener) Wait(ctx context.Context, d time.Duration) (bool, error) {
	wctx, cancel := context.WithTimeout(ctx, d)
	defer cancel()
	_, err := l.conn.Conn().WaitForNotification(wctx)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
		return false, nil
	}
	return false, fmt.Errorf("wait for notification: %w", err)
}

// Close drops the connection rather than returning a LISTENing one to the pool.
func (l *pgListener) Close() {
	_ = l.conn.Conn().Close(context.Background())
	l.conn.Release()
}
