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

// OutboxRepo is outbox.Store over money-out-middleware's outbox table, so MOM
// reuses the core's relay unchanged. It is the core's repository.OutboxRepo
// against this database's table (that code stays untouched, P§0 #2).
type OutboxRepo struct{ db *pgxpool.Pool }

func NewOutboxRepo(db *pgxpool.Pool) *OutboxRepo { return &OutboxRepo{db: db} }

// Unpublished returns up to limit unpublished rows in id order, with no
// cursor, so a row that committed late is still returned.
func (r *OutboxRepo) Unpublished(ctx context.Context, limit int) ([]outbox.Pending, error) {
	rows, err := r.db.Query(ctx, `SELECT id, event_id, topic, msg_key, payload::text, headers
		FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT $1`, limit)
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

func (r *OutboxRepo) MarkPublished(ctx context.Context, ids []int64) error {
	if _, err := r.db.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1) AND published_at IS NULL`, ids); err != nil {
		return fmt.Errorf("mark outbox published: %w", err)
	}
	return nil
}

func (r *OutboxRepo) Lag(ctx context.Context) (pending int64, oldest time.Duration, err error) {
	var ms float64
	err = r.db.QueryRow(ctx, `SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)) * 1000, 0)
		FROM outbox WHERE published_at IS NULL`).Scan(&pending, &ms)
	if err != nil {
		return 0, 0, fmt.Errorf("outbox lag: %w", err)
	}
	return pending, time.Duration(ms * float64(time.Millisecond)), nil
}

func (r *OutboxRepo) Purge(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := r.db.Exec(ctx, `WITH oldest AS (SELECT id, published_at FROM outbox ORDER BY id LIMIT $2)
		DELETE FROM outbox o USING oldest WHERE o.id = oldest.id AND oldest.published_at < $1`, cutoff, limit)
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
	return &listener{conn: conn}, nil
}

type listener struct{ conn *pgxpool.Conn }

func (l *listener) Wait(ctx context.Context, d time.Duration) (bool, error) {
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

func (l *listener) Close() {
	_ = l.conn.Conn().Close(context.Background())
	l.conn.Release()
}
