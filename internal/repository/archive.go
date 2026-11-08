package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"auth-api/internal/archive"
)

// ArchiveRepo is the archiver's access to Postgres (spec/TRD §7.8). Partition
// names come from pg_inherits, never from callers, before being quoted into DDL.
type ArchiveRepo struct{ db *pgxpool.Pool }

func NewArchiveRepo(db *pgxpool.Pool) *ArchiveRepo { return &ArchiveRepo{db: db} }

// Partition is one monthly partition of journal_entries or events.
type Partition struct {
	Table         string    // journal_entries or events
	Name          string    // e.g. journal_entries_y2019m03
	Month         time.Time // lower bound, UTC month start
	DetachPending bool      // an interrupted DETACH ... CONCURRENTLY
	Status        string    // "" (not archived), verified or dropped (ledger.archives)
}

// Partitions lists the attached partitions of both tables whose range ends
// at or before cutoff, oldest first, with any ledger.archives status. A month
// archived as 'verified' whose partition is gone (dropped but not yet
// recorded) is listed too, with an empty Name.
func (r *ArchiveRepo) Partitions(ctx context.Context, cutoff time.Time) ([]Partition, error) {
	rows, err := r.db.Query(ctx, `
		WITH parts AS (
			SELECT parent.relname::text AS tbl, child.relname::text AS name, i.inhdetachpending AS pending,
			       substring(pg_get_expr(child.relpartbound, child.oid) FROM 'FROM \(''([^'']+)''\)')::timestamptz AS lo,
			       substring(pg_get_expr(child.relpartbound, child.oid) FROM 'TO \(''([^'']+)''\)')::timestamptz AS hi
			  FROM pg_inherits i
			  JOIN pg_class parent ON parent.oid = i.inhparent
			  JOIN pg_namespace n ON n.oid = parent.relnamespace AND n.nspname = 'ledger'
			  JOIN pg_class child ON child.oid = i.inhrelid
			 WHERE parent.relname IN ('journal_entries', 'events')
		)
		SELECT p.tbl, p.name, p.lo, p.pending, coalesce(a.status, '')
		  FROM parts p LEFT JOIN ledger.archives a ON a.table_name = p.tbl AND a.month = p.lo
		 WHERE p.hi <= $1
		UNION ALL
		SELECT a.table_name, '', a.month, false, a.status
		  FROM ledger.archives a
		 WHERE a.status = 'verified' AND NOT EXISTS (SELECT 1 FROM parts p WHERE p.tbl = a.table_name AND p.lo = a.month)
		ORDER BY 3, 1 DESC`, cutoff)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Partition, error) {
		var p Partition
		err := row.Scan(&p.Table, &p.Name, &p.Month, &p.DetachPending, &p.Status)
		p.Month = p.Month.UTC()
		return p, err
	})
}

// HeldAccounts returns which of accounts have rows in the partition: journal
// lines on their USER ledgers, or events of theirs.
func (r *ArchiveRepo) HeldAccounts(ctx context.Context, p Partition, accounts []string) ([]string, error) {
	if len(accounts) == 0 {
		return nil, nil
	}
	var sql string
	if p.Table == "journal_entries" {
		sql = `SELECT DISTINCT l.account_id::text FROM ` + qualified(p.Name) + ` j
			JOIN ledger.ledgers l ON l.id = j.ledger_id WHERE l.account_id = ANY($1::uuid[])`
	} else {
		sql = `SELECT DISTINCT e.account_id::text FROM ` + qualified(p.Name) + ` e WHERE e.account_id = ANY($1::uuid[])`
	}
	return listIDs(ctx, r.db, "held accounts", sql, accounts)
}

// WriteCheckpoints records, for every ledger with sequenced lines in the
// journal partition, its balance at the last sequence inside it (§7.8 step 1).
// Idempotent. It returns every checkpoint for the partition.
func (r *ArchiveRepo) WriteCheckpoints(ctx context.Context, p Partition, manifestURI string) ([]archive.Checkpoint, error) {
	if _, err := r.db.Exec(ctx, `
		INSERT INTO ledger.checkpoints (ledger_id, as_of_sequence, balance, archive_uri)
		SELECT DISTINCT ON (ledger_id) ledger_id, ledger_sequence, balance_after, $1
		  FROM `+qualified(p.Name)+` WHERE ledger_sequence IS NOT NULL
		 ORDER BY ledger_id, ledger_sequence DESC
		ON CONFLICT (ledger_id, as_of_sequence) DO NOTHING`, manifestURI); err != nil {
		return nil, fmt.Errorf("write checkpoints: %w", err)
	}
	rows, err := r.db.Query(ctx, `
		SELECT DISTINCT ON (ledger_id) ledger_id::text, ledger_sequence, balance_after
		  FROM `+qualified(p.Name)+` WHERE ledger_sequence IS NOT NULL
		 ORDER BY ledger_id, ledger_sequence DESC`)
	if err != nil {
		return nil, fmt.Errorf("read checkpoints: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (archive.Checkpoint, error) {
		var c archive.Checkpoint
		return c, row.Scan(&c.LedgerID, &c.AsOfSequence, &c.Balance)
	})
}

// CountRows is the partition's row count.
func (r *ArchiveRepo) CountRows(ctx context.Context, p Partition) (int64, error) {
	var n int64
	err := r.db.QueryRow(ctx, `SELECT count(*) FROM `+qualified(p.Name)).Scan(&n)
	return n, wrapRead("count partition", err)
}

// JournalPage returns up to limit journal rows of the partition after id, by id.
func (r *ArchiveRepo) JournalPage(ctx context.Context, p Partition, after string, limit int) ([]archive.JournalRow, error) {
	rows, err := r.db.Query(ctx, `SELECT id::text, transaction_id::text, ledger_id::text, amount, currency,
		ledger_sequence, balance_after, trace_id, created_at FROM `+qualified(p.Name)+`
		WHERE id > $1 ORDER BY id LIMIT $2`, afterID(after), limit)
	if err != nil {
		return nil, fmt.Errorf("read journal partition: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (archive.JournalRow, error) {
		var j archive.JournalRow
		err := row.Scan(&j.ID, &j.TransactionID, &j.LedgerID, &j.Amount, &j.Currency, &j.LedgerSequence,
			&j.BalanceAfter, &j.TraceID, &j.CreatedAt)
		j.CreatedAt = j.CreatedAt.UTC()
		return j, err
	})
}

// EventPage returns up to limit event rows of the partition after id, by id.
func (r *ArchiveRepo) EventPage(ctx context.Context, p Partition, after string, limit int) ([]archive.EventRow, error) {
	rows, err := r.db.Query(ctx, `SELECT event_id::text, stream_type, stream_id::text, stream_version, event_type,
		account_id::text, schema_version, payload::text, trace_id, occurred_at FROM `+qualified(p.Name)+`
		WHERE event_id > $1 ORDER BY event_id LIMIT $2`, afterID(after), limit)
	if err != nil {
		return nil, fmt.Errorf("read event partition: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (archive.EventRow, error) {
		var e archive.EventRow
		var schema int16
		err := row.Scan(&e.EventID, &e.StreamType, &e.StreamID, &e.StreamVersion, &e.EventType, &e.AccountID,
			&schema, &e.Payload, &e.TraceID, &e.OccurredAt)
		e.SchemaVersion, e.OccurredAt = int32(schema), e.OccurredAt.UTC()
		return e, err
	})
}

// MarkVerified records that the partition's export was read back and verified.
func (r *ArchiveRepo) MarkVerified(ctx context.Context, p Partition, manifestURI string, rows int64, checkpoints int) error {
	_, err := r.db.Exec(ctx, `INSERT INTO ledger.archives (table_name, month, status, manifest_uri, rows, checkpoints)
		VALUES ($1, $2, 'verified', $3, $4, $5)
		ON CONFLICT (table_name, month) DO UPDATE SET manifest_uri = EXCLUDED.manifest_uri, rows = EXCLUDED.rows,
			checkpoints = EXCLUDED.checkpoints, verified_at = now()
		WHERE ledger.archives.status = 'verified'`, p.Table, p.Month, manifestURI, rows, checkpoints)
	if err != nil {
		return fmt.Errorf("record archive: %w", err)
	}
	return nil
}

// Detach detaches the partition without blocking writers on the parent
// (DETACH ... CONCURRENTLY, which can't run in a transaction block), or
// finishes an interrupted detach. A no-op when it is already detached.
func (r *ArchiveRepo) Detach(ctx context.Context, p Partition) error {
	if p.Name == "" {
		return nil
	}
	var attached, pending bool
	if err := r.db.QueryRow(ctx, `SELECT count(*) > 0, coalesce(bool_or(i.inhdetachpending), false)
		FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE c.relname = $1 AND i.inhparent = ('ledger.' || $2)::regclass`, p.Name, p.Table).Scan(&attached, &pending); err != nil {
		return fmt.Errorf("detach %s: %w", p.Name, err)
	}
	if !attached {
		return nil
	}
	mode := "CONCURRENTLY"
	if pending {
		mode = "FINALIZE"
	}
	if _, err := r.db.Exec(ctx, `ALTER TABLE `+qualified(p.Table)+` DETACH PARTITION `+qualified(p.Name)+` `+mode); err != nil {
		return fmt.Errorf("detach %s: %w", p.Name, err)
	}
	return nil
}

// Drop drops the detached partition and records the month as dropped, in
// one transaction. It refuses a partition that is still attached.
func (r *ArchiveRepo) Drop(ctx context.Context, p Partition) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("drop %s: %w", p.Name, err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // no-op after Commit
	if p.Name != "" {
		var attached bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
			WHERE c.relname = $1 AND i.inhparent = ('ledger.' || $2)::regclass)`, p.Name, p.Table).Scan(&attached); err != nil {
			return fmt.Errorf("drop %s: %w", p.Name, err)
		}
		if attached {
			return errors.New("drop " + p.Name + ": still attached")
		}
		if _, err := tx.Exec(ctx, `DROP TABLE IF EXISTS `+qualified(p.Name)); err != nil {
			return fmt.Errorf("drop %s: %w", p.Name, err)
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE ledger.archives SET status = 'dropped', dropped_at = now()
		WHERE table_name = $1 AND month = $2 AND status = 'verified'`, p.Table, p.Month)
	if err != nil || tag.RowsAffected() != 1 {
		return fmt.Errorf("drop %s: record: %w (rows %d)", p.Name, err, tag.RowsAffected())
	}
	return tx.Commit(ctx)
}

func qualified(name string) string { return "ledger." + pgx.Identifier{name}.Sanitize() }
