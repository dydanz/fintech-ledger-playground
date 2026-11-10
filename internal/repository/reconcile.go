package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"fintech-ledger-playground/internal/ledger"
)

// The reconciler's reads (spec/TRD §7.6). The List* methods choose what to
// check; the checks themselves run inside Snapshot, so a projection row and
// the source rows it is compared with come from one commit point.

// LedgerBase is where a ledger check starts: its lines after Sequence must
// sum from Balance. Sequence < 0 means "from the latest checkpoint, or 1".
type LedgerBase struct {
	LedgerID string
	Sequence int64
	Balance  int64
}

// LedgerCheck is a USER ledger projection with the aggregates of its journal
// lines after the base (checks 1 and 2).
type LedgerCheck struct {
	LedgerID     string
	BaseSequence int64 // the base actually used (the later of the request and the checkpoint)
	BaseBalance  int64
	Balance      int64 // projection
	LastSequence int64 // projection
	Lines        int64 // lines after the base
	DistinctSeqs int64
	MinSeq       int64 // 0 when Lines is 0
	MaxSeq       int64
	JournalSum   int64
	BadRunning   int64 // lines whose balance_after isn't the running sum
	NullSeq      int64 // lines on this USER ledger without a sequence (any age)
}

// TransactionCheck is a transaction projection with the aggregates of its
// journal lines (check 3). Exists is false for an id with lines but no row.
type TransactionCheck struct {
	ID                    string
	Exists                bool
	AccountID             string
	LedgerID              string
	Type                  ledger.TransactionType
	Status                ledger.Status
	Amount                int64
	Currency              string
	Version               int64
	FailureReason         ledger.FailureReason
	OriginalTransactionID string
	RequestedBy           string
	Lines                 int64
	JournalSum            int64
	Currencies            int64
	WrongCurrency         int64 // lines in another currency than the transaction
	WrongLedger           int64 // lines on neither the transaction's ledger nor its counterparty
	// The archiver (§7.8) has removed the month its lines were posted in, or
	// a month its events may be in: checks 3 and 4 can't see them in Postgres.
	ArchivedLines  bool
	ArchivedEvents bool
}

// AccountCheck is an account projection and its USER ledgers, currency → id.
type AccountCheck struct {
	ID      string
	Exists  bool
	UserID  string
	Status  ledger.AccountStatus
	Version int64
	Ledgers map[string]string
	// ArchivedEvents: some of the account's events may be in an archived month.
	ArchivedEvents bool
}

// StoredEvent is an event row with its payload still encoded, so a payload
// that no longer decodes is reported as drift instead of failing the run.
type StoredEvent struct {
	StreamID      string
	StreamVersion int64
	Type          ledger.EventType
	AccountID     string
	Payload       []byte
	OccurredAt    time.Time
}

// CurrencyTotal is the sum of every journal line in one currency.
type CurrencyTotal struct {
	Currency string
	Total    string // numeric; exact even past int64
}

// ReconcileReader is the read-only view inside a Snapshot.
type ReconcileReader interface {
	CheckLedgers(ctx context.Context, bases []LedgerBase) ([]LedgerCheck, error)
	CheckTransactions(ctx context.Context, ids []string) ([]TransactionCheck, error)
	CheckAccounts(ctx context.Context, ids []string) ([]AccountCheck, error)
	StreamEvents(ctx context.Context, stream ledger.StreamType, ids []string) ([]StoredEvent, error)
}

type ReconcileRepo struct{ db *pgxpool.Pool }

func NewReconcileRepo(db *pgxpool.Pool) *ReconcileRepo { return &ReconcileRepo{db: db} }

const zeroUUID = "00000000-0000-0000-0000-000000000000"

func afterID(after string) string {
	if after == "" {
		return zeroUUID
	}
	return after
}

// ListTransactionIDs pages, by id, through transactions updated after since
// (every transaction when since is zero).
func (r *ReconcileRepo) ListTransactionIDs(ctx context.Context, since time.Time, after string, limit int) ([]string, error) {
	if since.IsZero() {
		return listIDs(ctx, r.db, "list transactions", `SELECT id::text FROM ledger.transactions
			WHERE id > $1 ORDER BY id LIMIT $2`, afterID(after), limit)
	}
	return listIDs(ctx, r.db, "list transactions", `SELECT id::text FROM ledger.transactions
		WHERE updated_at > $3 AND id > $1 ORDER BY id LIMIT $2`, afterID(after), limit, since)
}

// ListAccountIDs pages through accounts updated after since (all when zero).
func (r *ReconcileRepo) ListAccountIDs(ctx context.Context, since time.Time, after string, limit int) ([]string, error) {
	if since.IsZero() {
		return listIDs(ctx, r.db, "list accounts", `SELECT id::text FROM ledger.accounts
			WHERE id > $1 ORDER BY id LIMIT $2`, afterID(after), limit)
	}
	return listIDs(ctx, r.db, "list accounts", `SELECT id::text FROM ledger.accounts
		WHERE updated_at > $3 AND id > $1 ORDER BY id LIMIT $2`, afterID(after), limit, since)
}

// ListUserLedgerIDs pages through every USER ledger.
func (r *ReconcileRepo) ListUserLedgerIDs(ctx context.Context, after string, limit int) ([]string, error) {
	return listIDs(ctx, r.db, "list ledgers", `SELECT id::text FROM ledger.ledgers
		WHERE kind = 'USER' AND id > $1 ORDER BY id LIMIT $2`, afterID(after), limit)
}

// LedgersOfTransactions returns the distinct USER ledgers the transactions post to.
func (r *ReconcileRepo) LedgersOfTransactions(ctx context.Context, ids []string) ([]string, error) {
	return listIDs(ctx, r.db, "list transaction ledgers", `SELECT DISTINCT t.ledger_id::text
		FROM ledger.transactions t JOIN ledger.ledgers l ON l.id = t.ledger_id AND l.kind = 'USER'
		WHERE t.id = ANY($1::uuid[])`, ids)
}

// UnbalancedCurrencies returns every currency whose journal lines don't sum
// to zero (check 3, global). It reads the whole journal.
func (r *ReconcileRepo) UnbalancedCurrencies(ctx context.Context) ([]CurrencyTotal, error) {
	rows, err := r.db.Query(ctx, `SELECT currency, sum(amount)::text FROM ledger.journal_entries
		GROUP BY currency HAVING sum(amount) <> 0 ORDER BY currency`)
	if err != nil {
		return nil, fmt.Errorf("sum journal: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (CurrencyTotal, error) {
		var c CurrencyTotal
		return c, row.Scan(&c.Currency, &c.Total)
	})
}

// OrphanJournalTransactions returns transaction ids that have journal lines
// but no SETTLED or VOIDED projection (up to limit). It reads the whole journal.
func (r *ReconcileRepo) OrphanJournalTransactions(ctx context.Context, limit int) ([]string, error) {
	return listIDs(ctx, r.db, "list orphan lines", `SELECT DISTINCT j.transaction_id::text
		FROM ledger.journal_entries j LEFT JOIN ledger.transactions t ON t.id = j.transaction_id
		WHERE t.id IS NULL OR t.status NOT IN ('SETTLED', 'VOIDED')
		LIMIT $1`, limit)
}

// Now is the database clock, which stamps updated_at.
func (r *ReconcileRepo) Now(ctx context.Context) (time.Time, error) {
	var t time.Time
	if err := r.db.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&t); err != nil {
		return t, fmt.Errorf("read clock: %w", err)
	}
	return t, nil
}

// Snapshot runs fn in one REPEATABLE READ, READ ONLY transaction: every
// read in it sees the same committed state. It takes no row locks.
func (r *ReconcileRepo) Snapshot(ctx context.Context, fn func(ReconcileReader) error) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return fmt.Errorf("begin snapshot: %w", err)
	}
	defer tx.Rollback(context.WithoutCancel(ctx)) //nolint:errcheck // read-only; nothing to keep
	if err := fn(reconcileQueries{db: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type reconcileQueries struct{ db dbtx }

func (q reconcileQueries) CheckLedgers(ctx context.Context, bases []LedgerBase) ([]LedgerCheck, error) {
	ids := make([]string, len(bases))
	seqs := make([]int64, len(bases))
	bals := make([]int64, len(bases))
	for i, b := range bases {
		ids[i], seqs[i], bals[i] = b.LedgerID, b.Sequence, b.Balance
	}
	// The base is the later of the requested one and the latest checkpoint:
	// lines at or before a checkpoint may already be archived (§7.8).
	rows, err := q.db.Query(ctx, `
		WITH req AS (
			SELECT * FROM unnest($1::uuid[], $2::bigint[], $3::bigint[]) AS r(id, from_seq, from_balance)
		), base AS (
			SELECT req.id,
			       CASE WHEN req.from_seq >= coalesce(cp.as_of_sequence, 0) THEN req.from_seq     ELSE coalesce(cp.as_of_sequence, 0) END AS seq,
			       CASE WHEN req.from_seq >= coalesce(cp.as_of_sequence, 0) THEN req.from_balance ELSE coalesce(cp.balance, 0)        END AS balance
			  FROM req
			  LEFT JOIN LATERAL (SELECT c.as_of_sequence, c.balance FROM ledger.checkpoints c
			                      WHERE c.ledger_id = req.id ORDER BY c.as_of_sequence DESC LIMIT 1) cp ON true
		)
		SELECT l.id::text, base.seq, base.balance, l.balance, l.last_sequence,
		       agg.lines, agg.distinct_seqs, agg.min_seq, agg.max_seq, agg.total, agg.bad_running,
		       (SELECT count(*) FROM ledger.journal_entries n WHERE n.ledger_id = l.id AND n.ledger_sequence IS NULL)
		  FROM base
		  JOIN ledger.ledgers l ON l.id = base.id AND l.kind = 'USER'
		 CROSS JOIN LATERAL (
			SELECT count(*) AS lines, count(DISTINCT s.ledger_sequence) AS distinct_seqs,
			       coalesce(min(s.ledger_sequence), 0) AS min_seq, coalesce(max(s.ledger_sequence), 0) AS max_seq,
			       coalesce(sum(s.amount), 0)::bigint AS total,
			       count(*) FILTER (WHERE s.balance_after <> s.running) AS bad_running
			  FROM (SELECT j.ledger_sequence, j.amount, j.balance_after,
			               base.balance + sum(j.amount) OVER (ORDER BY j.ledger_sequence, j.id ROWS UNBOUNDED PRECEDING) AS running
			          FROM ledger.journal_entries j
			         WHERE j.ledger_id = l.id AND j.ledger_sequence > base.seq) s
		 ) agg
		 ORDER BY l.id`, ids, seqs, bals)
	if err != nil {
		return nil, fmt.Errorf("check ledgers: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (LedgerCheck, error) {
		var c LedgerCheck
		return c, row.Scan(&c.LedgerID, &c.BaseSequence, &c.BaseBalance, &c.Balance, &c.LastSequence,
			&c.Lines, &c.DistinctSeqs, &c.MinSeq, &c.MaxSeq, &c.JournalSum, &c.BadRunning, &c.NullSeq)
	})
}

func (q reconcileQueries) CheckTransactions(ctx context.Context, ids []string) ([]TransactionCheck, error) {
	rows, err := q.db.Query(ctx, `
		SELECT ids.id::text, t.id IS NOT NULL,
		       coalesce(t.account_id::text, ''), coalesce(t.ledger_id::text, ''), coalesce(t.type, ''),
		       coalesce(t.status, ''), coalesce(t.amount, 0), coalesce(t.currency, ''), coalesce(t.version, 0),
		       coalesce(t.failure_reason, ''), coalesce(t.original_transaction_id::text, ''), coalesce(t.requested_by, ''),
		       j.lines, j.total, j.currencies, j.wrong_currency, j.wrong_ledger,
		       EXISTS (SELECT 1 FROM ledger.archives a WHERE a.table_name = 'journal_entries'
		                 AND t.settled_at >= a.month AND t.settled_at < a.month + interval '1 month'),
		       coalesce(t.created_at < (SELECT max(a.month) + interval '1 month' FROM ledger.archives a WHERE a.table_name = 'events'), false)
		  FROM unnest($1::uuid[]) AS ids(id)
		  LEFT JOIN ledger.transactions t ON t.id = ids.id
		 CROSS JOIN LATERAL (
			SELECT count(*) AS lines, coalesce(sum(e.amount), 0)::bigint AS total,
			       count(DISTINCT e.currency) AS currencies,
			       count(*) FILTER (WHERE e.currency IS DISTINCT FROM t.currency) AS wrong_currency,
			       count(*) FILTER (WHERE e.ledger_id IS DISTINCT FROM t.ledger_id
			                          AND e.ledger_id IS DISTINCT FROM t.counterparty_ledger_id) AS wrong_ledger
			  FROM ledger.journal_entries e WHERE e.transaction_id = ids.id
		 ) j
		 ORDER BY ids.id`, ids)
	if err != nil {
		return nil, fmt.Errorf("check transactions: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (TransactionCheck, error) {
		var c TransactionCheck
		return c, row.Scan(&c.ID, &c.Exists, &c.AccountID, &c.LedgerID, &c.Type, &c.Status, &c.Amount, &c.Currency,
			&c.Version, &c.FailureReason, &c.OriginalTransactionID, &c.RequestedBy,
			&c.Lines, &c.JournalSum, &c.Currencies, &c.WrongCurrency, &c.WrongLedger, &c.ArchivedLines, &c.ArchivedEvents)
	})
}

func (q reconcileQueries) CheckAccounts(ctx context.Context, ids []string) ([]AccountCheck, error) {
	rows, err := q.db.Query(ctx, `
		SELECT ids.id::text, a.id IS NOT NULL, coalesce(a.user_id::text, ''), coalesce(a.status, ''), coalesce(a.version, 0),
		       coalesce((SELECT array_agg(l.currency::text ORDER BY l.currency) FROM ledger.ledgers l
		                  WHERE l.account_id = ids.id AND l.kind = 'USER'), '{}'),
		       coalesce((SELECT array_agg(l.id::text ORDER BY l.currency) FROM ledger.ledgers l
		                  WHERE l.account_id = ids.id AND l.kind = 'USER'), '{}'),
		       coalesce(a.created_at < (SELECT max(x.month) + interval '1 month' FROM ledger.archives x WHERE x.table_name = 'events'), false)
		  FROM unnest($1::uuid[]) AS ids(id)
		  LEFT JOIN ledger.accounts a ON a.id = ids.id
		 ORDER BY ids.id`, ids)
	if err != nil {
		return nil, fmt.Errorf("check accounts: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (AccountCheck, error) {
		var c AccountCheck
		var currencies, ledgerIDs []string
		if err := row.Scan(&c.ID, &c.Exists, &c.UserID, &c.Status, &c.Version, &currencies, &ledgerIDs, &c.ArchivedEvents); err != nil {
			return c, err
		}
		c.Ledgers = make(map[string]string, len(currencies))
		for i, cur := range currencies {
			c.Ledgers[cur] = ledgerIDs[i]
		}
		return c, nil
	})
}

func (q reconcileQueries) StreamEvents(ctx context.Context, stream ledger.StreamType, ids []string) ([]StoredEvent, error) {
	rows, err := q.db.Query(ctx, `
		SELECT stream_id::text, stream_version, event_type, account_id::text, payload, occurred_at
		  FROM ledger.events
		 WHERE stream_type = $1 AND stream_id = ANY($2::uuid[])
		 ORDER BY stream_id, stream_version`, stream, ids)
	if err != nil {
		return nil, fmt.Errorf("list stream events: %w", err)
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (StoredEvent, error) {
		var e StoredEvent
		return e, row.Scan(&e.StreamID, &e.StreamVersion, &e.Type, &e.AccountID, &e.Payload, &e.OccurredAt)
	})
}

func listIDs(ctx context.Context, db dbtx, op, sql string, args ...any) ([]string, error) {
	rows, err := db.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return ids, nil
}
