//go:build integration

// Integration tests for migrations/002_ledger.up.sql: the database-level
// guarantees in spec/TRD §5.1. They run against a throwaway database per test.
package migrations_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"auth-api/internal/testutil/pgtest"
)

const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"

type fixture struct {
	t         *testing.T
	pool      *pgxpool.Pool
	ctx       context.Context
	userID    string
	accountID string
	ledgerID  string // USER ledger, IDR
	topupID   string // SYSTEM topup_clearing, IDR
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{t: t, pool: pgtest.New(t), ctx: context.Background()}
	f.userID = f.scan(`INSERT INTO users (email, password_hash) VALUES ('a@example.com', 'x') RETURNING id`)
	f.accountID = f.scan(`INSERT INTO ledger.accounts (id, user_id) VALUES (gen_random_uuid(), $1) RETURNING id`, f.userID)
	f.ledgerID = f.scan(`INSERT INTO ledger.ledgers (id, kind, account_id, currency)
		VALUES (gen_random_uuid(), 'USER', $1, 'IDR') RETURNING id`, f.accountID)
	f.topupID = f.scan(`SELECT id FROM ledger.ledgers WHERE kind = 'SYSTEM' AND code = 'topup_clearing' AND currency = 'IDR'`)
	return f
}

func (f *fixture) scan(sql string, args ...any) string {
	f.t.Helper()
	var s string
	if err := f.pool.QueryRow(f.ctx, sql, args...).Scan(&s); err != nil {
		f.t.Fatalf("%s: %v", strings.Fields(sql)[0], err)
	}
	return s
}

func (f *fixture) exec(sql string, args ...any) error {
	_, err := f.pool.Exec(f.ctx, sql, args...)
	return err
}

func (f *fixture) count(sql string, args ...any) int {
	f.t.Helper()
	var n int
	if err := f.pool.QueryRow(f.ctx, sql, args...).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// postCredit writes a balanced two-line credit of amount into f.ledgerID
// in one statement, as JournalRepo.Post will.
func (f *fixture) postCredit(amount int64) error {
	return f.exec(`INSERT INTO ledger.journal_entries
		(id, transaction_id, ledger_id, amount, currency, ledger_sequence, balance_after, trace_id)
		SELECT gen_random_uuid(), tx, l, a, 'IDR', s, b, $4
		FROM (SELECT gen_random_uuid() AS tx) t,
		     (VALUES ($1::uuid, $3::bigint, 1::bigint, $3::bigint), ($2::uuid, -$3::bigint, NULL, NULL)) v(l, a, s, b)`,
		f.ledgerID, f.topupID, amount, traceID)
}

func (f *fixture) appendEvent() error {
	return f.exec(`INSERT INTO ledger.events
		(event_id, stream_type, stream_id, stream_version, event_type, account_id, payload, trace_id)
		VALUES (gen_random_uuid(), 'account', $1, 1, 'AccountOpened', $1, '{}', $2)`, f.accountID, traceID)
}

func wantErr(t *testing.T, name string, err error, contains string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), contains) {
		t.Errorf("%s: err = %v, want it to contain %q", name, err, contains)
	}
}

func monthPartition(parent string, m time.Time) string {
	return fmt.Sprintf("%s_y%04dm%02d", parent, m.Year(), m.Month())
}

func TestInitialPartitionsCoverCurrentMonthPlusThree(t *testing.T) {
	f := newFixture(t)
	now := time.Now().UTC()
	start := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	for i := range 4 {
		m := start.AddDate(0, i, 0)
		for _, parent := range []string{"journal_entries", "events"} {
			part := monthPartition(parent, m)
			if n := f.count(`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE n.nspname = 'ledger' AND c.relname = $1`, part); n != 1 {
				t.Errorf("partition ledger.%s missing", part)
			}
			if n := f.count(`SELECT count(*) FROM pg_trigger WHERE tgrelid = ('ledger.' || $1)::regclass
				AND tgname = $1 || '_guard_trunc'`, part); n != 1 {
				t.Errorf("partition ledger.%s has no TRUNCATE guard", part)
			}
		}
	}

	if n := f.count(`SELECT ledger.ensure_partitions((now() AT TIME ZONE 'UTC')::date, 4)`); n != 0 {
		t.Errorf("ensure_partitions is not idempotent: created %d on second call", n)
	}
	if n := f.count(`SELECT ledger.ensure_partitions((now() AT TIME ZONE 'UTC')::date, 6)`); n != 4 {
		t.Errorf("extending by 2 months created %d partitions, want 4", n)
	}
}

func TestInsertOutsidePartitionsFails(t *testing.T) {
	f := newFixture(t)
	err := f.exec(`INSERT INTO ledger.events
		(event_id, stream_type, stream_id, stream_version, event_type, account_id, payload, trace_id, occurred_at)
		VALUES (gen_random_uuid(), 'account', $1, 1, 'AccountOpened', $1, '{}', $2, now() + interval '2 years')`,
		f.accountID, traceID)
	wantErr(t, "insert 2 years ahead", err, "no partition")
}

func TestSourceOfTruthIsAppendOnly(t *testing.T) {
	f := newFixture(t)
	if err := f.postCredit(100); err != nil {
		t.Fatal(err)
	}
	if err := f.appendEvent(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()

	for _, tbl := range []string{"journal_entries", "events"} {
		part := "ledger." + monthPartition(tbl, now)
		parent := "ledger." + tbl
		before := f.count("SELECT count(*) FROM " + parent)

		wantErr(t, "UPDATE "+parent, f.exec("UPDATE "+parent+" SET trace_id = trace_id"), "append-only")
		wantErr(t, "DELETE "+parent, f.exec("DELETE FROM "+parent), "append-only")
		wantErr(t, "UPDATE "+part, f.exec("UPDATE "+part+" SET trace_id = trace_id"), "append-only")
		wantErr(t, "DELETE "+part, f.exec("DELETE FROM "+part), "append-only")
		wantErr(t, "TRUNCATE "+parent, f.exec("TRUNCATE "+parent), "append-only")
		// A TRUNCATE aimed at the partition bypasses the parent's statement trigger;
		// ensure_partitions must have added one to the partition itself.
		wantErr(t, "TRUNCATE "+part, f.exec("TRUNCATE "+part), "append-only")

		if after := f.count("SELECT count(*) FROM " + parent); after != before {
			t.Errorf("%s row count changed from %d to %d", parent, before, after)
		}
	}
}

func TestJournalMustBalanceAtCommit(t *testing.T) {
	f := newFixture(t)
	usd := f.scan(`INSERT INTO ledger.ledgers (id, kind, code, currency)
		VALUES (gen_random_uuid(), 'SYSTEM', 'topup_clearing', 'USD') RETURNING id`)

	// line inserts one journal line for transaction tx inside an explicit transaction.
	line := func(tx pgx.Tx, txID, ledgerID, currency string, amount int64) error {
		_, err := tx.Exec(f.ctx, `INSERT INTO ledger.journal_entries
			(id, transaction_id, ledger_id, amount, currency, trace_id)
			VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)`, txID, ledgerID, amount, currency, traceID)
		return err
	}
	cases := []struct {
		name  string
		lines func(tx pgx.Tx, txID string) error
		want  string // "" = commit succeeds
	}{
		{"balanced pair", func(tx pgx.Tx, id string) error {
			return firstErr(line(tx, id, f.ledgerID, "IDR", 50), line(tx, id, f.topupID, "IDR", -50))
		}, ""},
		{"single line", func(tx pgx.Tx, id string) error {
			return line(tx, id, f.ledgerID, "IDR", 50)
		}, "unbalanced"},
		{"nonzero sum", func(tx pgx.Tx, id string) error {
			return firstErr(line(tx, id, f.ledgerID, "IDR", 50), line(tx, id, f.topupID, "IDR", -49))
		}, "unbalanced"},
		{"three lines", func(tx pgx.Tx, id string) error {
			return firstErr(line(tx, id, f.ledgerID, "IDR", 50), line(tx, id, f.topupID, "IDR", -25), line(tx, id, f.topupID, "IDR", -25))
		}, "unbalanced"},
		{"two currencies", func(tx pgx.Tx, id string) error {
			return firstErr(line(tx, id, f.topupID, "IDR", 50), line(tx, id, usd, "USD", -50))
		}, "unbalanced"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(f.ctx)
			if err := tc.lines(tx, f.scan(`SELECT gen_random_uuid()`)); err != nil {
				t.Fatalf("insert failed before commit: %v", err)
			}
			err = tx.Commit(f.ctx)
			if tc.want == "" && err != nil {
				t.Fatalf("commit: %v", err)
			}
			if tc.want != "" {
				wantErr(t, "commit", err, tc.want)
			}
		})
	}

	// A line's currency must match its ledger's (composite FK), whatever the balance.
	wantErr(t, "USD line on IDR ledger", f.exec(`INSERT INTO ledger.journal_entries
		(id, transaction_id, ledger_id, amount, currency, trace_id)
		VALUES (gen_random_uuid(), gen_random_uuid(), $1, 1, 'USD', $2)`, f.ledgerID, traceID), "journal_ledger_fk")
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestSequenceIsUniquePerLedgerWithinPartition(t *testing.T) {
	f := newFixture(t)
	if err := f.postCredit(10); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "duplicate ledger_sequence 1", f.postCredit(10), "_seq_key")
}

func TestBalanceFloor(t *testing.T) {
	f := newFixture(t)
	wantErr(t, "negative balance, no overdraft",
		f.exec(`UPDATE ledger.ledgers SET balance = -1 WHERE id = $1`, f.ledgerID), "ledgers_no_negative")

	if err := f.exec(`UPDATE ledger.ledgers SET overdraft_limit = 100, balance = -100 WHERE id = $1`, f.ledgerID); err != nil {
		t.Fatalf("balance at the overdraft floor should be allowed: %v", err)
	}
	wantErr(t, "below overdraft floor",
		f.exec(`UPDATE ledger.ledgers SET balance = -101 WHERE id = $1`, f.ledgerID), "ledgers_no_negative")
	wantErr(t, "system ledger with a stored balance",
		f.exec(`UPDATE ledger.ledgers SET balance = 5 WHERE id = $1`, f.topupID), "ledgers_kind_shape")
}

func TestOneAccountPerUserAndOneLedgerPerCurrency(t *testing.T) {
	f := newFixture(t)
	wantErr(t, "second account for user", f.exec(
		`INSERT INTO ledger.accounts (id, user_id) VALUES (gen_random_uuid(), $1)`, f.userID), "accounts_user_key")
	wantErr(t, "second IDR ledger", f.exec(`INSERT INTO ledger.ledgers (id, kind, account_id, currency)
		VALUES (gen_random_uuid(), 'USER', $1, 'IDR')`, f.accountID), "ledgers_user_currency_key")
	if err := f.exec(`INSERT INTO ledger.ledgers (id, kind, account_id, currency)
		VALUES (gen_random_uuid(), 'USER', $1, 'USD')`, f.accountID); err != nil {
		t.Fatalf("a second currency should be allowed: %v", err)
	}
}

func TestSingleActiveVoidPerOriginal(t *testing.T) {
	f := newFixture(t)
	insertTx := func(typ, status string, original any) (string, error) {
		var id string
		reason := any(nil)
		if status == "FAILED" {
			reason = "insufficient_funds"
		}
		settled := any(nil)
		if status == "SETTLED" || status == "VOIDED" {
			settled = time.Now()
		}
		err := f.pool.QueryRow(f.ctx, `INSERT INTO ledger.transactions
			(id, account_id, ledger_id, counterparty_ledger_id, type, status, amount, currency,
			 original_transaction_id, failure_reason, requested_by, version, trace_id, settled_at)
			VALUES (gen_random_uuid(), $1, $2, $3, $4, $5, 10, 'IDR', $6, $7, 'payments', 1, $8, $9)
			RETURNING id`,
			f.accountID, f.ledgerID, f.topupID, typ, status, original, reason, traceID, settled).Scan(&id)
		return id, err
	}

	orig, err := insertTx("CREDIT", "VOIDED", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := insertTx("VOID", "FAILED", orig); err != nil {
		t.Fatalf("a failed void must not block: %v", err)
	}
	if _, err := insertTx("VOID", "SETTLED", orig); err != nil {
		t.Fatalf("first active void: %v", err)
	}
	_, err = insertTx("VOID", "PENDING", orig)
	wantErr(t, "second active void", err, "transactions_single_void_key")
	_, err = insertTx("VOID", "PENDING", nil)
	wantErr(t, "void without original", err, "transactions_void_has_original")
	_, err = insertTx("CREDIT", "FAILED", nil)
	if err != nil {
		t.Fatalf("failed credit with reason: %v", err)
	}
	wantErr(t, "settled without settled_at", f.exec(`UPDATE ledger.transactions SET status = 'SETTLED', settled_at = NULL
		WHERE id = $1`, orig), "transactions_settled_at")
}

func TestEventTypeMustMatchStream(t *testing.T) {
	f := newFixture(t)
	wantErr(t, "transaction event on account stream", f.exec(`INSERT INTO ledger.events
		(event_id, stream_type, stream_id, stream_version, event_type, account_id, payload, trace_id)
		VALUES (gen_random_uuid(), 'account', $1, 1, 'TransactionSettled', $1, '{}', $2)`, f.accountID, traceID),
		"events_type_valid")
	wantErr(t, "malformed trace_id", f.exec(`INSERT INTO ledger.events
		(event_id, stream_type, stream_id, stream_version, event_type, account_id, payload, trace_id)
		VALUES (gen_random_uuid(), 'account', $1, 1, 'AccountOpened', $1, '{}', 'nope')`, f.accountID),
		"events_trace_format")
	if err := f.appendEvent(); err != nil {
		t.Fatal(err)
	}
	wantErr(t, "duplicate stream version", f.appendEvent(), "_stream_key")
}

func TestSystemLedgersSeeded(t *testing.T) {
	f := newFixture(t)
	if n := f.count(`SELECT count(*) FROM ledger.ledgers WHERE kind = 'SYSTEM' AND currency = 'IDR'
		AND code IN ('topup_clearing', 'settlement')`); n != 2 {
		t.Fatalf("seeded IDR system ledgers = %d, want 2", n)
	}
}

func TestDownMigrationRemovesSchema(t *testing.T) {
	f := newFixture(t)
	pgtest.ExecMigration(t, f.pool, "002_ledger.down.sql")
	if n := f.count(`SELECT count(*) FROM pg_namespace WHERE nspname = 'ledger'`); n != 0 {
		t.Fatal("ledger schema still present after down migration")
	}
}

func TestIdempotencyResourceIndex(t *testing.T) {
	f := newFixture(t)
	const q = `SELECT count(*) FROM pg_indexes WHERE schemaname = 'ledger' AND indexname = 'idempotency_resource_idx'`
	if n := f.count(q); n != 1 {
		t.Fatal("migration 004 index missing")
	}
	pgtest.ExecMigration(t, f.pool, "004_idempotency_resource_index.down.sql")
	if n := f.count(q); n != 0 {
		t.Fatal("index still present after 004 down")
	}
}
