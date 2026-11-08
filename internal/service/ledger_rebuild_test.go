//go:build integration

package service_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"auth-api/internal/repository"
	"auth-api/internal/service"
)

func (e *svcEnv) rebuild(opts service.RebuildOptions) service.RebuildReport {
	e.t.Helper()
	b, err := service.NewRebuilder(repository.NewRebuildRepo(e.pool, 5*time.Second), 7)
	if err != nil {
		e.t.Fatal(err)
	}
	rep, err := b.Rebuild(e.ctx, opts)
	if err != nil {
		e.t.Fatalf("rebuild %+v: %v", opts, err)
	}
	return rep
}

// projections is every rebuilt column of every projection row, as text.
func (e *svcEnv) projections() []string {
	e.t.Helper()
	rows, err := e.pool.Query(e.ctx, `
		SELECT 'a ' || id || ' ' || user_id || ' ' || status || ' ' || version FROM ledger.accounts
		UNION ALL
		SELECT 'l ' || id || ' ' || balance || ' ' || last_sequence FROM ledger.ledgers WHERE kind = 'USER'
		UNION ALL
		SELECT concat_ws(' ', 't', id, account_id, ledger_id, counterparty_ledger_id, type, status, amount, currency,
		       original_transaction_id, failure_reason, requested_by, version, settled_at, reference, metadata)
		  FROM ledger.transactions
		ORDER BY 1`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			e.t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

// corrupt overwrites every rebuilt column of every projection row.
func (e *svcEnv) corrupt(where string) {
	e.t.Helper()
	for _, q := range []string{
		`UPDATE ledger.ledgers SET balance = 0, last_sequence = 0 WHERE kind = 'USER'` + where,
		`UPDATE ledger.accounts SET status = 'ACTIVE', version = 1 WHERE true` + strings.ReplaceAll(where, "account_id", "id"),
		// FAILED isn't covered by transactions_single_void_key, so every row can take it.
		`UPDATE ledger.transactions SET status = 'FAILED', failure_reason = 'processing_timeout', settled_at = NULL,
			version = 99, ledger_id = NULL, amount = amount + 1, requested_by = 'nobody' WHERE true` + where,
	} {
		if _, err := e.pool.Exec(e.ctx, q); err != nil {
			e.t.Fatalf("%s: %v", q, err)
		}
	}
}

// TRD §13.3 "Rebuild": rebuild-projections from the events and the journal
// reproduces the projections exactly.
func TestRebuildReproducesProjectionsExactly(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	e.workload(10)
	want := e.projections()

	e.resetLogs()
	if rep := e.rebuild(service.RebuildOptions{}); rep.Changed != 0 || rep.Checked == 0 {
		t.Fatalf("dry run on intact projections: %+v", rep)
	}
	wantEvent(t, e.logs, "ledger.rebuild", map[string]any{"outcome": "success", "mode": "dry_run", "changed": 0.0, "level": "info"})

	e.corrupt("")
	corrupted := e.projections()
	rows := e.count(`SELECT (SELECT count(*) FROM ledger.accounts) + (SELECT count(*) FROM ledger.transactions)
		+ (SELECT count(*) FROM ledger.ledgers WHERE kind = 'USER' AND id IN (SELECT ledger_id FROM ledger.journal_entries))`)

	// Dry run: reports every corrupted row, writes nothing.
	e.resetLogs()
	rep := e.rebuild(service.RebuildOptions{})
	if rep.Changed != rows {
		t.Fatalf("dry run found %d differing rows, want %d", rep.Changed, rows)
	}
	if got := e.projections(); !slices.Equal(got, corrupted) {
		t.Fatal("dry run wrote to the projections")
	}
	var diffEvents int
	for _, r := range records(t, e.logs) {
		if r["event"] == "ledger.rebuild" && r["field"] != nil {
			diffEvents++
			if r["applied"] != false || r["level"] != "warn" || r["mode"] != "dry_run" {
				t.Fatalf("dry-run diff event = %v", r)
			}
		}
	}
	if diffEvents != len(rep.Diffs) {
		t.Fatalf("logged %d diff events for %d diffs", diffEvents, len(rep.Diffs))
	}
	if strings.Contains(e.logs.String(), "SECRET-REF") || strings.Contains(e.logs.String(), "secret-meta") {
		t.Fatal("rebuild logged a reference or metadata")
	}

	// Apply: the projections are exactly what they were.
	e.resetLogs()
	if rep := e.rebuild(service.RebuildOptions{Apply: true}); rep.Changed != rows {
		t.Fatalf("apply changed %d rows, want %d", rep.Changed, rows)
	}
	var summary map[string]any
	for _, r := range records(t, e.logs) {
		if r["event"] == "ledger.rebuild" && r["field"] == nil {
			summary = r
		}
	}
	if summary["outcome"] != "success" || summary["mode"] != "apply" || summary["changed"] != float64(rows) || summary["level"] != "info" {
		t.Fatalf("apply summary = %v", summary)
	}
	if got := e.projections(); !slices.Equal(got, want) {
		for i := range min(len(got), len(want)) {
			if got[i] != want[i] {
				t.Errorf("row %d:\n got  %s\n want %s", i, got[i], want[i])
				break
			}
		}
		t.Fatal("rebuild did not reproduce the projections")
	}
	if rep := e.reconcile(e.reconciler(time.Minute), true); len(rep.Mismatches) != 0 {
		t.Fatalf("reconcile after rebuild: %v", reasons(rep))
	}
	if rep := e.rebuild(service.RebuildOptions{Apply: true}); rep.Changed != 0 {
		t.Fatalf("second apply changed %d rows", rep.Changed)
	}
}

// --account limits the rebuild to one account's rows.
func TestRebuildOneAccount(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accts := e.workload(11)
	want := e.projections()
	e.corrupt(fmt.Sprintf(" AND account_id IN ('%s', '%s')", accts[0], accts[1]))

	e.rebuild(service.RebuildOptions{Apply: true, AccountID: accts[0]})
	rep := e.rebuild(service.RebuildOptions{})
	for _, d := range rep.Diffs {
		var owner string
		switch d.Kind {
		case "account":
			owner = d.ID
		default:
			table := map[string]string{"ledger": "ledgers", "transaction": "transactions"}[d.Kind]
			if err := e.pool.QueryRow(e.ctx, `SELECT account_id::text FROM ledger.`+table+` WHERE id = $1`, d.ID).Scan(&owner); err != nil {
				t.Fatal(err)
			}
		}
		if owner != accts[1] {
			t.Fatalf("diff left on %s %s of account %s; only %s should remain", d.Kind, d.ID, owner, accts[1])
		}
	}
	if rep.Changed == 0 {
		t.Fatal("the other corrupted account was rebuilt too")
	}
	e.rebuild(service.RebuildOptions{Apply: true, AccountID: accts[1]})
	if got := e.projections(); !slices.Equal(got, want) {
		t.Fatal("projections differ after rebuilding both accounts")
	}
}

// Rebuilding under live traffic is exact: each row is rebuilt under the
// write path's own lock.
func TestRebuildUnderLoad(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accts := e.workload(12)
	ctx, cancel := context.WithCancel(e.ctx)
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ctx.Err() == nil; i++ {
				op := e.svc.Credit
				if i%2 == 1 {
					op = e.svc.Debit
				}
				_, _ = e.money(op, fmt.Sprintf("load%d-%d", w, i), accts[w%len(accts)], 3, "IDR")
			}
		}()
	}
	for range 3 {
		if rep := e.rebuild(service.RebuildOptions{Apply: true}); rep.Changed != 0 {
			t.Errorf("rebuild under load changed %d rows of intact projections: %v", rep.Changed, rep.Diffs)
		}
	}
	cancel()
	wg.Wait()
	if rep := e.reconcile(e.reconciler(time.Minute), true); len(rep.Mismatches) != 0 {
		t.Fatalf("reconcile after load + rebuild: %v", reasons(rep))
	}
}
