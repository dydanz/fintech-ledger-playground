//go:build integration

package service_test

import (
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"auth-api/internal/repository"
	"auth-api/internal/service"
)

func (e *svcEnv) reconciler(overlap time.Duration) *service.Reconciler {
	e.t.Helper()
	// A small batch, so paging and multi-snapshot runs are exercised.
	r, err := service.NewReconciler(repository.NewReconcileRepo(e.pool), service.ReconcileConfig{
		Overlap: overlap, FullEvery: 24 * time.Hour, Batch: 7, BatchTimeout: 10 * time.Second, MaxReports: 100})
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

func (e *svcEnv) reconcile(r *service.Reconciler, full bool) service.ReconcileReport {
	e.t.Helper()
	rep, err := r.Reconcile(e.ctx, full)
	if err != nil {
		e.t.Fatalf("reconcile (full=%v): %v", full, err)
	}
	return rep
}

func reasons(rep service.ReconcileReport) []string {
	var out []string
	for _, m := range rep.Mismatches {
		out = append(out, fmt.Sprintf("%d:%s:%s", m.Check, m.Reason, m.Field))
	}
	slices.Sort(out)
	return out
}

// workload runs a seeded random mix of credits, debits (some refused for
// funds), voids, a freeze/unfreeze, concurrent debits on one account and a
// transaction left PENDING, and returns the account ids.
func (e *svcEnv) workload(seed uint64) []string {
	e.t.Helper()
	rng := rand.New(rand.NewPCG(seed, 1))
	var accts []string
	for i := range 4 {
		accts = append(accts, e.openAccount(fmt.Sprintf("rec%d-%d@x.io", seed, i)))
	}
	var settled []string
	for i := range 60 {
		acct := accts[rng.IntN(len(accts))]
		key := fmt.Sprintf("w%d-%d", seed, i)
		var r service.Response
		var err error
		switch n := rng.IntN(10); {
		case n < 5:
			r, err = e.money(e.svc.Credit, key, acct, int64(1+rng.IntN(500)), "IDR")
		case n < 9:
			r, err = e.money(e.svc.Debit, key, acct, int64(1+rng.IntN(400)), "IDR")
		default:
			if len(settled) == 0 {
				continue
			}
			// An original voided earlier is refused with an error; that's fine here.
			if r, err = e.void("v"+key, settled[rng.IntN(len(settled))], "reconcile test"); err != nil {
				continue
			}
		}
		if err != nil {
			e.t.Fatal(err)
		}
		if r.Status == http.StatusCreated || r.Status == http.StatusOK {
			if tx := decode[txBody](e.t, r).Transaction; tx.Status == "SETTLED" && tx.Type != "VOID" {
				settled = append(settled, tx.ID)
			}
		}
	}
	e.status(e.freeze(fmt.Sprintf("rec-freeze%d", seed), accts[0], "test", "topup_velocity"))
	e.status(e.unfreeze(fmt.Sprintf("rec-unfreeze%d", seed), accts[0], "test"))

	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = e.money(e.svc.Debit, fmt.Sprintf("race%d-%d", seed, i), accts[1], 10, "IDR")
		}()
	}
	wg.Wait()
	e.strand(fmt.Sprintf("stuck%d", seed), accts[2], 5)
	return accts
}

// TRD §13.3 "Double-entry": after a random mixed workload, checks 1–4 pass,
// in both modes.
func TestReconcileCleanAfterMixedWorkload(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	e.workload(1)
	// The mix covers every shape the checks distinguish.
	for _, q := range []string{
		`SELECT count(*) FROM ledger.transactions WHERE type = 'VOID' AND status = 'SETTLED'`,
		`SELECT count(*) FROM ledger.transactions WHERE status = 'VOIDED'`,
		`SELECT count(*) FROM ledger.transactions WHERE status = 'FAILED'`,
		`SELECT count(*) FROM ledger.transactions WHERE status = 'PENDING'`,
		`SELECT count(*) FROM ledger.events WHERE event_type = 'AccountUnfrozen'`,
	} {
		if e.count(q) == 0 {
			t.Fatalf("workload produced none of: %s", q)
		}
	}
	r := e.reconciler(time.Minute)

	e.resetLogs()
	rep := e.reconcile(r, true)
	if len(rep.Mismatches) != 0 {
		t.Fatalf("mismatches after a clean workload: %v", reasons(rep))
	}
	if want := e.count(`SELECT count(*) FROM ledger.transactions`); rep.Transactions != want {
		t.Fatalf("checked %d transactions, want %d", rep.Transactions, want)
	}
	if want := e.count(`SELECT count(*) FROM ledger.ledgers WHERE kind = 'USER'`); rep.Ledgers != want {
		t.Fatalf("checked %d ledgers, want %d", rep.Ledgers, want)
	}
	wantEvent(t, e.logs, "ledger.reconcile", map[string]any{"outcome": "success", "mode": "full", "level": "info", "mismatches": 0.0})

	// More traffic, then an incremental run: it checks only what changed.
	e.workload(2)
	rep = e.reconcile(r, false)
	if len(rep.Mismatches) != 0 {
		t.Fatalf("incremental mismatches: %v", reasons(rep))
	}
	if rep.Transactions == 0 || rep.Ledgers == 0 || rep.Accounts == 0 {
		t.Fatalf("incremental run checked nothing: %+v", rep)
	}
}

// Injected projection drift: a direct UPDATE of a ledger's balance.
func TestReconcileDetectsBalanceDrift(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accts := e.workload(3)
	// No overlap: the incremental run then covers only rows changed after the full sweep.
	r := e.reconciler(0)
	e.reconcile(r, true)

	var ledgerID string
	var balance int64
	if err := e.pool.QueryRow(e.ctx, `SELECT id::text, balance FROM ledger.ledgers WHERE account_id = $1`, accts[3]).Scan(&ledgerID, &balance); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.ledgers SET balance = balance + 7 WHERE id = $1`, ledgerID); err != nil {
		t.Fatal(err)
	}

	// updated_at didn't move, so only the full sweep sees it.
	if rep := e.reconcile(r, false); len(rep.Mismatches) != 0 {
		t.Fatalf("incremental run reported untouched rows: %v", reasons(rep))
	}
	e.resetLogs()
	rep := e.reconcile(r, true)
	if got := reasons(rep); !slices.Equal(got, []string{"1:balance_mismatch:balance"}) {
		t.Fatalf("mismatches = %v", got)
	}
	wantReconcile(t, e.logs, "balance_mismatch", map[string]any{"outcome": "failure", "level": "error", "check": 1.0,
		"ledger_id": ledgerID, "expected": float64(balance), "actual": float64(balance + 7)})
	wantReconcile(t, e.logs, "mismatches_found", map[string]any{"outcome": "failure", "level": "error", "mismatches": 1.0})

	// It keeps firing on incremental runs until it is repaired.
	if got := reasons(e.reconcile(r, false)); !slices.Equal(got, []string{"1:balance_mismatch:balance"}) {
		t.Fatalf("carried mismatch = %v", got)
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.ledgers SET balance = balance - 7 WHERE id = $1`, ledgerID); err != nil {
		t.Fatal(err)
	}
	if rep := e.reconcile(r, false); len(rep.Mismatches) != 0 {
		t.Fatalf("after repair: %v", reasons(rep))
	}
}

// A sequence gap: a line posted with ledger_sequence last+2 (and a projection
// that followed it), from a transaction with no projection row.
func TestReconcileDetectsSequenceGap(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accts := e.workload(4)
	r := e.reconciler(time.Minute)

	var userLedger, sysLedger string
	var balance, last int64
	if err := e.pool.QueryRow(e.ctx, `SELECT id::text, balance, last_sequence FROM ledger.ledgers WHERE account_id = $1`,
		accts[3]).Scan(&userLedger, &balance, &last); err != nil {
		t.Fatal(err)
	}
	if err := e.pool.QueryRow(e.ctx, `SELECT id::text FROM ledger.ledgers WHERE code = 'topup_clearing' AND currency = 'IDR'`).Scan(&sysLedger); err != nil {
		t.Fatal(err)
	}
	tx, err := e.pool.Begin(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	stray := "0192f3c4-0000-7000-8000-0000000000ff"
	if _, err := tx.Exec(e.ctx, `INSERT INTO ledger.journal_entries
		(id, transaction_id, ledger_id, amount, currency, ledger_sequence, balance_after, trace_id) VALUES
		(gen_random_uuid(), $1, $2, 5, 'IDR', $3, $4, $6),
		(gen_random_uuid(), $1, $5, -5, 'IDR', NULL, NULL, $6)`,
		stray, userLedger, last+2, balance+5, sysLedger, testTrace); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(e.ctx, `UPDATE ledger.ledgers SET balance = $2, last_sequence = $3 WHERE id = $1`,
		userLedger, balance+5, last+2); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(e.ctx); err != nil {
		t.Fatal(err)
	}

	e.resetLogs()
	rep := e.reconcile(r, true)
	if got := reasons(rep); !slices.Equal(got, []string{"2:sequence_gap:lines", "3:unbalanced_transaction:transaction"}) {
		t.Fatalf("mismatches = %v", got)
	}
	wantReconcile(t, e.logs, "sequence_gap", map[string]any{"ledger_id": userLedger, "check": 2.0,
		"expected": float64(last + 2), "actual": float64(last + 1)})
	wantReconcile(t, e.logs, "unbalanced_transaction", map[string]any{"transaction_id": stray, "check": 3.0})
}

// Projection drift on a transaction and an account, found by the
// incremental run because the bad write moved updated_at.
func TestReconcileDetectsTransactionAndAccountDrift(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("drift@x.io")
	cr := decode[txBody](t, must(e.money(e.svc.Credit, "drift-cr", acct, 100, "IDR"))).Transaction
	r := e.reconciler(time.Minute)
	if rep := e.reconcile(r, true); len(rep.Mismatches) != 0 {
		t.Fatalf("baseline: %v", reasons(rep))
	}

	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.transactions SET status = 'FAILED', failure_reason = 'insufficient_funds',
		settled_at = NULL, updated_at = now() WHERE id = $1`, cr.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.accounts SET status = 'FROZEN', updated_at = now() WHERE id = $1`, acct); err != nil {
		t.Fatal(err)
	}
	e.resetLogs()
	rep := e.reconcile(r, false)
	if got := reasons(rep); !slices.Equal(got, []string{"3:unbalanced_transaction:lines", "4:projection_drift:status", "4:projection_drift:status"}) {
		t.Fatalf("mismatches = %v", got)
	}
	var drift []map[string]any
	for _, rec := range records(t, e.logs) {
		if rec["reason"] == "projection_drift" {
			drift = append(drift, rec)
		}
	}
	if len(drift) != 2 || drift[0]["transaction_id"] != cr.ID || drift[0]["expected"] != "SETTLED" || drift[0]["actual"] != "FAILED" ||
		drift[1]["account_id"] != acct || drift[1]["expected"] != "ACTIVE" || drift[1]["actual"] != "FROZEN" {
		t.Fatalf("projection_drift events = %v", drift)
	}
}

func must(r service.Response, err error) service.Response {
	if err != nil {
		panic(err)
	}
	return r
}
