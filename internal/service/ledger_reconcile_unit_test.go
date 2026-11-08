package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"auth-api/internal/ledger"
	"auth-api/internal/repository"
	"auth-api/internal/service"
)

const (
	recLedger  = "0192f3c4-0000-7000-8000-00000000a001"
	recLedger2 = "0192f3c4-0000-7000-8000-00000000a002"
	recTx      = "0192f3c4-0000-7000-8000-00000000b001"
	recUser    = "0192f3c4-0000-7000-8000-00000000c001"
)

// fakeReconcileStore serves crafted check rows. Full runs (zero since) list
// every id; incremental runs list only the touched ones.
type fakeReconcileStore struct {
	txs, touchedTxs, ledgers, accounts []string
	txLedgers                          map[string]string
	ledgerChecks                       map[string]repository.LedgerCheck
	txChecks                           map[string]repository.TransactionCheck
	accountChecks                      map[string]repository.AccountCheck
	events                             map[string][]repository.StoredEvent
	unbalanced                         []repository.CurrencyTotal
	listErr                            error

	clock  *time.Time                // the "database" clock; nil means time.Now
	sinces []time.Time               // since of each ListTransactionIDs page
	bases  [][]repository.LedgerBase // each CheckLedgers call
}

func page(ids []string, after string, limit int) []string {
	var out []string
	for _, id := range slices.Sorted(slices.Values(ids)) {
		if id > after && len(out) < limit {
			out = append(out, id)
		}
	}
	return out
}

func (f *fakeReconcileStore) ListTransactionIDs(_ context.Context, since time.Time, after string, limit int) ([]string, error) {
	f.sinces = append(f.sinces, since)
	if f.listErr != nil {
		return nil, f.listErr
	}
	if since.IsZero() {
		return page(f.txs, after, limit), nil
	}
	return page(f.touchedTxs, after, limit), nil
}

func (f *fakeReconcileStore) ListAccountIDs(_ context.Context, since time.Time, after string, limit int) ([]string, error) {
	if since.IsZero() {
		return page(f.accounts, after, limit), nil
	}
	return nil, nil
}

func (f *fakeReconcileStore) ListUserLedgerIDs(_ context.Context, after string, limit int) ([]string, error) {
	return page(f.ledgers, after, limit), nil
}

func (f *fakeReconcileStore) LedgersOfTransactions(_ context.Context, ids []string) ([]string, error) {
	var out []string
	for _, id := range ids {
		if l, ok := f.txLedgers[id]; ok {
			out = append(out, l)
		}
	}
	return out, nil
}

func (f *fakeReconcileStore) UnbalancedCurrencies(context.Context) ([]repository.CurrencyTotal, error) {
	return f.unbalanced, nil
}

func (f *fakeReconcileStore) OrphanJournalTransactions(context.Context, int) ([]string, error) {
	return nil, nil
}

func (f *fakeReconcileStore) Now(context.Context) (time.Time, error) {
	if f.clock != nil {
		return *f.clock, nil
	}
	return time.Now(), nil
}

func (f *fakeReconcileStore) Snapshot(_ context.Context, fn func(repository.ReconcileReader) error) error {
	return fn(f)
}

func (f *fakeReconcileStore) CheckLedgers(_ context.Context, bases []repository.LedgerBase) ([]repository.LedgerCheck, error) {
	f.bases = append(f.bases, bases)
	var out []repository.LedgerCheck
	for _, b := range bases {
		if c, ok := f.ledgerChecks[b.LedgerID]; ok {
			out = append(out, c)
		}
	}
	return out, nil
}

func (f *fakeReconcileStore) CheckTransactions(_ context.Context, ids []string) ([]repository.TransactionCheck, error) {
	var out []repository.TransactionCheck
	for _, id := range ids {
		c, ok := f.txChecks[id]
		if !ok {
			c = repository.TransactionCheck{ID: id}
		}
		out = append(out, c)
	}
	return out, nil
}

func (f *fakeReconcileStore) CheckAccounts(_ context.Context, ids []string) ([]repository.AccountCheck, error) {
	var out []repository.AccountCheck
	for _, id := range ids {
		out = append(out, f.accountChecks[id])
	}
	return out, nil
}

func (f *fakeReconcileStore) StreamEvents(_ context.Context, _ ledger.StreamType, ids []string) ([]repository.StoredEvent, error) {
	var out []repository.StoredEvent
	for _, id := range ids {
		out = append(out, f.events[id]...)
	}
	return out, nil
}

func stored(t *testing.T, stream string, payloads ...ledger.Payload) []repository.StoredEvent {
	t.Helper()
	out := make([]repository.StoredEvent, len(payloads))
	for i, p := range payloads {
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = repository.StoredEvent{StreamID: stream, StreamVersion: int64(i + 1), Type: p.EventType(),
			AccountID: testAccount, Payload: raw}
	}
	return out
}

// settledCredit is a consistent SETTLED credit of 100: its projection row,
// journal aggregates and events.
func settledCredit(t *testing.T) (repository.TransactionCheck, []repository.StoredEvent) {
	return repository.TransactionCheck{ID: recTx, Exists: true, AccountID: testAccount, LedgerID: recLedger,
			Type: ledger.Credit, Status: ledger.StatusSettled, Amount: 100, Currency: "IDR", Version: 3,
			RequestedBy: "payments", Lines: 2, JournalSum: 0, Currencies: 1},
		stored(t, recTx,
			ledger.TransactionRequested{Type: ledger.Credit, Amount: 100, Currency: "IDR", Counterparty: "topup_clearing", RequestedBy: "payments"},
			ledger.TransactionProcessing{LedgerID: recLedger},
			ledger.TransactionSettled{LedgerSequence: 1, BalanceAfter: 100})
}

func cleanLedger(id string) repository.LedgerCheck {
	return repository.LedgerCheck{LedgerID: id, Balance: 100, LastSequence: 2, Lines: 2, DistinctSeqs: 2,
		MinSeq: 1, MaxSeq: 2, JournalSum: 100}
}

func newTestReconciler(t *testing.T, f *fakeReconcileStore, clock *time.Time) *service.Reconciler {
	t.Helper()
	r, err := service.NewReconciler(f, service.ReconcileConfig{Overlap: time.Minute, FullEvery: 24 * time.Hour,
		Batch: 2, BatchTimeout: time.Second, MaxReports: 3, Now: func() time.Time { return *clock }})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

type found struct {
	check         int
	reason, field string
}

func mismatches(rep service.ReconcileReport) []found {
	var out []found
	for _, m := range rep.Mismatches {
		out = append(out, found{m.Check, m.Reason, m.Field})
	}
	return out
}

func TestReconcileLedgerChecks(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*repository.LedgerCheck)
		want   []found
	}{
		"clean":            {func(*repository.LedgerCheck) {}, nil},
		"balance drift":    {func(c *repository.LedgerCheck) { c.Balance = 101 }, []found{{1, "balance_mismatch", "balance"}}},
		"missing line":     {func(c *repository.LedgerCheck) { c.LastSequence = 3 }, []found{{2, "sequence_gap", "lines"}}},
		"duplicate":        {func(c *repository.LedgerCheck) { c.DistinctSeqs = 1 }, []found{{2, "sequence_gap", "duplicate_sequences"}}},
		"shifted range":    {func(c *repository.LedgerCheck) { c.MinSeq, c.MaxSeq = 2, 3 }, []found{{2, "sequence_gap", "ledger_sequence"}}},
		"line without seq": {func(c *repository.LedgerCheck) { c.NullSeq = 1 }, []found{{2, "sequence_gap", "ledger_sequence"}}},
		"behind base": {func(c *repository.LedgerCheck) {
			c.BaseSequence, c.BaseBalance, c.Lines, c.DistinctSeqs, c.JournalSum = 5, 0, 0, 0, 100
		}, []found{{2, "sequence_gap", "last_sequence"}}},
		"bad balance_after": {func(c *repository.LedgerCheck) { c.BadRunning = 1 }, []found{{2, "balance_mismatch", "balance_after"}}},
		"from checkpoint": {func(c *repository.LedgerCheck) {
			c.BaseSequence, c.BaseBalance, c.Lines, c.DistinctSeqs, c.MinSeq, c.JournalSum = 1, 40, 1, 1, 2, 60
		}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			c := cleanLedger(recLedger)
			tc.mutate(&c)
			f := &fakeReconcileStore{ledgers: []string{recLedger}, ledgerChecks: map[string]repository.LedgerCheck{recLedger: c}}
			now := time.Now()
			ctx, _ := logCtx()
			rep, err := newTestReconciler(t, f, &now).Reconcile(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			if got := mismatches(rep); !slices.Equal(got, tc.want) {
				t.Fatalf("mismatches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcileTransactionChecks(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*repository.TransactionCheck, *[]repository.StoredEvent)
		want   []found
	}{
		"clean":          {func(*repository.TransactionCheck, *[]repository.StoredEvent) {}, nil},
		"one line":       {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) { c.Lines = 1 }, []found{{3, "unbalanced_transaction", "lines"}}},
		"sum not zero":   {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) { c.JournalSum = 5 }, []found{{3, "unbalanced_transaction", "sum"}}},
		"wrong currency": {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) { c.WrongCurrency = 1 }, []found{{3, "unbalanced_transaction", "currency"}}},
		"wrong ledger":   {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) { c.WrongLedger = 1 }, []found{{3, "unbalanced_transaction", "ledger_id"}}},
		"lines but no row": {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) {
			*c = repository.TransactionCheck{ID: recTx, Lines: 2}
		}, []found{{3, "unbalanced_transaction", "transaction"}}},
		"status drift": {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) {
			c.Status, c.FailureReason, c.Lines = ledger.StatusFailed, ledger.ReasonInsufficientFunds, 0
		}, []found{{4, "projection_drift", "status"}}},
		"version drift": {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) { c.Version = 4 }, []found{{4, "projection_drift", "version"}}},
		"amount drift":  {func(c *repository.TransactionCheck, _ *[]repository.StoredEvent) { c.Amount = 99 }, []found{{4, "projection_drift", "amount"}}},
		"no events": {func(_ *repository.TransactionCheck, e *[]repository.StoredEvent) { *e = nil },
			[]found{{4, "projection_drift", "events"}}},
		"version gap in stream": {func(_ *repository.TransactionCheck, e *[]repository.StoredEvent) { (*e)[2].StreamVersion = 4 },
			[]found{{4, "projection_drift", "events"}}},
		"undecodable payload": {func(_ *repository.TransactionCheck, e *[]repository.StoredEvent) { (*e)[1].Payload = []byte(`{"x":1}`) },
			[]found{{4, "projection_drift", "events"}}},
	} {
		t.Run(name, func(t *testing.T) {
			c, events := settledCredit(t)
			tc.mutate(&c, &events)
			f := &fakeReconcileStore{txs: []string{recTx},
				txChecks: map[string]repository.TransactionCheck{recTx: c}, events: map[string][]repository.StoredEvent{recTx: events}}
			now := time.Now()
			ctx, _ := logCtx()
			rep, err := newTestReconciler(t, f, &now).Reconcile(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			if got := mismatches(rep); !slices.Equal(got, tc.want) {
				t.Fatalf("mismatches = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestReconcileAccountAndGlobalChecks(t *testing.T) {
	events := stored(t, testAccount,
		ledger.AccountOpened{UserID: recUser},
		ledger.LedgerOpened{LedgerID: recLedger, Currency: "IDR"},
		ledger.AccountFrozen{Reason: "x", Actor: "fraud-engine", Rule: "topup_velocity"})
	clean := repository.AccountCheck{ID: testAccount, Exists: true, UserID: recUser, Status: ledger.AccountStatusFrozen,
		Version: 3, Ledgers: map[string]string{"IDR": recLedger}}
	for name, tc := range map[string]struct {
		mutate func(*repository.AccountCheck)
		want   []found
	}{
		"clean":         {func(*repository.AccountCheck) {}, nil},
		"status drift":  {func(c *repository.AccountCheck) { c.Status = ledger.AccountStatusActive }, []found{{4, "projection_drift", "status"}}},
		"extra ledger":  {func(c *repository.AccountCheck) { c.Ledgers = map[string]string{"IDR": recLedger, "USD": recLedger2} }, []found{{4, "projection_drift", "ledgers"}}},
		"missing row":   {func(c *repository.AccountCheck) { *c = repository.AccountCheck{ID: testAccount} }, []found{{4, "projection_drift", "account"}}},
		"user mismatch": {func(c *repository.AccountCheck) { c.UserID = recTx }, []found{{4, "projection_drift", "user_id"}}},
	} {
		t.Run(name, func(t *testing.T) {
			c := clean
			tc.mutate(&c)
			f := &fakeReconcileStore{accounts: []string{testAccount}, accountChecks: map[string]repository.AccountCheck{testAccount: c},
				events: map[string][]repository.StoredEvent{testAccount: events}}
			now := time.Now()
			ctx, _ := logCtx()
			rep, err := newTestReconciler(t, f, &now).Reconcile(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			if got := mismatches(rep); !slices.Equal(got, tc.want) {
				t.Fatalf("mismatches = %v, want %v", got, tc.want)
			}
		})
	}

	t.Run("currency sum", func(t *testing.T) {
		f := &fakeReconcileStore{unbalanced: []repository.CurrencyTotal{{Currency: "IDR", Total: "7"}}}
		now := time.Now()
		ctx, logs := logCtx()
		rep, err := newTestReconciler(t, f, &now).Reconcile(ctx, true)
		if err != nil || !slices.Equal(mismatches(rep), []found{{3, "unbalanced_transaction", "currency_sum"}}) {
			t.Fatalf("got %v %v", mismatches(rep), err)
		}
		wantReconcile(t, logs, "unbalanced_transaction", map[string]any{"outcome": "failure", "currency": "IDR", "level": "error"})
	})
}

// The schedule: the first run is full, the next ones incremental from the
// previous start minus the overlap, a full sweep again after FullEvery, and a
// failed run keeps the watermark.
func TestReconcileScheduleAndWatermark(t *testing.T) {
	c, events := settledCredit(t)
	f := &fakeReconcileStore{txs: []string{recTx}, ledgers: []string{recLedger},
		txChecks:     map[string]repository.TransactionCheck{recTx: c},
		events:       map[string][]repository.StoredEvent{recTx: events},
		ledgerChecks: map[string]repository.LedgerCheck{recLedger: cleanLedger(recLedger)}}
	t0 := time.Date(2026, 11, 8, 10, 0, 0, 0, time.UTC)
	now := t0
	db := t0.Add(-7 * time.Minute) // the worker's clock is 7 minutes fast: the watermark follows the database
	f.clock = &db
	r := newTestReconciler(t, f, &now)
	ctx, logs := logCtx()

	run := func() time.Time {
		t.Helper()
		f.sinces = nil
		_ = r.Run(ctx)
		return f.sinces[0]
	}
	if s := run(); !s.IsZero() {
		t.Fatalf("first run since = %v, want a full sweep", s)
	}
	wantEvent(t, logs, "ledger.reconcile", map[string]any{"outcome": "success", "mode": "full", "transactions": 1.0, "ledgers": 1.0})

	now, db = t0.Add(time.Minute), db.Add(time.Minute)
	if s := run(); !s.Equal(t0.Add(-8 * time.Minute)) {
		t.Fatalf("incremental since = %v, want the first run's database start minus the overlap", s)
	}

	f.listErr = errors.New("connection refused")
	now, db = t0.Add(2*time.Minute), db.Add(time.Minute)
	logs.Reset()
	if err := r.Run(ctx); err == nil {
		t.Fatal("list failure not returned")
	}
	wantEvent(t, logs, "ledger.reconcile", map[string]any{"outcome": "failure", "reason": "pg_error", "level": "error"})
	f.listErr = nil
	now, db = t0.Add(3*time.Minute), db.Add(time.Minute)
	if s := run(); !s.Equal(t0.Add(-7 * time.Minute)) { // still from the last successful run (db t0-6m) minus the overlap
		t.Fatalf("since after a failed run = %v, want %v", s, t0)
	}

	now = t0.Add(24 * time.Hour)
	if s := run(); !s.IsZero() {
		t.Fatalf("since after FullEvery = %v, want a full sweep", s)
	}
}

// A ledger found broken is checked again on every run, even when nothing
// touches it, until it is clean; a clean one is read from its verified point.
func TestReconcileCarriesMismatchesAndVerifiedPoints(t *testing.T) {
	bad := cleanLedger(recLedger)
	bad.Balance = 101
	f := &fakeReconcileStore{ledgers: []string{recLedger, recLedger2},
		ledgerChecks: map[string]repository.LedgerCheck{recLedger: bad, recLedger2: cleanLedger(recLedger2)}}
	now := time.Now()
	r := newTestReconciler(t, f, &now)
	ctx, logs := logCtx()
	if _, err := r.Reconcile(ctx, true); err != nil {
		t.Fatal(err)
	}
	wantReconcile(t, logs, "balance_mismatch", map[string]any{"outcome": "failure",
		"ledger_id": recLedger, "check": 1.0, "expected": 100.0, "actual": 101.0})

	// Nothing touched: the incremental run checks only the carried ledger, from its checkpoint.
	f.bases = nil
	logs.Reset()
	rep, err := r.Reconcile(ctx, false)
	if err != nil || len(rep.Mismatches) != 1 {
		t.Fatalf("carried mismatch not reported again: %v %v", mismatches(rep), err)
	}
	if want := [][]repository.LedgerBase{{{LedgerID: recLedger, Sequence: -1}}}; len(f.bases) != 1 || !slices.Equal(f.bases[0], want[0]) {
		t.Fatalf("bases = %v, want %v", f.bases, want)
	}

	// Repaired, and touched by a new transaction: clean, and the next check
	// starts at the verified point.
	f.ledgerChecks[recLedger] = cleanLedger(recLedger)
	f.touchedTxs, f.txLedgers = []string{recTx}, map[string]string{recTx: recLedger}
	c, events := settledCredit(t)
	f.txChecks, f.events = map[string]repository.TransactionCheck{recTx: c}, map[string][]repository.StoredEvent{recTx: events}
	if rep, err := r.Reconcile(ctx, false); err != nil || len(rep.Mismatches) != 0 {
		t.Fatalf("after repair: %v %v", mismatches(rep), err)
	}
	f.bases = nil
	if _, err := r.Reconcile(ctx, false); err != nil {
		t.Fatal(err)
	}
	if want := (repository.LedgerBase{LedgerID: recLedger, Sequence: 2, Balance: 100}); len(f.bases) != 1 || f.bases[0][0] != want {
		t.Fatalf("bases = %v, want %v", f.bases, want)
	}
}

func TestReconcileCapsMismatchEvents(t *testing.T) {
	f := &fakeReconcileStore{ledgerChecks: map[string]repository.LedgerCheck{}}
	for _, id := range []string{"0192f3c4-0000-7000-8000-00000000d001", "0192f3c4-0000-7000-8000-00000000d002",
		"0192f3c4-0000-7000-8000-00000000d003", "0192f3c4-0000-7000-8000-00000000d004", "0192f3c4-0000-7000-8000-00000000d005"} {
		c := cleanLedger(id)
		c.Balance = 1
		f.ledgers = append(f.ledgers, id)
		f.ledgerChecks[id] = c
	}
	now := time.Now()
	ctx, logs := logCtx()
	if _, err := newTestReconciler(t, f, &now).Reconcile(ctx, true); err != nil {
		t.Fatal(err)
	}
	var mismatchEvents int
	for _, r := range records(t, logs) {
		if r["event"] == "ledger.reconcile" && r["reason"] == "balance_mismatch" {
			mismatchEvents++
		}
	}
	if mismatchEvents != 3 { // MaxReports
		t.Fatalf("logged %d mismatch events, want 3", mismatchEvents)
	}
	wantReconcile(t, logs, "mismatches_found", map[string]any{"outcome": "failure", "mismatches": 5.0, "suppressed": 2.0,
		"ledgers": 5.0, "level": "error"})
}

// wantReconcile asserts exactly one ledger.reconcile record with reason, and fields.
func wantReconcile(t *testing.T, buf *bytes.Buffer, reason string, fields map[string]any) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, r := range records(t, buf) {
		if r["event"] == "ledger.reconcile" && r["reason"] == reason {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want 1 ledger.reconcile %q record, got %d in:\n%s", reason, len(found), buf)
	}
	for k, v := range fields {
		if found[0][k] != v {
			t.Errorf("ledger.reconcile.%s = %v, want %v (record %v)", k, found[0][k], v, found[0])
		}
	}
	return found[0]
}
