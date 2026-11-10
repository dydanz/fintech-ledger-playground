package service

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/repository"
)

// ReconcileStore is what the reconciler reads (repository.ReconcileRepo).
type ReconcileStore interface {
	ListTransactionIDs(ctx context.Context, since time.Time, after string, limit int) ([]string, error)
	ListAccountIDs(ctx context.Context, since time.Time, after string, limit int) ([]string, error)
	ListUserLedgerIDs(ctx context.Context, after string, limit int) ([]string, error)
	LedgersOfTransactions(ctx context.Context, ids []string) ([]string, error)
	UnbalancedCurrencies(ctx context.Context) ([]repository.CurrencyTotal, error)
	OrphanJournalTransactions(ctx context.Context, limit int) ([]string, error)
	Snapshot(ctx context.Context, fn func(repository.ReconcileReader) error) error
	Now(ctx context.Context) (time.Time, error) // the database clock
}

type ReconcileConfig struct {
	// Overlap re-checks rows updated this long before the previous run
	// started. A row's updated_at is its transaction's start time, so a
	// transaction that commits after a run began is still covered. It must
	// exceed the longest write transaction (LEDGER_PROCESS_TIMEOUT). The
	// watermark is read from the Postgres clock, so worker clock skew
	// doesn't matter.
	Overlap time.Duration
	// FullEvery is how often the full sweep runs (TRD §7.6 "nightly"). The
	// first run after a replica becomes leader is always a full sweep.
	FullEvery    time.Duration
	Batch        int              // ids per snapshot
	BatchTimeout time.Duration    // bound on one batch's queries
	MaxReports   int              // mismatch events logged per run; the summary counts all
	Now          func() time.Time // nil means time.Now; tests inject a clock
}

// Mismatch reasons (logging catalog, ledger.reconcile).
const (
	reasonBalanceMismatch = "balance_mismatch"
	reasonSequenceGap     = "sequence_gap"
	reasonUnbalanced      = "unbalanced_transaction"
	reasonProjectionDrift = "projection_drift"
)

// Mismatch is one failed check (TRD §7.6, checks 1–4). Field names what
// differs; Expected is what the source of truth implies, Actual what was found.
type Mismatch struct {
	Check         int
	Reason        string
	LedgerID      string
	TransactionID string
	AccountID     string
	Currency      string
	Field         string
	Expected      any
	Actual        any
}

// ReconcileReport is the outcome of one run.
type ReconcileReport struct {
	Full                            bool
	Transactions, Ledgers, Accounts int
	Mismatches                      []Mismatch
}

// Reconciler proves the projections equal the fold of the source of truth
// (TRD §7.6). It never corrects anything: a mismatch is logged as
// ledger.reconcile failure (which pages) and fixed through a VOID or a
// reviewed rebuild-projections.
//
// Incremental runs check what changed since the previous run; the state for
// that lives in memory, on the job's leader. A new leader starts with a full
// sweep, so nothing is skipped across a failover.
type Reconciler struct {
	store ReconcileStore
	cfg   ReconcileConfig
	now   func() time.Time

	watermark time.Time // start of the last successful run, by the database clock
	lastFull  time.Time
	// verified is, per USER ledger, the last (sequence, balance) a run found
	// consistent. Incremental runs only read the journal after it; the full
	// sweep starts again from the checkpoints.
	verified map[string]repository.LedgerBase
	// carry is what last run found broken. It is checked again every run,
	// so the failure keeps firing until the data is repaired.
	carry carried
}

type carried struct {
	transactions, ledgers, accounts map[string]struct{}
	global                          bool // a currency sum or an orphan line
}

func newCarried() carried {
	return carried{transactions: map[string]struct{}{}, ledgers: map[string]struct{}{}, accounts: map[string]struct{}{}}
}

func (c *carried) add(m Mismatch) {
	switch {
	case m.TransactionID != "":
		c.transactions[m.TransactionID] = struct{}{}
	case m.LedgerID != "":
		c.ledgers[m.LedgerID] = struct{}{}
	case m.AccountID != "":
		c.accounts[m.AccountID] = struct{}{}
	default:
		c.global = true
	}
}

func (c *carried) merge(o carried) {
	maps.Copy(c.transactions, o.transactions)
	maps.Copy(c.ledgers, o.ledgers)
	maps.Copy(c.accounts, o.accounts)
	c.global = c.global || o.global
}

// maxVerified bounds the verified map; past it the map is dropped and the
// next runs read those ledgers from their checkpoints again.
const maxVerified = 200_000

func NewReconciler(store ReconcileStore, cfg ReconcileConfig) (*Reconciler, error) {
	if cfg.Overlap < 0 || cfg.FullEvery <= 0 || cfg.Batch < 1 || cfg.BatchTimeout <= 0 || cfg.MaxReports < 1 {
		return nil, errors.New("reconciler: need overlap >= 0 and positive full-every, batch, batch timeout and max reports")
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Reconciler{store: store, cfg: cfg, now: now, verified: map[string]repository.LedgerBase{}, carry: newCarried()}, nil
}

// Run is the ledger-worker job: a full sweep when one is due, otherwise an
// incremental run.
func (r *Reconciler) Run(ctx context.Context) error {
	full := r.lastFull.IsZero() || r.now().Sub(r.lastFull) >= r.cfg.FullEvery
	_, err := r.Reconcile(ctx, full)
	return err
}

// Reconcile runs checks 1–4 once and logs the outcome: one ledger.reconcile
// failure per mismatch (up to MaxReports), then a summary.
func (r *Reconciler) Reconcile(ctx context.Context, full bool) (ReconcileReport, error) {
	log := logger.FromContext(ctx)
	start := r.now()
	mode := "incremental"
	if full {
		mode = "full"
	}
	p := &reconcilePass{r: r, rep: ReconcileReport{Full: full}, next: newCarried()}
	dbStart, err := r.store.Now(ctx)
	switch {
	case err != nil:
	case full:
		err = p.full(ctx)
	default:
		err = p.incremental(ctx, r.watermark.Add(-r.cfg.Overlap))
	}

	mism := p.rep.Mismatches
	for i, m := range mism {
		if i == r.cfg.MaxReports {
			break
		}
		attrs := []any{"event", "ledger.reconcile", "outcome", "failure", "reason", m.Reason, "mode", mode,
			"check", m.Check, "field", m.Field, "expected", m.Expected, "actual", m.Actual}
		for _, kv := range [][2]string{{"ledger_id", m.LedgerID}, {"transaction_id", m.TransactionID},
			{"account_id", m.AccountID}, {"currency", m.Currency}} {
			if kv[1] != "" {
				attrs = append(attrs, kv[0], kv[1])
			}
		}
		log.ErrorContext(ctx, "reconcile mismatch", attrs...)
	}

	durationMS := float64(r.now().Sub(start).Microseconds()) / 1000
	counts := []any{"mode", mode, "transactions", p.rep.Transactions, "ledgers", p.rep.Ledgers,
		"accounts", p.rep.Accounts, "mismatches", len(mism), "duration_ms", durationMS}
	if err != nil {
		// Keep the watermark, so the next run covers this one's range again,
		// and keep checking whatever was broken before.
		r.carry.merge(p.next)
		log.ErrorContext(ctx, "reconcile failed", append([]any{"event", "ledger.reconcile", "outcome", "failure",
			"reason", "pg_error", "error", err}, counts...)...)
		return p.rep, err
	}
	r.watermark, r.carry = dbStart, p.next
	if full {
		r.lastFull = start
	}
	if len(mism) > 0 {
		log.ErrorContext(ctx, "reconcile found mismatches", append([]any{"event", "ledger.reconcile", "outcome", "failure",
			"reason", "mismatches_found", "suppressed", max(0, len(mism)-r.cfg.MaxReports)}, counts...)...)
	} else {
		log.InfoContext(ctx, "reconcile clean", append([]any{"event", "ledger.reconcile", "outcome", "success"}, counts...)...)
	}
	return p.rep, nil
}

// reconcilePass is the state of one run.
type reconcilePass struct {
	r    *Reconciler
	rep  ReconcileReport
	next carried
}

func (p *reconcilePass) add(ms ...Mismatch) {
	for _, m := range ms {
		p.rep.Mismatches = append(p.rep.Mismatches, m)
		p.next.add(m)
	}
}

// full checks every transaction, USER ledger and account, plus the
// whole-journal checks, and starts every ledger again from its checkpoint.
func (p *reconcilePass) full(ctx context.Context) error {
	r := p.r
	if err := pages(ctx, func(after string) ([]string, error) {
		return r.store.ListTransactionIDs(ctx, time.Time{}, after, r.cfg.Batch)
	}, p.checkTransactions); err != nil {
		return err
	}
	clear(r.verified)
	if err := pages(ctx, func(after string) ([]string, error) {
		return r.store.ListUserLedgerIDs(ctx, after, r.cfg.Batch)
	}, p.checkLedgers); err != nil {
		return err
	}
	if err := pages(ctx, func(after string) ([]string, error) {
		return r.store.ListAccountIDs(ctx, time.Time{}, after, r.cfg.Batch)
	}, p.checkAccounts); err != nil {
		return err
	}
	return p.global(ctx)
}

// incremental checks the transactions and accounts updated after since, the
// USER ledgers those transactions post to, and whatever the last run found
// broken.
func (p *reconcilePass) incremental(ctx context.Context, since time.Time) error {
	r := p.r
	txs, err := collect(ctx, func(after string) ([]string, error) {
		return r.store.ListTransactionIDs(ctx, since, after, r.cfg.Batch)
	}, r.carry.transactions)
	if err != nil {
		return err
	}
	ledgers := maps.Clone(r.carry.ledgers)
	for batch := range slices.Chunk(txs, r.cfg.Batch) {
		if err := p.checkTransactions(ctx, batch); err != nil {
			return err
		}
		ids, err := r.store.LedgersOfTransactions(ctx, batch)
		if err != nil {
			return err
		}
		for _, id := range ids {
			ledgers[id] = struct{}{}
		}
	}
	for batch := range slices.Chunk(slices.Sorted(maps.Keys(ledgers)), r.cfg.Batch) {
		if err := p.checkLedgers(ctx, batch); err != nil {
			return err
		}
	}
	accounts, err := collect(ctx, func(after string) ([]string, error) {
		return r.store.ListAccountIDs(ctx, since, after, r.cfg.Batch)
	}, r.carry.accounts)
	if err != nil {
		return err
	}
	for batch := range slices.Chunk(accounts, r.cfg.Batch) {
		if err := p.checkAccounts(ctx, batch); err != nil {
			return err
		}
	}
	if r.carry.global {
		return p.global(ctx)
	}
	return nil
}

// global is check 3 over the whole journal: every currency sums to zero, and
// no line lacks a SETTLED or VOIDED transaction.
func (p *reconcilePass) global(ctx context.Context) error {
	r := p.r
	totals, err := r.store.UnbalancedCurrencies(ctx)
	if err != nil {
		return err
	}
	for _, t := range totals {
		p.add(Mismatch{Check: 3, Reason: reasonUnbalanced, Currency: t.Currency, Field: "currency_sum", Expected: "0", Actual: t.Total})
	}
	orphans, err := r.store.OrphanJournalTransactions(ctx, r.cfg.Batch)
	if err != nil {
		return err
	}
	if len(orphans) > 0 {
		return p.checkTransactions(ctx, orphans)
	}
	return nil
}

func (p *reconcilePass) checkTransactions(ctx context.Context, ids []string) error {
	ctx, cancel := context.WithTimeout(ctx, p.r.cfg.BatchTimeout)
	defer cancel()
	var checks []repository.TransactionCheck
	var events []repository.StoredEvent
	if err := p.r.store.Snapshot(ctx, func(q repository.ReconcileReader) error {
		var err error
		if checks, err = q.CheckTransactions(ctx, ids); err != nil {
			return err
		}
		events, err = q.StreamEvents(ctx, ledger.StreamTransaction, ids)
		return err
	}); err != nil {
		return err
	}
	byStream := groupEvents(events)
	for _, c := range checks {
		// Archived rows are proven by their export's manifest and read-back
		// (§7.8), not here: a SETTLED transaction whose month was archived has
		// no lines left, and a stream with archived events can't be folded.
		if !c.ArchivedLines {
			p.add(transactionJournalMismatches(c)...)
		}
		if c.Exists && !c.ArchivedEvents {
			p.add(transactionProjectionMismatches(c, byStream[c.ID])...)
		}
	}
	p.rep.Transactions += len(checks)
	return nil
}

func (p *reconcilePass) checkLedgers(ctx context.Context, ids []string) error {
	r := p.r
	ctx, cancel := context.WithTimeout(ctx, r.cfg.BatchTimeout)
	defer cancel()
	bases := make([]repository.LedgerBase, len(ids))
	for i, id := range ids {
		b, ok := r.verified[id]
		if !ok {
			b = repository.LedgerBase{LedgerID: id, Sequence: -1}
		}
		bases[i] = b
	}
	var checks []repository.LedgerCheck
	if err := r.store.Snapshot(ctx, func(q repository.ReconcileReader) error {
		var err error
		checks, err = q.CheckLedgers(ctx, bases)
		return err
	}); err != nil {
		return err
	}
	if len(r.verified) > maxVerified {
		clear(r.verified)
	}
	for _, c := range checks {
		ms := ledgerMismatches(c)
		p.add(ms...)
		if len(ms) == 0 {
			r.verified[c.LedgerID] = repository.LedgerBase{LedgerID: c.LedgerID, Sequence: c.LastSequence, Balance: c.Balance}
		} else {
			delete(r.verified, c.LedgerID)
		}
	}
	p.rep.Ledgers += len(checks)
	return nil
}

func (p *reconcilePass) checkAccounts(ctx context.Context, ids []string) error {
	ctx, cancel := context.WithTimeout(ctx, p.r.cfg.BatchTimeout)
	defer cancel()
	var checks []repository.AccountCheck
	var events []repository.StoredEvent
	if err := p.r.store.Snapshot(ctx, func(q repository.ReconcileReader) error {
		var err error
		if checks, err = q.CheckAccounts(ctx, ids); err != nil {
			return err
		}
		events, err = q.StreamEvents(ctx, ledger.StreamAccount, ids)
		return err
	}); err != nil {
		return err
	}
	byStream := groupEvents(events)
	for _, c := range checks {
		if !c.ArchivedEvents {
			p.add(accountProjectionMismatches(c, byStream[c.ID])...)
		}
	}
	p.rep.Accounts += len(checks)
	return nil
}

// --- the checks: pure functions over what the snapshot read ---

// ledgerMismatches is checks 1 and 2 for one USER ledger: the balance equals
// the base plus its later lines, those lines are numbered base+1..last with
// no gap or duplicate, and each balance_after is the running sum.
func ledgerMismatches(c repository.LedgerCheck) []Mismatch {
	var out []Mismatch
	m := func(check int, reason, field string, expected, actual any) {
		out = append(out, Mismatch{Check: check, Reason: reason, LedgerID: c.LedgerID, Field: field, Expected: expected, Actual: actual})
	}
	if want := c.BaseBalance + c.JournalSum; c.Balance != want {
		m(1, reasonBalanceMismatch, "balance", want, c.Balance)
	}
	wantLines := c.LastSequence - c.BaseSequence
	switch {
	case c.LastSequence < c.BaseSequence:
		m(2, reasonSequenceGap, "last_sequence", c.BaseSequence, c.LastSequence)
	case c.DistinctSeqs != c.Lines:
		m(2, reasonSequenceGap, "duplicate_sequences", int64(0), c.Lines-c.DistinctSeqs)
	case c.Lines != wantLines:
		m(2, reasonSequenceGap, "lines", wantLines, c.Lines)
	case c.Lines > 0 && (c.MinSeq != c.BaseSequence+1 || c.MaxSeq != c.LastSequence):
		m(2, reasonSequenceGap, "ledger_sequence", fmt.Sprintf("%d..%d", c.BaseSequence+1, c.LastSequence),
			fmt.Sprintf("%d..%d", c.MinSeq, c.MaxSeq))
	case c.NullSeq > 0:
		m(2, reasonSequenceGap, "ledger_sequence", int64(0), c.NullSeq) // lines without a sequence
	}
	if c.BadRunning > 0 {
		m(2, reasonBalanceMismatch, "balance_after", int64(0), c.BadRunning)
	}
	return out
}

// transactionJournalMismatches is check 3 for one transaction: a SETTLED or
// VOIDED one has exactly two lines in its currency, on its two ledgers,
// summing to zero; any other has none.
func transactionJournalMismatches(c repository.TransactionCheck) []Mismatch {
	m := func(field string, expected, actual any) []Mismatch {
		return []Mismatch{{Check: 3, Reason: reasonUnbalanced, TransactionID: c.ID, AccountID: c.AccountID,
			Currency: c.Currency, Field: field, Expected: expected, Actual: actual}}
	}
	if !c.Exists {
		return m("transaction", "SETTLED or VOIDED", "missing")
	}
	if c.Status != ledger.StatusSettled && c.Status != ledger.StatusVoided {
		if c.Lines != 0 {
			return m("lines", int64(0), c.Lines)
		}
		return nil
	}
	switch {
	case c.Lines != 2:
		return m("lines", int64(2), c.Lines)
	case c.JournalSum != 0:
		return m("sum", int64(0), c.JournalSum)
	case c.Currencies != 1 || c.WrongCurrency > 0:
		return m("currency", int64(0), c.WrongCurrency+c.Currencies-1)
	case c.WrongLedger > 0:
		return m("ledger_id", int64(0), c.WrongLedger)
	}
	return nil
}

// transactionProjectionMismatches is check 4 for one transaction: the
// projection row equals the fold of its event stream.
func transactionProjectionMismatches(c repository.TransactionCheck, stored []repository.StoredEvent) []Mismatch {
	m := func(field string, expected, actual any) []Mismatch {
		return []Mismatch{{Check: 4, Reason: reasonProjectionDrift, TransactionID: c.ID, AccountID: c.AccountID,
			Field: field, Expected: expected, Actual: actual}}
	}
	events, bad := decodeStream(stored)
	if bad != "" {
		return m("events", "decodable", bad)
	}
	if len(events) == 0 {
		return m("events", "TransactionRequested first", "none")
	}
	s, err := ledger.FoldTransaction(events)
	if err != nil {
		return m("events", "valid stream", foldError(err))
	}
	for _, f := range []struct {
		name      string
		want, got any
	}{
		{"status", string(s.Status), string(c.Status)},
		{"version", s.Version, c.Version},
		{"type", string(s.Type), string(c.Type)},
		{"amount", s.Amount, c.Amount},
		{"currency", s.Currency, c.Currency},
		{"account_id", s.AccountID, c.AccountID},
		{"ledger_id", s.LedgerID, c.LedgerID},
		{"failure_reason", string(s.FailureReason), string(c.FailureReason)},
		{"original_transaction_id", s.OriginalTransactionID, c.OriginalTransactionID},
		{"requested_by", s.RequestedBy, c.RequestedBy},
	} {
		if f.want != f.got {
			return m(f.name, f.want, f.got)
		}
	}
	return nil
}

// accountProjectionMismatches is check 4 for one account, including its set
// of USER ledgers.
func accountProjectionMismatches(c repository.AccountCheck, stored []repository.StoredEvent) []Mismatch {
	m := func(field string, expected, actual any) []Mismatch {
		return []Mismatch{{Check: 4, Reason: reasonProjectionDrift, AccountID: c.ID, Field: field, Expected: expected, Actual: actual}}
	}
	events, bad := decodeStream(stored)
	switch {
	case bad != "":
		return m("events", "decodable", bad)
	case !c.Exists && len(events) == 0:
		return nil // the id came from a stale carry; nothing to compare
	case len(events) == 0:
		return m("events", "AccountOpened first", "none")
	}
	s, err := ledger.FoldAccount(events)
	if err != nil {
		return m("events", "valid stream", foldError(err))
	}
	if !c.Exists {
		return m("account", "exists", "missing")
	}
	for _, f := range []struct {
		name      string
		want, got any
	}{
		{"status", string(s.Status), string(c.Status)},
		{"version", s.Version, c.Version},
		{"user_id", s.UserID, c.UserID},
		{"ledgers", ledgerSet(s.Ledgers), ledgerSet(c.Ledgers)},
	} {
		if f.want != f.got {
			return m(f.name, f.want, f.got)
		}
	}
	return nil
}

func ledgerSet(m map[string]string) string {
	parts := make([]string, 0, len(m))
	for _, cur := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, cur+":"+m[cur])
	}
	return strings.Join(parts, ",")
}

// decodeStream turns stored rows into events. bad names the first event
// type whose payload doesn't decode.
func decodeStream(stored []repository.StoredEvent) (events []ledger.Event, bad string) {
	events = make([]ledger.Event, 0, len(stored))
	for _, s := range stored {
		p, err := ledger.DecodePayload(s.Type, s.Payload)
		if err != nil {
			return nil, string(s.Type)
		}
		events = append(events, ledger.Event{StreamID: s.StreamID, StreamVersion: s.StreamVersion,
			AccountID: s.AccountID, Payload: p, OccurredAt: s.OccurredAt})
	}
	return events, ""
}

func foldError(err error) string {
	switch {
	case errors.Is(err, ledger.ErrVersionMismatch):
		return "version_gap"
	case errors.Is(err, ledger.ErrIllegalTransition):
		return "illegal_transition"
	default:
		return "invalid"
	}
}

func groupEvents(events []repository.StoredEvent) map[string][]repository.StoredEvent {
	out := map[string][]repository.StoredEvent{}
	for _, e := range events {
		out[e.StreamID] = append(out[e.StreamID], e)
	}
	return out
}

// pages calls fn with each page list returns, until an empty one.
func pages(ctx context.Context, list func(after string) ([]string, error), fn func(context.Context, []string) error) error {
	after := ""
	for {
		ids, err := list(after)
		if err != nil {
			return err
		}
		if len(ids) > 0 {
			if err := fn(ctx, ids); err != nil {
				return err
			}
			after = ids[len(ids)-1]
		}
		if len(ids) == 0 || ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

// collect gathers every page into a sorted, de-duplicated slice, plus extra
// (which may be nil).
func collect(ctx context.Context, list func(after string) ([]string, error), extra map[string]struct{}) ([]string, error) {
	set := make(map[string]struct{}, len(extra))
	maps.Copy(set, extra)
	if err := pages(ctx, list, func(_ context.Context, ids []string) error {
		for _, id := range ids {
			set[id] = struct{}{}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(set)), nil
}
