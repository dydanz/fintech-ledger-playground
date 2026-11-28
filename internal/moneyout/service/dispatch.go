package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/bmwclient"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/routing"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

// BankClient is the signed call to a bank middleware (bmwclient.Client).
type BankClient interface {
	Transfer(ctx context.Context, bmw string, req contract.TransferRequest) bmwclient.Outcome
	Inquire(ctx context.Context, bmw, transferID, requestID string) bmwclient.Outcome
}

type DispatchConfig struct {
	InquiryInterval time.Duration // between status inquiries of an open attempt (P§3.3)
	InquiryFor      time.Duration // after this, still not final: NEEDS_MANUAL_REVIEW
	StaleDispatch   time.Duration // a DISPATCHING attempt this old was interrupted: inquire it
	RetryBase       time.Duration // same-node retry backoff: base × 2^n, with jitter
	RetryMax        time.Duration
	Concurrency     int // calls in flight per job run
	Batch           int
}

// Dispatch sends attempts, inquires open ones and applies bank events
// (P§3.2, §3.3; spec/money-out TRD §6.2, §6.3). Every answer, whichever way
// it came, goes through apply, under the transfer's row lock.
type Dispatch struct {
	store  *repository.Store
	routes RouteSource
	bank   BankClient
	cfg    DispatchConfig
	now    func() time.Time
}

func NewDispatch(store *repository.Store, routes RouteSource, bank BankClient, cfg DispatchConfig) *Dispatch {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 100
	}
	return &Dispatch{store: store, routes: routes, bank: bank, cfg: cfg, now: time.Now}
}

type source string

const (
	fromDispatch source = "dispatch"
	fromInquiry  source = "inquiry"
	fromEvent    source = "event"
)

// report is one answer about one attempt.
type report struct {
	res        contract.BankResult
	noSameNode bool
	src        source
	eventID    string // for bank events: deduplicated, and journaled
}

// note is a log line decided inside a transaction, written after it commits.
type note func(ctx context.Context)

// RunDispatch sends the next attempt of every transfer waiting for one.
func (d *Dispatch) RunDispatch(ctx context.Context) error {
	ids, err := d.store.ListDispatchable(ctx, d.now(), d.cfg.Batch)
	if err != nil {
		return d.pgError(ctx, "transfer.dispatch", "", err)
	}
	var g errgroup.Group // no shared cancel: one failure doesn't abandon calls in flight
	g.SetLimit(d.cfg.Concurrency)
	for _, id := range ids {
		g.Go(func() error { return d.dispatchOne(ctx, id) })
	}
	return g.Wait()
}

func (d *Dispatch) pgError(ctx context.Context, event, id string, err error) error {
	logger.FromContext(ctx).ErrorContext(ctx, "money-out job failed", "event", event, "outcome", "failure", "reason", "pg_error",
		"transfer_id", id, "error", err)
	return err
}

func candidates(t repository.Transfer) []routing.Candidate {
	var r routing.Result
	_ = json.Unmarshal(t.Routing, &r)
	return r.Candidates
}

// dispatchOne claims a transfer, records the attempt as DISPATCHING, and
// only after that commits calls the bank middleware (P§3.5).
func (d *Dispatch) dispatchOne(ctx context.Context, id string) error {
	var (
		t       repository.Transfer
		a       repository.Attempt
		changes []change
		started bool
	)
	err := d.store.InTx(ctx, func(tx *repository.Tx) error {
		var ok bool
		var err error
		t, ok, err = tx.TryLockTransfer(ctx, id)
		if err != nil || !ok || (t.State != transfer.Routed && t.State != transfer.AttemptFailed) {
			return err
		}
		now := d.now()
		if t.NextDispatchAt != nil && t.NextDispatchAt.After(now) {
			return nil
		}
		cands := candidates(t)
		if t.CandidateIndex >= len(cands) { // nothing left to try
			c, err := finish(ctx, tx, &t, now, contract.TransferResult{Status: contract.StatusFailed, Reason: contract.ReasonRailUnavailable,
				Attempts: t.CurrentAttempt})
			changes = append(changes, c)
			return err
		}
		cand := cands[t.CandidateIndex]
		next, deadline := now.Add(d.cfg.StaleDispatch), now.Add(d.cfg.InquiryFor)
		a = repository.Attempt{TransferID: t.ID, AttemptNo: t.CurrentAttempt + 1, RequestID: uuid.Must(uuid.NewV7()).String(),
			BankMiddleware: cand.BankMiddleware, Rail: cand.Rail, Status: "DISPATCHING", NextInquiryAt: &next, InquiryDeadline: &deadline}
		if err := tx.InsertAttempt(ctx, a); err != nil {
			return err
		}
		if err := tx.UpdateAttempt(ctx, a); err != nil {
			return err
		}
		if err := tx.SetCurrentAttempt(ctx, t.ID, a.AttemptNo); err != nil {
			return err
		}
		t.CurrentAttempt = a.AttemptNo
		detail, _ := json.Marshal(map[string]any{"bank_middleware": a.BankMiddleware, "rail": a.Rail, "request_id": a.RequestID,
			"same_node_retry": t.SameNodeRetries})
		c, err := transition(ctx, tx, &t, transfer.Dispatching, contract.EventAttemptDispatching, now, contract.JournalPayload{AttemptNo: a.AttemptNo, Detail: detail})
		changes = append(changes, c)
		started = err == nil
		return err
	})
	if err != nil {
		return d.pgError(ctx, "transfer.dispatch", id, err)
	}
	logChanges(ctx, changes)
	if !started {
		return nil
	}
	req := contract.TransferRequest{RequestID: a.RequestID, TransferID: t.ID, AttemptNo: a.AttemptNo, Rail: a.Rail,
		Amount: contract.AmountOf(ledger.Money{Amount: t.Amount, Currency: t.Currency}), Beneficiary: contract.Beneficiary{BankCode: t.BankCode, AccountNo: t.AccountNo}}
	out := d.bank.Transfer(ctx, a.BankMiddleware, req)
	logCall(ctx, "transfer.dispatch", a, out)
	if ctx.Err() != nil { // stopping: the attempt may have been sent; the inquiry job resolves it
		return nil
	}
	return d.applyReport(ctx, t.ID, a.AttemptNo, report{res: out.Result, noSameNode: out.NoSameNode, src: fromDispatch})
}

func logCall(ctx context.Context, event string, a repository.Attempt, out bmwclient.Outcome) {
	attrs := []any{"event", event, "transfer_id", a.TransferID, "request_id", a.RequestID, "attempt_no", a.AttemptNo,
		"bank_middleware", a.BankMiddleware, "rail", a.Rail, "status", string(out.Result.Status), "result_reason", string(out.Result.Reason),
		"reached_bank", out.Result.ReachedBank, "http_status", out.HTTPStatus, "duration_ms", out.Duration.Milliseconds()}
	log := logger.FromContext(ctx)
	if out.Reason == "ok" {
		log.InfoContext(ctx, "bank middleware answered", append(attrs, "outcome", "success")...)
		return
	}
	log.WarnContext(ctx, "bank middleware call failed", append(attrs, "outcome", "failure", "reason", out.Reason)...)
}

// RunInquiries asks about every open attempt whose inquiry is due, and
// parks those past their deadline (P§3.3). It never consults a breaker.
func (d *Dispatch) RunInquiries(ctx context.Context) error {
	refs, err := d.store.ListInquiriesDue(ctx, d.now(), d.cfg.Batch)
	if err != nil {
		return d.pgError(ctx, "transfer.inquiry", "", err)
	}
	var g errgroup.Group
	g.SetLimit(d.cfg.Concurrency)
	for _, ref := range refs {
		g.Go(func() error { return d.inquireOne(ctx, ref) })
	}
	return g.Wait()
}

func (d *Dispatch) inquireOne(ctx context.Context, ref repository.AttemptRef) error {
	var (
		a       repository.Attempt
		changes []change
		ask     bool
	)
	err := d.store.InTx(ctx, func(tx *repository.Tx) error {
		t, ok, err := tx.TryLockTransfer(ctx, ref.TransferID)
		if err != nil || !ok || t.CurrentAttempt != ref.AttemptNo || !t.State.AttemptOpen() {
			return err
		}
		if a, err = tx.GetAttempt(ctx, t.ID, ref.AttemptNo); err != nil {
			return err
		}
		now := d.now()
		if !a.Open() || a.NextInquiryAt == nil || a.NextInquiryAt.After(now) {
			return nil
		}
		if a.InquiryDeadline != nil && now.After(*a.InquiryDeadline) {
			a.NextInquiryAt = nil
			if err := tx.UpdateAttempt(ctx, a); err != nil {
				return err
			}
			c, err := park(ctx, tx, &t, now, "inquiry_deadline", a.AttemptNo)
			changes = append(changes, c)
			return err
		}
		claim := now.Add(d.cfg.StaleDispatch) // a second inquirer skips it while this call is out
		a.NextInquiryAt = &claim
		ask = true
		return tx.UpdateAttempt(ctx, a)
	})
	if err != nil {
		return d.pgError(ctx, "transfer.inquiry", ref.TransferID, err)
	}
	if len(changes) > 0 {
		logger.FromContext(ctx).WarnContext(ctx, "transfer parked for manual review", "event", "transfer.review", "outcome", "success",
			"reason", "inquiry_deadline", "transfer_id", ref.TransferID, "attempt_no", ref.AttemptNo, "bank_middleware", a.BankMiddleware)
		logChanges(ctx, changes)
	}
	if !ask {
		return nil
	}
	out := d.bank.Inquire(ctx, a.BankMiddleware, a.TransferID, a.RequestID)
	logCall(ctx, "transfer.inquiry", a, out)
	if ctx.Err() != nil {
		return nil
	}
	res := out.Result
	if out.Reason != "ok" { // the middleware didn't answer: that says nothing about the bank
		res = bankmwUnknown
	}
	return d.applyReport(ctx, a.TransferID, a.AttemptNo, report{res: res, src: fromInquiry})
}

var bankmwUnknown = contract.BankResult{Status: contract.StatusUnknown, ReachedBank: true}

// park moves an open transfer to NEEDS_MANUAL_REVIEW, nothing to the core.
func park(ctx context.Context, tx *repository.Tx, t *repository.Transfer, at time.Time, why string, attemptNo int) (change, error) {
	if err := tx.SetReviewReason(ctx, t.ID, why); err != nil {
		return change{}, err
	}
	detail, _ := json.Marshal(map[string]any{"reason": why})
	c, err := transition(ctx, tx, t, transfer.NeedsManualReview, contract.EventTransferNeedsReview, at, contract.JournalPayload{AttemptNo: attemptNo, Detail: detail})
	t.ReviewReason = why
	return c, err
}

// record journals ev without a state change.
func record(ctx context.Context, tx *repository.Tx, t *repository.Transfer, eventType string, at time.Time, p contract.JournalPayload) error {
	p.TransferID, p.FromState, p.ToState, p.CoreTransactionID = t.ID, string(t.State), string(t.State), t.CoreTransactionID
	ev, err := contract.NewEvent(eventType, contract.StreamTransfer, t.ID, t.Version+1, at, t.TraceID, p)
	if err != nil {
		return err
	}
	v, err := tx.Record(ctx, t.ID, t.Version, ev)
	t.Version = v
	return err
}

func (d *Dispatch) applyReport(ctx context.Context, transferID string, attemptNo int, rep report) error {
	var (
		changes []change
		notes   []note
	)
	err := d.store.InTx(ctx, func(tx *repository.Tx) error {
		t, err := tx.LockTransfer(ctx, transferID)
		if err != nil {
			return err
		}
		a, err := tx.GetAttempt(ctx, transferID, attemptNo)
		if err != nil {
			return err
		}
		if rep.eventID != "" {
			first, err := tx.MarkProcessed(ctx, rep.eventID, contract.TopicBankEvents)
			if err != nil || !first {
				return err
			}
			detail, _ := json.Marshal(map[string]any{"request_id": a.RequestID, "bank_middleware": a.BankMiddleware, "result": rep.res,
				"current_attempt": t.CurrentAttempt})
			if err := record(ctx, tx, &t, contract.EventBankStatusReceived, d.now(), contract.JournalPayload{AttemptNo: a.AttemptNo, Detail: detail}); err != nil {
				return err
			}
		}
		changes, notes, err = d.apply(ctx, tx, &t, &a, rep)
		return err
	})
	if err != nil {
		return d.pgError(ctx, "transfer.dispatch", transferID, err)
	}
	logChanges(ctx, changes)
	for _, n := range notes {
		n(ctx)
	}
	return nil
}

func terminal(s contract.Status) bool {
	return s == contract.StatusSucceeded || s == contract.StatusFailed
}

// apply decides what one answer means for the transfer (P§3.2, §3.3).
func (d *Dispatch) apply(ctx context.Context, tx *repository.Tx, t *repository.Transfer, a *repository.Attempt, rep report) ([]change, []note, error) {
	now, res := d.now(), rep.res
	if res.Status == contract.StatusUnknown {
		res.ReachedBank = true
	}
	var changes []change
	add := func(c change, err error) error {
		changes = append(changes, c)
		return err
	}

	// An attempt MOM already gave up on, or one that's no longer current.
	if a.AttemptNo != t.CurrentAttempt || !a.Open() {
		if a.Status == "SUCCEEDED" || !(res.Status == contract.StatusSucceeded || res.Status == contract.StatusProcessing || res.Status == contract.StatusAccepted) {
			return nil, nil, nil // a repeat, or a failure for a failed attempt: recorded only
		}
		// Money may be moving on an attempt MOM treated as failed: a
		// possible double payment (Review Focus 1). Never finish it.
		parked := false
		switch {
		case t.State == transfer.NeedsManualReview:
			if err := tx.SetReviewReason(ctx, t.ID, "late_event"); err != nil {
				return nil, nil, err
			}
		case !t.State.Terminal():
			if err := add(park(ctx, tx, t, now, "late_event", a.AttemptNo)); err != nil {
				return nil, nil, err
			}
			parked = true
		}
		state := t.State
		return changes, []note{func(ctx context.Context) {
			logger.FromContext(ctx).ErrorContext(ctx, "bank reports money moving on an attempt MOM had given up on", "event", "transfer.late_event",
				"outcome", "failure", "reason", "possible_double_payment", "transfer_id", t.ID, "attempt_no", a.AttemptNo,
				"current_attempt", t.CurrentAttempt, "bank_middleware", a.BankMiddleware, "status", string(res.Status), "state", string(state), "parked", parked)
		}}, nil
	}
	if t.State == transfer.NeedsManualReview && (t.ReviewReason == "late_event" || !terminal(res.Status)) {
		return nil, nil, nil // a possible double payment waits for ops; a parked transfer moves only on a final answer (Q4)
	}
	if rep.src == fromDispatch && a.Status != "DISPATCHING" && !terminal(res.Status) {
		return nil, nil, nil // a bank event got here first; the synchronous answer is older
	}
	a.Reason, a.RetryElsewhere, a.ReachedBank, a.BankRawCode = string(res.Reason), res.RetryElsewhere, res.ReachedBank, res.BankRawCode
	if res.BankReference != "" {
		a.BankReference = res.BankReference
	}
	detail, _ := json.Marshal(map[string]any{"request_id": a.RequestID, "bank_middleware": a.BankMiddleware, "rail": a.Rail, "source": rep.src,
		"status": res.Status, "reason": res.Reason, "retry_elsewhere": res.RetryElsewhere, "reached_bank": res.ReachedBank,
		"bank_reference": res.BankReference, "bank_raw_code": res.BankRawCode})
	jp := contract.JournalPayload{AttemptNo: a.AttemptNo, Detail: detail}
	schedule := func() {
		if rep.src != fromEvent || a.NextInquiryAt == nil {
			next := now.Add(d.cfg.InquiryInterval)
			a.NextInquiryAt = &next
		}
	}
	result := contract.TransferResult{BankMiddleware: a.BankMiddleware, Rail: a.Rail, BankReference: a.BankReference, Attempts: t.CurrentAttempt}

	switch res.Status {
	case contract.StatusSucceeded:
		a.Status, a.NextInquiryAt = "SUCCEEDED", nil
		if err := tx.UpdateAttempt(ctx, *a); err != nil {
			return nil, nil, err
		}
		result.Status = contract.StatusSucceeded
		return changes, nil, add(finish(ctx, tx, t, now, result))

	case contract.StatusFailed:
		a.Status, a.NextInquiryAt = "FAILED", nil
		if err := tx.UpdateAttempt(ctx, *a); err != nil {
			return nil, nil, err
		}
		result.Status, result.Reason = contract.StatusFailed, res.Reason
		if t.State == transfer.NeedsManualReview { // the parked attempt's answer: done, no failover (Q4)
			return changes, nil, add(finish(ctx, tx, t, now, result))
		}
		if err := add(transition(ctx, tx, t, transfer.AttemptFailed, contract.EventAttemptFailed, now, jp)); err != nil {
			return nil, nil, err
		}
		retries := d.routes.Chain().Config().SameNodeRetries(a.BankMiddleware)
		switch {
		case !res.ReachedBank && !rep.noSameNode && t.SameNodeRetries < retries: // provably not sent: again, here, later
			at := now.Add(d.backoff(t.SameNodeRetries))
			return changes, nil, tx.SetDispatchPlan(ctx, t.ID, t.CandidateIndex, t.SameNodeRetries+1, &at)
		case res.RetryElsewhere && t.CandidateIndex+1 < len(candidates(*t)): // definitive, and allowed elsewhere: failover
			return changes, nil, tx.SetDispatchPlan(ctx, t.ID, t.CandidateIndex+1, 0, nil)
		}
		return changes, nil, add(finish(ctx, tx, t, now, result))

	case contract.StatusAccepted:
		if a.Status != "DISPATCHING" {
			return nil, nil, nil // never back from PROCESSING or UNKNOWN
		}
		a.Status = "ACCEPTED"
		schedule()
		if err := tx.UpdateAttempt(ctx, *a); err != nil {
			return nil, nil, err
		}
		return changes, nil, add(transition(ctx, tx, t, transfer.Accepted, contract.EventAttemptAccepted, now, jp))

	case contract.StatusProcessing, contract.StatusUnknown:
		to, status, ev := transfer.Processing, "PROCESSING", contract.EventAttemptProcessing
		if res.Status == contract.StatusUnknown {
			to, status, ev = transfer.Unknown, "UNKNOWN", contract.EventAttemptUnknown
		}
		a.Status = status
		schedule()
		if err := tx.UpdateAttempt(ctx, *a); err != nil {
			return nil, nil, err
		}
		if t.State == to {
			return changes, nil, nil
		}
		return changes, nil, add(transition(ctx, tx, t, to, ev, now, jp))
	}
	return nil, nil, fmt.Errorf("apply: status %q", res.Status)
}

// backoff is base × 2^n, at most RetryMax, ±20 % jitter.
func (d *Dispatch) backoff(n int) time.Duration {
	b := d.cfg.RetryBase << n
	if b <= 0 || b > d.cfg.RetryMax {
		b = d.cfg.RetryMax
	}
	return time.Duration(float64(b) * (0.8 + 0.4*rand.Float64()))
}

var requestIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// ReceiveBankEvent applies one BankStatusReported from
// money-out.bank-events.v1. Its error is only for failures worth retrying
// (the database); an event that can't apply is logged and skipped.
func (d *Dispatch) ReceiveBankEvent(ctx context.Context, e contract.Envelope) error {
	log := logger.FromContext(ctx)
	deny := func(reason string, attrs ...any) error {
		log.WarnContext(ctx, "bank event refused", append([]any{"event", "transfer.bank_event", "outcome", "denied", "reason", reason,
			"event_id", e.EventID}, attrs...)...)
		return nil
	}
	var p contract.BankStatusReported
	if e.EventType != contract.EventBankStatusReported || e.DecodePayload(&p) != nil || p.Result.Validate() != nil ||
		!requestIDPattern.MatchString(p.RequestID) || !requestIDPattern.MatchString(e.EventID) {
		return deny("invalid_input")
	}
	a, err := d.store.AttemptByRequestID(ctx, p.RequestID)
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return deny("unknown_attempt", "transfer_id", p.TransferID, "request_id", p.RequestID)
	case err != nil:
		return err
	case a.TransferID != p.TransferID:
		return deny("unknown_attempt", "transfer_id", p.TransferID, "request_id", p.RequestID)
	}
	if err := d.applyReport(ctx, a.TransferID, a.AttemptNo, report{res: p.Result, src: fromEvent, eventID: e.EventID}); err != nil {
		return err
	}
	log.InfoContext(ctx, "bank event received", "event", "transfer.bank_event", "outcome", "success", "event_id", e.EventID,
		"transfer_id", a.TransferID, "request_id", a.RequestID, "attempt_no", a.AttemptNo, "bank_middleware", a.BankMiddleware,
		"status", string(p.Result.Status))
	return nil
}
