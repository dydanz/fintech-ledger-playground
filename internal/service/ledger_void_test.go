//go:build integration

package service_test

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"fintech-ledger-playground/internal/idempotency"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/service"
)

func (e *svcEnv) void(key, txID, reason string) (service.Response, error) {
	return e.svc.Void(e.ctx, service.VoidCommand{Request: req("ops", key), TransactionID: txID, Reason: reason})
}

func (e *svcEnv) mustSettle(r service.Response, err error) service.TransactionView {
	e.t.Helper()
	if err != nil || r.Status != http.StatusCreated {
		e.t.Fatalf("want 201: %d %s %v", r.Status, r.Body, err)
	}
	return decode[txBody](e.t, r).Transaction
}

// assertNegates checks the void's journal lines cancel the original's on
// every ledger (TRD §4.3), and that every ledger balance equals its lines.
func (e *svcEnv) assertNegates(origID, voidID string) {
	e.t.Helper()
	if n := e.count(`SELECT count(*) FROM (SELECT ledger_id FROM ledger.journal_entries WHERE transaction_id IN ($1, $2)
		GROUP BY ledger_id HAVING sum(amount) <> 0 OR count(*) <> 2) x`, origID, voidID); n != 0 {
		e.t.Fatalf("void %s does not negate %s on %d ledger(s)", voidID, origID, n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.journal_entries WHERE transaction_id = $1`, voidID); n != 2 {
		e.t.Fatalf("void has %d journal lines, want 2", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.ledgers l WHERE kind = 'USER'
		AND l.balance <> (SELECT coalesce(sum(amount), 0) FROM ledger.journal_entries j WHERE j.ledger_id = l.id)`); n != 0 {
		e.t.Fatalf("%d user ledger(s) whose balance differs from their journal", n)
	}
}

func TestVoidOfCreditAndOfDebit(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("void@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 1000, "IDR"))
	debit := e.mustSettle(e.money(e.svc.Debit, "d1", acct, 300, "IDR"))

	e.resetLogs()
	v := e.mustSettle(e.void("v-d1", debit.ID, "duplicate charge"))
	if v.Type != "VOID" || v.Status != "SETTLED" || v.OriginalID != debit.ID || *v.BalanceAfter != 1000 ||
		v.Amount != 300 || v.AccountID != acct || v.Reference != "duplicate charge" {
		t.Fatalf("void of debit = %+v", v)
	}
	rec := wantEvent(t, e.logs, "ledger.void", map[string]any{"outcome": "success", "original_transaction_id": debit.ID,
		"transaction_id": v.ID, "client_id": "ops", "level": "info"})
	if strings.Contains(e.logs.String(), "duplicate charge") {
		t.Fatalf("void reason logged: %v", rec)
	}
	e.assertNegates(debit.ID, v.ID)

	orig, err := e.svc.Transaction(e.ctx, debit.ID)
	if err != nil || orig.Status != "VOIDED" || *orig.BalanceAfter != 700 {
		t.Fatalf("original after void: %+v %v", orig, err)
	}
	// The original's stream records the void; its outbox row carries the trace.
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND stream_version = 4
		AND event_type = 'TransactionVoided' AND payload->>'void_transaction_id' = $2::text AND trace_id = $3`,
		debit.ID, v.ID, testTrace); n != 1 {
		t.Fatalf("TransactionVoided on the original's stream: %d", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.outbox WHERE headers->>'event_type' = 'TransactionVoided'`); n != 1 {
		t.Fatalf("TransactionVoided outbox rows: %d", n)
	}

	vc := e.mustSettle(e.void("v-c1", credit.ID, "fraudulent top-up"))
	if *vc.BalanceAfter != 0 {
		t.Fatalf("void of credit balance_after = %d", *vc.BalanceAfter)
	}
	e.assertNegates(credit.ID, vc.ID)
}

func TestVoidRejections(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("vrej@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 100, "IDR"))
	v := e.mustSettle(e.void("v1", credit.ID, "r"))

	// Replay returns the same bytes; the same key with another reason is 422.
	first, _ := e.void("v1", credit.ID, "r")
	again, err := e.void("v1", credit.ID, "r")
	if err != nil || !again.Replayed || !bytes.Equal(first.Body, again.Body) {
		t.Fatalf("replay: %s %v", again.Body, err)
	}
	if _, err := e.void("v1", credit.ID, "other"); !errors.Is(err, service.ErrIdempotencyKeyReused) {
		t.Fatalf("key reuse: %v", err)
	}

	// A second void of a voided original is denied before any transaction.
	e.resetLogs()
	if _, err := e.void("v2", credit.ID, "r"); !errors.Is(err, service.ErrNotVoidable) {
		t.Fatalf("second void: %v", err)
	}
	wantEvent(t, e.logs, "ledger.void", map[string]any{"outcome": "denied", "reason": "not_voidable"})
	if n := e.count(`SELECT count(*) FROM ledger.transactions WHERE original_transaction_id = $1`, credit.ID); n != 1 {
		t.Fatalf("%d voids of the original, want 1", n)
	}

	// A void of a void, and of a FAILED transaction: FAILED not_voidable.
	nsf, _ := e.money(e.svc.Debit, "nsf", acct, 10_000, "IDR")
	for name, id := range map[string]string{"void of a void": v.ID, "void of a failed debit": decode[errBody](t, nsf).Error.TransactionID} {
		e.resetLogs()
		r, err := e.void("vv-"+id, id, "r")
		if err != nil || r.Status != http.StatusUnprocessableEntity || decode[errBody](t, r).Error.Code != "not_voidable" ||
			decode[errBody](t, r).Error.TransactionID == "" {
			t.Errorf("%s: %d %s %v", name, r.Status, r.Body, err)
		}
		wantEvent(t, e.logs, "ledger.void", map[string]any{"outcome": "denied", "reason": "not_voidable"})
	}

	for name, c := range map[string]service.VoidCommand{
		"unknown original":  {Request: req("ops", "x1"), TransactionID: "0192f3c4-0000-7000-8000-00000000dead", Reason: "r"},
		"malformed id":      {Request: req("ops", "x2"), TransactionID: "nope", Reason: "r"},
		"no reason":         {Request: req("ops", "x3"), TransactionID: credit.ID},
		"reason too long":   {Request: req("ops", "x4"), TransactionID: credit.ID, Reason: strings.Repeat("r", 65)},
		"no idempotent key": {Request: req("ops", ""), TransactionID: credit.ID, Reason: "r"},
	} {
		var ve *service.ValidationError
		_, err := e.svc.Void(e.ctx, c)
		if !errors.Is(err, service.ErrTransactionNotFound) && !errors.As(err, &ve) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}

func TestVoidRespectsTheFloorAndIgnoresFreeze(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("floor@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 100, "IDR"))
	debit := e.mustSettle(e.money(e.svc.Debit, "d1", acct, 80, "IDR"))

	// Q3: the top-up is already spent, so voiding it would go negative.
	r, err := e.void("v-c1", credit.ID, "fraud")
	if err != nil || r.Status != http.StatusUnprocessableEntity || decode[errBody](t, r).Error.Code != "insufficient_funds" {
		t.Fatalf("void of spent credit: %d %s %v", r.Status, r.Body, err)
	}
	if o, _ := e.svc.Transaction(e.ctx, credit.ID); o.Status != "SETTLED" {
		t.Fatalf("original after failed void: %s", o.Status)
	}

	// Q3: voids are allowed on a frozen account. Voiding the debit first
	// restores the funds, and the credit's void (a FAILED void doesn't block
	// a new one) then settles.
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.accounts SET status = 'FROZEN' WHERE id = $1`, acct); err != nil {
		t.Fatal(err)
	}
	e.mustSettle(e.void("v-d1", debit.ID, "refund"))
	if v := e.mustSettle(e.void("v-c1-again", credit.ID, "fraud")); *v.BalanceAfter != 0 {
		t.Fatalf("balance after both voids = %d", *v.BalanceAfter)
	}
}

func TestVoidDeferredToProcess(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("vpend@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 100, "IDR"))
	e.store.failInTx(2, repository.ErrTimeout)
	r, err := e.void("v-pend", credit.ID, "r")
	if err != nil || r.Status != http.StatusAccepted || decode[txBody](t, r).Transaction.OriginalID != credit.ID {
		t.Fatalf("want 202 with original id: %d %s %v", r.Status, r.Body, err)
	}
	pending := decode[txBody](t, r).Transaction.ID
	done, err := e.svc.Process(e.ctx, service.ProcessInput{TransactionID: pending, ClientID: "ops",
		KeyHash: idempotency.HashKey("v-pend"), RequestID: "sweeper"})
	if err != nil || done.Status != http.StatusCreated {
		t.Fatalf("process: %d %s %v", done.Status, done.Body, err)
	}
	if o, _ := e.svc.Transaction(e.ctx, credit.ID); o.Status != "VOIDED" {
		t.Fatalf("original = %s", o.Status)
	}
}

// TRD §13.3: concurrent voids of one original settle exactly once.
func TestConcurrentVoidsSettleOnce(t *testing.T) {
	e := newSvcEnv(t, 5*time.Second)
	acct := e.openAccount("vrace@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 500, "IDR"))

	const n = 10
	var wg sync.WaitGroup
	results := make([]string, n)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.void(fmt.Sprintf("race-%d", i), credit.ID, "r")
			switch {
			case err == nil && r.Status == http.StatusCreated:
				results[i] = "settled"
			case errors.Is(err, service.ErrNotVoidable),
				err == nil && r.Status == http.StatusUnprocessableEntity && decode[errBody](t, r).Error.Code == "not_voidable":
				results[i] = "not_voidable"
			default:
				results[i] = fmt.Sprintf("unexpected %d %s %v", r.Status, r.Body, err)
			}
		}()
	}
	wg.Wait()
	settled := 0
	for _, r := range results {
		if r == "settled" {
			settled++
		} else if r != "not_voidable" {
			t.Errorf("void: %s", r)
		}
	}
	if settled != 1 {
		t.Fatalf("%d voids settled, want exactly 1: %v", settled, results)
	}
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND event_type = 'TransactionVoided'`, credit.ID); n != 1 {
		t.Fatalf("%d TransactionVoided events, want 1", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.ledgers WHERE account_id = $1 AND balance = 0`, acct); n != 1 {
		t.Fatal("balance is not 0 after one void")
	}
}
