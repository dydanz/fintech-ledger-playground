//go:build integration

package repository_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/transfer"
	"fintech-ledger-playground/internal/testutil/pgtest"
)

type env struct {
	t     *testing.T
	ctx   context.Context
	store *repository.Store
}

func newEnv(t *testing.T) *env {
	t.Helper()
	return &env{t: t, ctx: context.Background(), store: repository.NewStore(pgtest.NewFrom(t, "migrations/money_out"))}
}

func newTransfer() repository.Transfer {
	return repository.Transfer{ID: uuid.NewString(), PayloadHash: "h1", Amount: 2500000, Currency: "IDR",
		BankCode: "014", AccountNo: "0201245681", State: transfer.Received, TraceID: "t", RequestedAt: time.Now().UTC()}
}

// event builds the journal event for version v of t.
func event(t *testing.T, id string, v int64, from, to transfer.State) contract.Envelope {
	t.Helper()
	e, err := contract.NewEvent(contract.EventTransferReceived, contract.StreamTransfer, id, v, time.Now(), "t",
		contract.JournalPayload{TransferID: id, FromState: string(from), ToState: string(to)})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) insert(tr repository.Transfer) bool {
	e.t.Helper()
	var created bool
	if err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		var err error
		created, err = tx.InsertTransfer(e.ctx, tr, event(e.t, tr.ID, 1, "", transfer.Received))
		return err
	}); err != nil {
		e.t.Fatal(err)
	}
	return created
}

func (e *env) outboxCount() int {
	e.t.Helper()
	var n int
	if err := e.store.DB().QueryRow(e.ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func TestInsertTransferIsUnique(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	if !e.insert(tr) {
		t.Fatal("first insert not created")
	}
	if e.insert(tr) {
		t.Fatal("second insert of the same transfer_id created a row")
	}
	if n := e.outboxCount(); n != 1 {
		t.Fatalf("outbox rows = %d, want 1 (the second insert journals nothing)", n)
	}
	got, _, err := e.store.GetTransfer(e.ctx, tr.ID)
	if err != nil || got.State != transfer.Received || got.Version != 1 || got.Amount != 2500000 {
		t.Fatalf("transfer = %+v, %v", got, err)
	}
}

// P§3.2: concurrent workers can't both move the same version on.
func TestCompareAndSetRejectsStaleVersion(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	e.insert(tr)
	move := func() error {
		return e.store.InTx(e.ctx, func(tx *repository.Tx) error {
			_, err := tx.Transition(e.ctx, tr.ID, transfer.Received, 1, transfer.Routed, event(t, tr.ID, 2, transfer.Received, transfer.Routed))
			return err
		})
	}
	if err := move(); err != nil {
		t.Fatal(err)
	}
	if err := move(); !errors.Is(err, repository.ErrVersionConflict) {
		t.Fatalf("second move from version 1: %v, want ErrVersionConflict", err)
	}
	got, _, _ := e.store.GetTransfer(e.ctx, tr.ID)
	if got.State != transfer.Routed || got.Version != 2 {
		t.Fatalf("transfer = %s v%d", got.State, got.Version)
	}
}

// The version, not just the state, decides: after a Record (a rejected
// duplicate) the state is still RECEIVED but the version moved on, and a
// worker holding version 1 must lose.
func TestCompareAndSetChecksVersionNotJustState(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	e.insert(tr)
	if err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		_, err := tx.Record(e.ctx, tr.ID, 1, event(t, tr.ID, 2, transfer.Received, transfer.Received))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		_, err := tx.Transition(e.ctx, tr.ID, transfer.Received, 1, transfer.Routed, event(t, tr.ID, 2, transfer.Received, transfer.Routed))
		return err
	})
	if !errors.Is(err, repository.ErrVersionConflict) {
		t.Fatalf("transition from stale version 1 in the same state: %v, want ErrVersionConflict", err)
	}
}

// The store refuses what the state machine refuses, and an event whose
// stream_version doesn't follow the row's version.
func TestTransitionGuards(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	e.insert(tr)
	err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		_, err := tx.Transition(e.ctx, tr.ID, transfer.Received, 1, transfer.Succeeded, event(t, tr.ID, 2, transfer.Received, transfer.Succeeded))
		return err
	})
	if !errors.Is(err, transfer.ErrIllegalTransition) {
		t.Fatalf("RECEIVED → SUCCEEDED: %v", err)
	}
	err = e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		_, err := tx.Transition(e.ctx, tr.ID, transfer.Received, 1, transfer.Routed, event(t, tr.ID, 5, transfer.Received, transfer.Routed))
		return err
	})
	if !errors.Is(err, repository.ErrStreamVersion) {
		t.Fatalf("event version 5 for row version 1: %v", err)
	}
	if n := e.outboxCount(); n != 1 {
		t.Fatalf("outbox rows = %d after refused transitions, want 1", n)
	}
}

func TestOneOpenAttemptPerTransfer(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	e.insert(tr)
	attempt := func(no int) error {
		return e.store.InTx(e.ctx, func(tx *repository.Tx) error {
			return tx.InsertAttempt(e.ctx, repository.Attempt{TransferID: tr.ID, AttemptNo: no, RequestID: uuid.NewString(),
				BankMiddleware: "bank-middleware-a", Rail: contract.RailBIFAST, Status: string(transfer.Dispatching)})
		})
	}
	if err := attempt(1); err != nil {
		t.Fatal(err)
	}
	if err := attempt(2); !errors.Is(err, repository.ErrAttemptOpen) {
		t.Fatalf("second open attempt: %v, want ErrAttemptOpen", err)
	}
	_, attempts, err := e.store.GetTransfer(e.ctx, tr.ID)
	if err != nil || len(attempts) != 1 || attempts[0].Rail != "BIFAST" {
		t.Fatalf("attempts = %+v, %v", attempts, err)
	}
}

// P§3.5: a state change and its journal event commit together or not at all.
func TestStateChangeAndJournalCommitTogether(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	e.insert(tr)
	boom := errors.New("crash after the outbox insert")
	err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		if _, err := tx.Transition(e.ctx, tr.ID, transfer.Received, 1, transfer.Routed, event(t, tr.ID, 2, transfer.Received, transfer.Routed)); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	got, _, _ := e.store.GetTransfer(e.ctx, tr.ID)
	if got.State != transfer.Received || got.Version != 1 || e.outboxCount() != 1 {
		t.Fatalf("after rollback: %s v%d, outbox %d; want RECEIVED v1, 1 row", got.State, got.Version, e.outboxCount())
	}
}

// Record moves the stream on without a state change (a rejection, a bank
// event): the version and the journal still advance together.
func TestRecordBumpsVersionOnly(t *testing.T) {
	e := newEnv(t)
	tr := newTransfer()
	e.insert(tr)
	if err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
		_, err := tx.Record(e.ctx, tr.ID, 1, event(t, tr.ID, 2, transfer.Received, transfer.Received))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := e.store.GetTransfer(e.ctx, tr.ID)
	if got.State != transfer.Received || got.Version != 2 || e.outboxCount() != 2 {
		t.Fatalf("after Record: %s v%d, outbox %d", got.State, got.Version, e.outboxCount())
	}
}

func TestMarkProcessedOnce(t *testing.T) {
	e := newEnv(t)
	id := uuid.NewString()
	for i, want := range []bool{true, false} {
		var first bool
		if err := e.store.InTx(e.ctx, func(tx *repository.Tx) error {
			var err error
			first, err = tx.MarkProcessed(e.ctx, id, contract.TopicCommands)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if first != want {
			t.Fatalf("call %d: first = %v, want %v", i+1, first, want)
		}
	}
}

func TestGetUnknownTransfer(t *testing.T) {
	e := newEnv(t)
	if _, _, err := e.store.GetTransfer(e.ctx, uuid.NewString()); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("unknown: %v, want ErrNotFound", err)
	}
}

// The outbox store hands the relay rows in id order, keyed by transfer_id,
// on the journal topic, and marks them published.
func TestOutboxRepo(t *testing.T) {
	e := newEnv(t)
	a, b := newTransfer(), newTransfer()
	e.insert(a)
	e.insert(b)
	ob := repository.NewOutboxRepo(e.store.DB())
	rows, err := ob.Unpublished(e.ctx, 10)
	if err != nil || len(rows) != 2 || rows[0].Key != a.ID || rows[1].Key != b.ID || rows[0].Topic != contract.TopicJournal {
		t.Fatalf("unpublished = %+v, %v", rows, err)
	}
	if rows[0].Headers["event_type"] != contract.EventTransferReceived {
		t.Fatalf("headers = %v", rows[0].Headers)
	}
	if err := ob.MarkPublished(e.ctx, []int64{rows[0].ID}); err != nil {
		t.Fatal(err)
	}
	if rows, _ := ob.Unpublished(e.ctx, 10); len(rows) != 1 || rows[0].Key != b.ID {
		t.Fatalf("after marking one: %+v", rows)
	}
	if pending, _, err := ob.Lag(e.ctx); err != nil || pending != 1 {
		t.Fatalf("lag pending = %d, %v", pending, err)
	}
}
