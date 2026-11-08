//go:build integration

package service_test

import (
	"bytes"
	"context"
	"net/http"
	"testing"
	"time"

	"auth-api/internal/idempotency"
	"auth-api/internal/outbox"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/worker"
)

func (e *svcEnv) recovery(sweepAfter, maxAge time.Duration) *service.Recovery {
	e.t.Helper()
	r, err := service.NewRecovery(e.svc, repository.NewLedgerRepo(e.pool, outbox.DefaultTopic, nil),
		service.RecoveryConfig{SweepAfter: sweepAfter, PendingMaxAge: maxAge, SweepBatch: 100, PurgeBatch: 2})
	if err != nil {
		e.t.Fatal(err)
	}
	return r
}

// strand runs a credit whose Tx2 fails, leaving it PENDING (202).
func (e *svcEnv) strand(key, acct string, amount int64) string {
	e.t.Helper()
	e.store.failInTx(2, repository.ErrTimeout)
	r, err := e.money(e.svc.Credit, key, acct, amount, "IDR")
	if err != nil || r.Status != http.StatusAccepted {
		e.t.Fatalf("want 202: %d %s %v", r.Status, r.Body, err)
	}
	return decode[txBody](e.t, r).Transaction.ID
}

func TestSweeperCompletesStuckTransactions(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("sweep@x.io")
	stuck := e.strand("cr-stuck", acct, 250)

	// Not stuck yet: younger than SweepAfter.
	if err := e.recovery(time.Hour, 2*time.Hour).Sweep(e.ctx); err != nil {
		t.Fatal(err)
	}
	if o, _ := e.svc.Transaction(e.ctx, stuck); o.Status != "PENDING" {
		t.Fatalf("swept too early: %s", o.Status)
	}
	// While PENDING, a retry of the key is 409.
	if _, err := e.money(e.svc.Credit, "cr-stuck", acct, 250, "IDR"); err == nil {
		t.Fatal("retry while PENDING succeeded")
	}

	e.resetLogs()
	if err := e.recovery(0, time.Hour).Sweep(e.ctx); err != nil {
		t.Fatal(err)
	}
	o, _ := e.svc.Transaction(e.ctx, stuck)
	if o.Status != "SETTLED" || *o.BalanceAfter != 250 {
		t.Fatalf("after sweep: %+v", o)
	}
	wantEvent(t, e.logs, "ledger.recover", map[string]any{"outcome": "success", "reason": "stuck_pending", "level": "warn",
		"transaction_id": stuck, "status": "SETTLED", "key_found": true})
	wantEvent(t, e.logs, "ledger.credit", map[string]any{"outcome": "success", "transaction_id": stuck})

	// The client's retry now replays the swept result.
	r, err := e.money(e.svc.Credit, "cr-stuck", acct, 250, "IDR")
	if err != nil || !r.Replayed || r.Status != http.StatusCreated || decode[txBody](t, r).Transaction.ID != stuck {
		t.Fatalf("retry after sweep: %d %s %v", r.Status, r.Body, err)
	}

	// A second sweep finds nothing and logs nothing.
	e.resetLogs()
	if err := e.recovery(0, time.Hour).Sweep(e.ctx); err != nil || e.logs.Len() != 0 {
		t.Fatalf("second sweep: %v\n%s", err, e.logs)
	}
}

func TestSweeperExpiresOldPendingThroughProcessing(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("expire@x.io")
	stuck := e.strand("cr-old", acct, 99)

	e.resetLogs()
	if err := e.recovery(0, 0).Sweep(e.ctx); err != nil {
		t.Fatal(err)
	}
	o, _ := e.svc.Transaction(e.ctx, stuck)
	if o.Status != "FAILED" || o.FailureReason != "processing_timeout" {
		t.Fatalf("expired: %+v", o)
	}
	wantEvent(t, e.logs, "ledger.recover", map[string]any{"outcome": "success", "reason": "processing_timeout", "status": "FAILED"})
	wantEvent(t, e.logs, "ledger.credit", map[string]any{"outcome": "failure", "reason": "processing_timeout", "level": "error"})
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND
		((stream_version = 2 AND event_type = 'TransactionProcessing') OR (stream_version = 3 AND event_type = 'TransactionFailed'))`, stuck); n != 2 {
		t.Fatalf("PENDING → PROCESSING → FAILED events: %d", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.journal_entries WHERE transaction_id = $1`, stuck); n != 0 {
		t.Fatal("an expired transaction posted journal lines")
	}
	if n := e.count(`SELECT count(*) FROM ledger.ledgers WHERE account_id = $1 AND balance = 0`, acct); n != 1 {
		t.Fatal("balance changed")
	}
	r, err := e.money(e.svc.Credit, "cr-old", acct, 99, "IDR")
	if err != nil || !r.Replayed || r.Status != http.StatusInternalServerError || decode[errBody](t, r).Error.Code != "processing_timeout" {
		t.Fatalf("retry after expiry: %d %s %v", r.Status, r.Body, err)
	}
}

func TestSweeperWithoutIdempotencyKey(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("nokey@x.io")
	stuck := e.strand("cr-nokey", acct, 10)
	if _, err := e.pool.Exec(e.ctx, `DELETE FROM ledger.idempotency_keys WHERE resource_id = $1`, stuck); err != nil {
		t.Fatal(err)
	}
	e.resetLogs()
	if err := e.recovery(0, time.Hour).Sweep(e.ctx); err != nil {
		t.Fatal(err)
	}
	if o, _ := e.svc.Transaction(e.ctx, stuck); o.Status != "SETTLED" {
		t.Fatalf("status = %s", o.Status)
	}
	wantEvent(t, e.logs, "ledger.recover", map[string]any{"key_found": false})
}

func TestSweeperRecoversAPendingVoid(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("svoid@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 40, "IDR"))
	e.store.failInTx(2, repository.ErrTimeout)
	r, _ := e.void("v-stuck", credit.ID, "r")
	if r.Status != http.StatusAccepted {
		t.Fatalf("void: %d %s", r.Status, r.Body)
	}
	if err := e.recovery(0, time.Hour).Sweep(e.ctx); err != nil {
		t.Fatal(err)
	}
	if o, _ := e.svc.Transaction(e.ctx, credit.ID); o.Status != "VOIDED" {
		t.Fatalf("original = %s", o.Status)
	}
	e.assertBalancesMatchJournal()
}

func TestPurgeDeletesOnlyExpiredKeys(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("purge@x.io")
	first, _ := e.money(e.svc.Credit, "live", acct, 1, "IDR")
	for i := range 5 {
		if _, err := e.money(e.svc.Credit, "old-"+string(rune('a'+i)), acct, 1, "IDR"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.idempotency_keys SET created_at = now() - interval '2 days',
		expires_at = now() - interval '1 day' WHERE key_hash <> $1`, idempotency.HashKey("live")); err != nil {
		t.Fatal(err)
	}
	e.resetLogs()
	if err := e.recovery(0, time.Hour).PurgeIdempotencyKeys(e.ctx); err != nil { // batch 2: three statements
		t.Fatal(err)
	}
	wantEvent(t, e.logs, "ledger.purge", map[string]any{"outcome": "success", "deleted": float64(6)}) // 5 credits + the account open
	if n := e.count(`SELECT count(*) FROM ledger.idempotency_keys`); n != 1 {
		t.Fatalf("%d keys left, want 1", n)
	}
	if r, _ := e.money(e.svc.Credit, "live", acct, 1, "IDR"); !r.Replayed || !bytes.Equal(r.Body, first.Body) {
		t.Fatal("live key no longer replays")
	}
	// Nothing left to purge: no event.
	e.resetLogs()
	if err := e.recovery(0, time.Hour).PurgeIdempotencyKeys(e.ctx); err != nil || e.logs.Len() != 0 {
		t.Fatalf("empty purge: %v %s", err, e.logs)
	}
}

// TRD §13.4: with the job running, a transaction whose Tx2 was lost reaches
// a terminal state within SweepAfter + interval (NFR-7 at small scale).
func TestSweeperJobRecoversWithinBound(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("bound@x.io")
	const after, interval = 300 * time.Millisecond, 100 * time.Millisecond
	rec := e.recovery(after, time.Hour)
	runner := worker.NewRunner(e.pool, worker.Job{Name: "sweeper", Interval: interval, Run: rec.Sweep})
	runner.AcquireEvery = interval
	ctx, stop := context.WithCancel(e.ctx)
	done := make(chan struct{})
	go func() { runner.Run(ctx); close(done) }()
	defer func() { stop(); <-done }()

	stuck := e.strand("cr-bound", acct, 5)
	start := time.Now()
	for time.Since(start) < after+interval+time.Second {
		if o, _ := e.svc.Transaction(e.ctx, stuck); o.Status == "SETTLED" {
			if took := time.Since(start); took > after+2*interval+200*time.Millisecond {
				t.Fatalf("recovered after %v, bound %v", took, after+interval)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("not recovered")
}
