//go:build integration

package service_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"auth-api/internal/repository"
	"auth-api/internal/service"
)

func (e *svcEnv) freeze(key, acct, reason, rule string) (service.Response, error) {
	return e.svc.Freeze(e.ctx, service.FreezeCommand{Request: req("ops", key), AccountID: acct, Reason: reason, Rule: rule})
}

func (e *svcEnv) unfreeze(key, acct, reason string) (service.Response, error) {
	return e.svc.Unfreeze(e.ctx, service.UnfreezeCommand{Request: req("ops", key), AccountID: acct, Reason: reason})
}

func (e *svcEnv) status(r service.Response, err error) string {
	e.t.Helper()
	if err != nil || r.Status != http.StatusOK {
		e.t.Fatalf("want 200: %d %s %v", r.Status, r.Body, err)
	}
	return decode[accountBody](e.t, r).Account.Status
}

func TestFreezeAndUnfreeze(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("freeze@x.io")
	credit := e.mustSettle(e.money(e.svc.Credit, "c1", acct, 500, "IDR"))

	e.resetLogs()
	first, err := e.freeze("f1", acct, "SECRET-REASON", "topup_velocity")
	if e.status(first, err) != "FROZEN" {
		t.Fatalf("freeze: %s", first.Body)
	}
	wantEvent(t, e.logs, "ledger.freeze", map[string]any{"outcome": "success", "account_id": acct, "actor": "ops",
		"rule": "topup_velocity", "changed": true, "level": "info"})
	if strings.Contains(e.logs.String(), "SECRET-REASON") {
		t.Fatal("freeze reason logged")
	}
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND event_type = 'AccountFrozen'
		AND payload->>'actor' = 'ops' AND payload->>'rule' = 'topup_velocity' AND trace_id = $2`, acct, testTrace); n != 1 {
		t.Fatalf("AccountFrozen events: %d", n)
	}
	if n := e.count(`SELECT count(*) FROM ledger.outbox WHERE headers->>'event_type' = 'AccountFrozen'`); n != 1 {
		t.Fatalf("AccountFrozen outbox rows: %d", n)
	}

	// Replay and key reuse.
	if r, err := e.freeze("f1", acct, "SECRET-REASON", "topup_velocity"); err != nil || !r.Replayed || !bytes.Equal(r.Body, first.Body) {
		t.Fatalf("replay: %s %v", r.Body, err)
	}
	if _, err := e.freeze("f1", acct, "other", ""); !errors.Is(err, service.ErrIdempotencyKeyReused) {
		t.Fatalf("key reuse: %v", err)
	}

	// Already frozen: 200, no new event.
	e.resetLogs()
	if e.status(e.freeze("f2", acct, "again", "")) != "FROZEN" {
		t.Fatal("second freeze")
	}
	wantEvent(t, e.logs, "ledger.freeze", map[string]any{"outcome": "success", "changed": false})
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND event_type = 'AccountFrozen'`, acct); n != 1 {
		t.Fatalf("idempotent freeze appended: %d events", n)
	}

	// Frozen: debits fail, credits settle, voids are allowed (Q3).
	r, _ := e.money(e.svc.Debit, "d1", acct, 1, "IDR")
	if r.Status != http.StatusUnprocessableEntity || decode[errBody](t, r).Error.Code != "account_frozen" {
		t.Fatalf("debit while frozen: %d %s", r.Status, r.Body)
	}
	e.mustSettle(e.money(e.svc.Credit, "c2", acct, 1, "IDR"))
	e.mustSettle(e.void("v1", credit.ID, "r"))

	// Unfreeze: ACTIVE, one AccountUnfrozen, debits work again.
	e.resetLogs()
	if e.status(e.unfreeze("u1", acct, "cleared")) != "ACTIVE" {
		t.Fatal("unfreeze")
	}
	wantEvent(t, e.logs, "ledger.unfreeze", map[string]any{"outcome": "success", "changed": true, "actor": "ops"})
	if e.status(e.unfreeze("u2", acct, "cleared")) != "ACTIVE" {
		t.Fatal("second unfreeze")
	}
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND event_type = 'AccountUnfrozen'`, acct); n != 1 {
		t.Fatalf("AccountUnfrozen events: %d", n)
	}
	e.mustSettle(e.money(e.svc.Debit, "d2", acct, 1, "IDR"))

	// The account stream still folds to the projection.
	if n := e.count(`SELECT count(*) FROM ledger.accounts WHERE id = $1 AND status = 'ACTIVE'
		AND version = (SELECT max(stream_version) FROM ledger.events WHERE stream_id = $1)`, acct); n != 1 {
		t.Fatal("account projection version differs from its stream")
	}
}

func TestFreezeRejections(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	acct := e.openAccount("frej@x.io")
	for name, c := range map[string]struct {
		cmd    service.FreezeCommand
		reason string
	}{
		"unknown rule":    {service.FreezeCommand{Request: req("ops", "k1"), AccountID: acct, Reason: "r", Rule: "SECRET-RULE"}, "invalid_input"},
		"no reason":       {service.FreezeCommand{Request: req("ops", "k2"), AccountID: acct}, "invalid_input"},
		"long reason":     {service.FreezeCommand{Request: req("ops", "k3"), AccountID: acct, Reason: strings.Repeat("r", 65)}, "invalid_input"},
		"missing key":     {service.FreezeCommand{Request: req("ops", ""), AccountID: acct, Reason: "r"}, "invalid_input"},
		"malformed id":    {service.FreezeCommand{Request: req("ops", "k4"), AccountID: "nope", Reason: "r"}, "account_not_found"},
		"unknown account": {service.FreezeCommand{Request: req("ops", "k5"), AccountID: "0192f3c4-0000-7000-8000-00000000dead", Reason: "r"}, "account_not_found"},
	} {
		e.resetLogs()
		_, err := e.svc.Freeze(e.ctx, c.cmd)
		var ve *service.ValidationError
		if !errors.As(err, &ve) && !errors.Is(err, service.ErrAccountNotFound) {
			t.Errorf("%s: err = %v", name, err)
		}
		wantEvent(t, e.logs, "ledger.freeze", map[string]any{"outcome": "denied", "reason": c.reason, "level": "warn"})
		if strings.Contains(e.logs.String(), "SECRET-RULE") {
			t.Errorf("%s: unknown rule value logged", name)
		}
	}
	if n := e.count(`SELECT count(*) FROM ledger.accounts WHERE id = $1 AND status = 'ACTIVE'`, acct); n != 1 {
		t.Fatal("a rejected freeze changed the account")
	}
}

// A freeze waits for an in-flight Tx2 that holds the account FOR SHARE, and
// every Tx2 that locks after it commits sees FROZEN (TRD §7.1, FR-09).
func TestFreezeWaitsForInFlightTx2(t *testing.T) {
	e := newSvcEnv(t, 5*time.Second)
	acct := e.openAccount("fwait@x.io")
	e.mustSettle(e.money(e.svc.Credit, "c1", acct, 100, "IDR"))

	inflight, err := e.pool.Begin(e.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inflight.Exec(e.ctx, `SELECT 1 FROM ledger.accounts WHERE id = $1 FOR SHARE`, acct); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := e.freeze("f-wait", acct, "r", ""); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("freeze committed while a Tx2 held the account: %v", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := inflight.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	r, _ := e.money(e.svc.Debit, "d-after", acct, 1, "IDR")
	if decode[errBody](t, r).Error.Code != "account_frozen" {
		t.Fatalf("debit after the freeze committed: %d %s", r.Status, r.Body)
	}
}

// A waiting freeze is not starved by Tx2s that arrive after it: they queue
// behind it and then see FROZEN. Row locks alone would let each new FOR
// SHARE jump the waiting FOR UPDATE (the advisory lock in LockAccount).
func TestFreezeIsNotStarvedByLaterTx2s(t *testing.T) {
	e := newSvcEnv(t, 5*time.Second)
	acct := e.openAccount("fstarve@x.io")
	e.mustSettle(e.money(e.svc.Credit, "c1", acct, 100, "IDR"))

	held, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock) // a failing assertion must not leave the holder waiting
	holder := make(chan error, 1)
	go func() { // an in-flight Tx2 holding the account
		holder <- e.store.InTx(e.ctx, 5*time.Second, func(tx repository.LedgerTx) error {
			if _, err := tx.LockAccount(e.ctx, acct, repository.LockShare); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	frozen := make(chan error, 1)
	go func() { _, err := e.freeze("f-starve", acct, "r", ""); frozen <- err }()
	time.Sleep(200 * time.Millisecond) // the freeze is now waiting

	debit := make(chan service.Response, 1)
	go func() { r, _ := e.money(e.svc.Debit, "d-late", acct, 1, "IDR"); debit <- r }()
	select {
	case r := <-debit:
		t.Fatalf("a debit arriving after the freeze ran first: %d %s", r.Status, r.Body)
	case <-time.After(300 * time.Millisecond):
	}
	unblock()
	if err := <-holder; err != nil {
		t.Fatal(err)
	}
	if err := <-frozen; err != nil {
		t.Fatal(err)
	}
	if r := <-debit; decode[errBody](t, r).Error.Code != "account_frozen" {
		t.Fatalf("late debit: %d %s", r.Status, r.Body)
	}
}

// Debits and credits race a freeze: nothing deadlocks or errors, every
// debit either settles or fails account_frozen, credits all settle, and the
// balance matches the journal.
func TestFreezeRace(t *testing.T) {
	e := newSvcEnv(t, 5*time.Second)
	acct := e.openAccount("frace@x.io")
	e.mustSettle(e.money(e.svc.Credit, "seed", acct, 1000, "IDR"))

	var wg sync.WaitGroup
	var mu sync.Mutex
	outcomes := map[string]int{}
	note := func(k string) { mu.Lock(); outcomes[k]++; mu.Unlock() }
	someSettled := make(chan struct{})
	var once sync.Once
	// The freeze starts once debits are settling, so it lands mid-stream.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-someSettled
		if _, err := e.freeze("f-race", acct, "r", "debit_volume"); err != nil {
			note("freeze error " + err.Error())
		}
	}()
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if i >= 10 {
				time.Sleep(time.Duration(i) * 2 * time.Millisecond) // stagger past the freeze
			}
			op, name := e.svc.Debit, "debit"
			if i%3 == 0 {
				op, name = e.svc.Credit, "credit"
			}
			r, err := e.money(op, fmt.Sprintf("race-%d", i), acct, 1, "IDR")
			switch {
			case err == nil && r.Status == http.StatusCreated:
				note(name + " settled")
				if name == "debit" {
					once.Do(func() { close(someSettled) })
				}
			case err == nil && name == "debit" && decode[errBody](t, r).Error.Code == "account_frozen":
				note("debit frozen")
			default:
				note(fmt.Sprintf("%s unexpected %d %s %v", name, r.Status, r.Body, err))
			}
		}()
	}
	wg.Wait()
	for k, n := range outcomes {
		if strings.Contains(k, "unexpected") || strings.Contains(k, "error") {
			t.Errorf("%s ×%d", k, n)
		}
	}
	if outcomes["credit settled"] != 20 {
		t.Errorf("credits settled = %d, want 20 (%v)", outcomes["credit settled"], outcomes)
	}
	if outcomes["debit settled"] == 0 || outcomes["debit frozen"] == 0 {
		t.Fatalf("the freeze did not land mid-stream: %v", outcomes)
	}
	// Each settled operation moved the balance by exactly 1.
	want := int64(1000 - outcomes["debit settled"] + outcomes["credit settled"])
	if n := e.count(`SELECT count(*) FROM ledger.ledgers WHERE account_id = $1 AND balance = $2`, acct, want); n != 1 {
		t.Fatalf("balance != %d", want)
	}
	r, _ := e.money(e.svc.Debit, "after", acct, 1, "IDR")
	if decode[errBody](t, r).Error.Code != "account_frozen" {
		t.Fatalf("debit after the race: %s", r.Body)
	}
	e.assertBalancesMatchJournal()
	t.Logf("outcomes: %v", outcomes)
}

func (e *svcEnv) assertBalancesMatchJournal() {
	e.t.Helper()
	if n := e.count(`SELECT count(*) FROM ledger.ledgers l WHERE kind = 'USER'
		AND l.balance <> (SELECT coalesce(sum(amount), 0) FROM ledger.journal_entries j WHERE j.ledger_id = l.id)`); n != 0 {
		e.t.Fatalf("%d user ledger(s) whose balance differs from their journal", n)
	}
}
