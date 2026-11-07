//go:build integration

package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"auth-api/internal/idempotency"
	"auth-api/internal/ledger"
	"auth-api/internal/outbox"
	"auth-api/internal/repository"
	"auth-api/internal/service"
	"auth-api/internal/testutil/pgtest"
)

// faultStore fails the InTx call numbered failAt (1-based, counted from
// creation) with err, to exercise Tx1 and Tx2 failure paths.
type faultStore struct {
	service.LedgerStore
	mu     sync.Mutex
	calls  int
	failAt int
	err    error
}

func (f *faultStore) InTx(ctx context.Context, d time.Duration, fn func(repository.LedgerTx) error) error {
	f.mu.Lock()
	f.calls++
	fail := f.calls == f.failAt
	f.mu.Unlock()
	if fail {
		return f.err
	}
	return f.LedgerStore.InTx(ctx, d, fn)
}

// failInTx makes the n-th InTx call from now fail with err.
func (f *faultStore) failInTx(n int, err error) {
	f.mu.Lock()
	f.failAt = f.calls + n
	f.err = err
	f.mu.Unlock()
}

type svcEnv struct {
	t     *testing.T
	ctx   context.Context
	logs  *bytes.Buffer
	pool  *pgxpool.Pool
	store *faultStore
	svc   *service.LedgerService
}

func newSvcEnv(t *testing.T, timeout time.Duration) *svcEnv {
	t.Helper()
	pool := pgtest.New(t)
	store := &faultStore{LedgerStore: repository.NewLedgerRepo(pool, outbox.DefaultTopic)}
	cfg := testConfig()
	cfg.ProcessTimeout = timeout
	svc, err := service.NewLedgerService(store, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, logs := logCtx()
	if err := svc.EnsureSystemLedgers(ctx); err != nil {
		t.Fatal(err)
	}
	logs.Reset()
	return &svcEnv{t: t, ctx: ctx, logs: logs, pool: pool, store: store, svc: svc}
}

func (e *svcEnv) resetLogs() { e.logs.Reset() }

func (e *svcEnv) user(email string) string {
	e.t.Helper()
	var id string
	if err := e.pool.QueryRow(e.ctx, `INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`, email).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func req(client, key string) service.Request {
	return service.Request{ClientID: client, IdempotencyKey: key, RequestID: "req-" + key, TraceID: testTrace}
}

type accountBody struct {
	Account service.AccountView `json:"account"`
}

type txBody struct {
	Transaction service.TransactionView `json:"transaction"`
}

type errBody struct {
	Error struct {
		Code          string `json:"code"`
		RequestID     string `json:"request_id"`
		TransactionID string `json:"transaction_id"`
	} `json:"error"`
}

func decode[T any](t *testing.T, r service.Response) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(r.Body, &v); err != nil {
		t.Fatalf("decode %s: %v", r.Body, err)
	}
	return v
}

// openAccount opens an account for a new user and returns its id.
func (e *svcEnv) openAccount(email string) string {
	e.t.Helper()
	r, err := e.svc.OpenAccount(e.ctx, service.OpenAccountCommand{Request: req("onboarding", "open-"+strings.NewReplacer("@", "_").Replace(email)), UserID: e.user(email)})
	if err != nil || r.Status != http.StatusCreated {
		e.t.Fatalf("open account: %d %s %v", r.Status, r.Body, err)
	}
	return decode[accountBody](e.t, r).Account.ID
}

func (e *svcEnv) money(op func(context.Context, service.MoneyCommand) (service.Response, error), key, accountID string, amount int64, currency string) (service.Response, error) {
	return op(e.ctx, service.MoneyCommand{Request: req("payments", key), AccountID: accountID, Amount: amount, Currency: currency,
		Reference: "SECRET-REF", Metadata: json.RawMessage(`{"channel":"secret-meta"}`)})
}

func (e *svcEnv) count(sql string, args ...any) int {
	e.t.Helper()
	var n int
	if err := e.pool.QueryRow(e.ctx, sql, args...).Scan(&n); err != nil {
		e.t.Fatal(err)
	}
	return n
}

func TestEnsureSystemLedgersIsIdempotent(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second) // already ran once
	if err := e.svc.EnsureSystemLedgers(e.ctx); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(e.logs.String(), "system ledger created") {
		t.Fatal("second run created ledgers")
	}
	if n := e.count(`SELECT count(*) FROM ledger.ledgers WHERE kind = 'SYSTEM'`); n != 4 {
		t.Fatalf("system ledgers = %d, want 4 (IDR and USD × topup_clearing and settlement)", n)
	}
}

func TestOpenAccount(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	userID := e.user("a@example.com")
	cmd := service.OpenAccountCommand{Request: req("onboarding", "open-1"), UserID: userID}

	first, err := e.svc.OpenAccount(e.ctx, cmd)
	if err != nil || first.Status != http.StatusCreated {
		t.Fatalf("open: %d %s %v", first.Status, first.Body, err)
	}
	acct := decode[accountBody](t, first).Account
	if acct.UserID != userID || acct.Status != "ACTIVE" || len(acct.Ledgers) != 1 || acct.Ledgers[0].Currency != "IDR" || acct.Ledgers[0].Balance != 0 {
		t.Fatalf("account = %+v", acct)
	}
	wantEvent(t, e.logs, "ledger.account_open", map[string]any{"outcome": "success", "account_id": acct.ID, "created": true})

	// The account stream folds to the projection.
	events, err := repository.NewLedgerRepo(e.pool, outbox.DefaultTopic).ListStreamEvents(e.ctx, acct.ID)
	if err != nil {
		t.Fatal(err)
	}
	folded, err := ledger.FoldAccount(events)
	if err != nil || folded.Version != 2 || folded.Status != ledger.AccountStatusActive || folded.Ledgers["IDR"] != acct.Ledgers[0].ID {
		t.Fatalf("fold = %+v, %v", folded, err)
	}

	e.resetLogs()
	replay, err := e.svc.OpenAccount(e.ctx, cmd)
	if err != nil || !replay.Replayed || replay.Status != first.Status || !bytes.Equal(replay.Body, first.Body) {
		t.Fatalf("replay: %+v %v", replay, err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"outcome": "success", "operation": "account_open"})

	e.resetLogs()
	other := cmd
	other.UserID = e.user("b@example.com")
	if _, err := e.svc.OpenAccount(e.ctx, other); !errors.Is(err, service.ErrIdempotencyKeyReused) {
		t.Fatalf("same key, other user: %v", err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"outcome": "denied", "reason": "key_reused"})

	e.resetLogs()
	again, err := e.svc.OpenAccount(e.ctx, service.OpenAccountCommand{Request: req("onboarding", "open-2"), UserID: userID})
	if err != nil || again.Status != http.StatusOK || decode[accountBody](t, again).Account.ID != acct.ID {
		t.Fatalf("second open with a new key: %d %s %v", again.Status, again.Body, err)
	}
	wantEvent(t, e.logs, "ledger.account_open", map[string]any{"outcome": "success", "created": false})

	e.resetLogs()
	if _, err := e.svc.OpenAccount(e.ctx, service.OpenAccountCommand{Request: req("onboarding", "open-3"), UserID: ledger.NewID()}); !errors.Is(err, service.ErrUserNotFound) {
		t.Fatalf("unknown user: %v", err)
	}
	wantEvent(t, e.logs, "ledger.account_open", map[string]any{"outcome": "denied", "reason": "user_not_found"})
}

func TestConcurrentOpenAccountForOneUser(t *testing.T) {
	e := newSvcEnv(t, 5*time.Second)
	userID := e.user("c@example.com")
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		statuses = map[int]int{}
		ids      = map[string]bool{}
	)
	for i := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.svc.OpenAccount(e.ctx, service.OpenAccountCommand{Request: req("onboarding", fmt.Sprintf("race-%d", i)), UserID: userID})
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			statuses[r.Status]++
			ids[decode[accountBody](t, r).Account.ID] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if statuses[http.StatusCreated] != 1 || statuses[http.StatusOK] != 9 || len(ids) != 1 {
		t.Fatalf("statuses %v, distinct accounts %d", statuses, len(ids))
	}
}

func TestOpenLedger(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("l@example.com")

	r, err := e.svc.OpenLedger(e.ctx, service.OpenLedgerCommand{Request: req("payments", "usd-1"), AccountID: accountID, Currency: "USD", OverdraftLimit: 500})
	if err != nil || r.Status != http.StatusCreated {
		t.Fatalf("open USD: %d %s %v", r.Status, r.Body, err)
	}
	var lb struct {
		Ledger service.LedgerView `json:"ledger"`
	}
	_ = json.Unmarshal(r.Body, &lb)
	if lb.Ledger.Currency != "USD" || lb.Ledger.OverdraftLimit != 0 {
		t.Fatalf("ledger = %+v (overdraft must be forced to 0 for a non-allow-listed client)", lb.Ledger)
	}
	if _, err := e.svc.OpenLedger(e.ctx, service.OpenLedgerCommand{Request: req("payments", "usd-2"), AccountID: accountID, Currency: "USD"}); !errors.Is(err, service.ErrLedgerExists) {
		t.Fatalf("second USD ledger: %v", err)
	}
	if _, err := e.svc.OpenLedger(e.ctx, service.OpenLedgerCommand{Request: req("payments", "usd-3"), AccountID: ledger.NewID(), Currency: "USD"}); !errors.Is(err, service.ErrAccountNotFound) {
		t.Fatalf("unknown account: %v", err)
	}

	other := e.openAccount("m@example.com")
	r, err = e.svc.OpenLedger(e.ctx, service.OpenLedgerCommand{Request: req("ops", "usd-4"), AccountID: other, Currency: "USD", OverdraftLimit: 500})
	_ = json.Unmarshal(r.Body, &lb)
	if err != nil || lb.Ledger.OverdraftLimit != 500 {
		t.Fatalf("allow-listed overdraft: %s %v", r.Body, err)
	}
	if v := e.count(`SELECT version FROM ledger.accounts WHERE id = $1`, other); v != 3 {
		t.Fatalf("account version after LedgerOpened = %d, want 3", v)
	}
}

func TestCreditThenDebitSettle(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("s@example.com")
	e.resetLogs()

	r, err := e.money(e.svc.Credit, "cr-1", accountID, 1000, "IDR")
	if err != nil || r.Status != http.StatusCreated {
		t.Fatalf("credit: %d %s %v", r.Status, r.Body, err)
	}
	cr := decode[txBody](t, r).Transaction
	if cr.Status != "SETTLED" || *cr.BalanceAfter != 1000 || *cr.LedgerSequence != 1 || cr.TraceID != testTrace || cr.Reference != "SECRET-REF" {
		t.Fatalf("credit = %+v", cr)
	}
	// The settled event says what settled, for the fraud engine (P4.1).
	if n := e.count(`SELECT count(*) FROM ledger.events WHERE stream_id = $1 AND event_type = 'TransactionSettled'
		AND payload->>'type' = 'CREDIT' AND (payload->>'amount')::bigint = 1000 AND payload->>'currency' = 'IDR'`, cr.ID); n != 1 {
		t.Fatalf("TransactionSettled lacks type/amount/currency (%d)", n)
	}
	wantEvent(t, e.logs, "ledger.credit", map[string]any{"outcome": "success", "level": "info", "transaction_id": cr.ID,
		"ledger_id": cr.LedgerID, "amount": float64(1000), "balance_after": float64(1000)})

	r, err = e.money(e.svc.Debit, "db-1", accountID, 300, "IDR")
	db := decode[txBody](t, r).Transaction
	if err != nil || r.Status != http.StatusCreated || *db.BalanceAfter != 700 || *db.LedgerSequence != 2 || db.Type != "DEBIT" {
		t.Fatalf("debit: %d %s %v", r.Status, r.Body, err)
	}
	for _, secret := range []string{"SECRET-REF", "secret-meta", "cr-1", "db-1"} {
		if strings.Contains(e.logs.String(), secret) {
			t.Errorf("logs contain %q", secret)
		}
	}

	// Projection == fold, journal balances, one outbox row per event.
	repo := repository.NewLedgerRepo(e.pool, outbox.DefaultTopic)
	for _, id := range []string{cr.ID, db.ID} {
		events, err := repo.ListStreamEvents(e.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		folded, err := ledger.FoldTransaction(events)
		rec, _ := repo.GetTransaction(e.ctx, id)
		if err != nil || folded.Status != rec.Status || folded.BalanceAfter != rec.BalanceAfter || folded.Version != rec.Version || folded.Version != 3 {
			t.Fatalf("fold %+v vs projection %+v (%v)", folded, rec, err)
		}
	}
	if bal := e.count(`SELECT balance FROM ledger.ledgers WHERE account_id = $1`, accountID); bal != 700 {
		t.Fatalf("ledger balance = %d", bal)
	}
	if sum := e.count(`SELECT COALESCE(sum(amount), 0) FROM ledger.journal_entries j JOIN ledger.ledgers l ON l.id = j.ledger_id WHERE l.account_id = $1`, accountID); sum != 700 {
		t.Fatalf("journal sum = %d", sum)
	}
	if n := e.count(`SELECT count(*) FROM ledger.outbox`); n != 2+3+3 {
		t.Fatalf("outbox rows = %d, want 8 (2 account + 3 per transaction)", n)
	}
}

func TestBusinessFailuresAreStoredAndReplayed(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("f@example.com")
	if _, err := e.money(e.svc.Credit, "seed", accountID, 100, "IDR"); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name, key string
		run       func() (service.Response, error)
		status    int
		code      string
	}{
		{"insufficient funds", "nsf", func() (service.Response, error) { return e.money(e.svc.Debit, "nsf", accountID, 101, "IDR") },
			http.StatusUnprocessableEntity, "insufficient_funds"},
		{"no USD ledger", "usd", func() (service.Response, error) { return e.money(e.svc.Credit, "usd", accountID, 5, "USD") },
			http.StatusNotFound, "ledger_not_found"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e.resetLogs()
			r, err := tc.run()
			if err != nil || r.Status != tc.status {
				t.Fatalf("%d %s %v", r.Status, r.Body, err)
			}
			b := decode[errBody](t, r)
			if b.Error.Code != tc.code || b.Error.TransactionID == "" || b.Error.RequestID != "req-"+tc.key {
				t.Fatalf("body = %s", r.Body)
			}
			wantEvent(t, e.logs, "ledger."+map[string]string{"nsf": "debit", "usd": "credit"}[tc.key],
				map[string]any{"outcome": "denied", "reason": tc.code, "level": "warn", "transaction_id": b.Error.TransactionID})
			if s := e.count(`SELECT count(*) FROM ledger.transactions WHERE id = $1 AND status = 'FAILED' AND failure_reason = $2`, b.Error.TransactionID, tc.code); s != 1 {
				t.Fatal("FAILED transaction not recorded")
			}
			again, err := tc.run()
			if err != nil || !again.Replayed || again.Status != r.Status || !bytes.Equal(again.Body, r.Body) {
				t.Fatalf("replay = %+v, %v", again, err)
			}
		})
	}
	if bal := e.count(`SELECT balance FROM ledger.ledgers WHERE account_id = $1`, accountID); bal != 100 {
		t.Fatalf("balance changed by failed transactions: %d", bal)
	}
}

func TestFrozenAccountBlocksDebitsNotCredits(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("z@example.com")
	if _, err := e.money(e.svc.Credit, "seed", accountID, 100, "IDR"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pool.Exec(e.ctx, `UPDATE ledger.accounts SET status = 'FROZEN' WHERE id = $1`, accountID); err != nil {
		t.Fatal(err)
	}
	r, _ := e.money(e.svc.Debit, "d", accountID, 1, "IDR")
	if r.Status != http.StatusUnprocessableEntity || decode[errBody](t, r).Error.Code != "account_frozen" {
		t.Fatalf("debit on frozen account: %d %s", r.Status, r.Body)
	}
	if r, err := e.money(e.svc.Credit, "c", accountID, 1, "IDR"); err != nil || r.Status != http.StatusCreated {
		t.Fatalf("credit on frozen account: %d %s %v", r.Status, r.Body, err)
	}
}

func TestRejectionsBeforeTx1WriteNothing(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("x@example.com")
	before := e.count(`SELECT count(*) FROM ledger.events`)

	if _, err := e.money(e.svc.Credit, "k1", ledger.NewID(), 1, "IDR"); !errors.Is(err, service.ErrAccountNotFound) {
		t.Errorf("unknown account: %v", err)
	}
	cmd := service.MoneyCommand{Request: req("payments", "k2"), AccountID: accountID, Amount: 1, Currency: "IDR", Counterparty: "nope"}
	var ve *service.ValidationError
	if _, err := e.svc.Credit(e.ctx, cmd); !errors.As(err, &ve) {
		t.Errorf("unknown counterparty: %v", err)
	}
	if _, err := e.money(e.svc.Credit, "k3", accountID, 10, "IDR"); err != nil {
		t.Fatal(err)
	}
	e.resetLogs()
	if _, err := e.money(e.svc.Credit, "k3", accountID, 11, "IDR"); !errors.Is(err, service.ErrIdempotencyKeyReused) {
		t.Errorf("same key, other amount: %v", err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"outcome": "denied", "reason": "key_reused"})

	if after := e.count(`SELECT count(*) FROM ledger.events`); after != before+3 {
		t.Fatalf("events grew by %d, want 3 (only the k3 credit)", after-before)
	}
	if n := e.count(`SELECT count(*) FROM ledger.idempotency_keys WHERE client_id = 'payments'`); n != 1 {
		t.Fatalf("payments idempotency keys = %d, want 1", n)
	}
}

// Tx2 fails after Tx1 committed: the client gets 202 PENDING, a retry with
// the same key gets 409 until the transaction is processed, then the stored
// response (TRD §6.4, §7.5).
func TestTx2FailureReturns202ThenCompletesThroughProcess(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("p@example.com")
	e.resetLogs()

	e.store.failInTx(2, fmt.Errorf("%w: lock wait", repository.ErrTimeout)) // InTx #1 is Tx1, #2 is Tx2
	r, err := e.money(e.svc.Credit, "slow", accountID, 50, "IDR")
	if err != nil || r.Status != http.StatusAccepted {
		t.Fatalf("want 202: %d %s %v", r.Status, r.Body, err)
	}
	pending := decode[txBody](t, r).Transaction
	if pending.Status != "PENDING" || pending.BalanceAfter != nil {
		t.Fatalf("pending = %+v", pending)
	}
	wantEvent(t, e.logs, "ledger.credit", map[string]any{"outcome": "partial", "reason": "tx2_timeout", "level": "error", "transaction_id": pending.ID})

	e.resetLogs()
	if _, err := e.money(e.svc.Credit, "slow", accountID, 50, "IDR"); !errors.Is(err, service.ErrRequestInProgress) {
		t.Fatalf("retry while pending: %v", err)
	}
	wantEvent(t, e.logs, "ledger.idempotency", map[string]any{"outcome": "denied", "reason": "in_progress"})

	// What the P2 sweeper will do.
	done, err := e.svc.Process(e.ctx, service.ProcessInput{TransactionID: pending.ID, ClientID: "payments",
		KeyHash: idempotency.HashKey("slow"), RequestID: "req-slow"})
	if err != nil || done.Status != http.StatusCreated || decode[txBody](t, done).Transaction.Status != "SETTLED" {
		t.Fatalf("process: %d %s %v", done.Status, done.Body, err)
	}
	replay, err := e.money(e.svc.Credit, "slow", accountID, 50, "IDR")
	if err != nil || !replay.Replayed || !bytes.Equal(replay.Body, done.Body) {
		t.Fatalf("replay after processing: %+v %v", replay, err)
	}

	// Processing an already-finished transaction is a no-op that reports it.
	e.resetLogs()
	again, err := e.svc.Process(e.ctx, service.ProcessInput{TransactionID: pending.ID, ClientID: "payments", KeyHash: idempotency.HashKey("slow")})
	if err != nil || again.Status != http.StatusCreated || strings.Contains(e.logs.String(), `"event":"ledger.credit"`) {
		t.Fatalf("second process: %d %v, logs %s", again.Status, err, e.logs)
	}
}

func TestTx1FailureIsAnErrorAndWritesNothing(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	accountID := e.openAccount("q@example.com")
	e.resetLogs()
	before := e.count(`SELECT count(*) FROM ledger.transactions`)

	e.store.failInTx(1, errors.New("connection reset"))
	if _, err := e.money(e.svc.Debit, "t1", accountID, 1, "IDR"); err == nil {
		t.Fatal("Tx1 failure was swallowed")
	}
	wantEvent(t, e.logs, "ledger.debit", map[string]any{"outcome": "failure", "reason": "pg_error", "level": "error"})
	if after := e.count(`SELECT count(*) FROM ledger.transactions`); after != before {
		t.Fatal("transaction written despite Tx1 failure")
	}
}

// TRD §13.3: balance 1,000; 200 concurrent debits of 10 → exactly 100
// SETTLED, 100 FAILED insufficient_funds, final balance 0, gapless sequence.
func TestConcurrentDebitsCannotDoubleSpend(t *testing.T) {
	e := newSvcEnv(t, 30*time.Second)
	accountID := e.openAccount("ds@example.com")
	if r, err := e.money(e.svc.Credit, "seed", accountID, 1000, "IDR"); err != nil || r.Status != http.StatusCreated {
		t.Fatalf("seed: %v", err)
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		statuses = map[int]int{}
	)
	for i := range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.money(e.svc.Debit, fmt.Sprintf("d-%d", i), accountID, 10, "IDR")
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			statuses[r.Status]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if statuses[http.StatusCreated] != 100 || statuses[http.StatusUnprocessableEntity] != 100 {
		t.Fatalf("statuses = %v, want 100 × 201 and 100 × 422", statuses)
	}
	if bal := e.count(`SELECT balance FROM ledger.ledgers WHERE account_id = $1`, accountID); bal != 0 {
		t.Fatalf("final balance = %d", bal)
	}
	if n := e.count(`SELECT count(*) FROM ledger.transactions WHERE account_id = $1 AND failure_reason = 'insufficient_funds'`, accountID); n != 100 {
		t.Fatalf("insufficient_funds transactions = %d", n)
	}
	// Gapless 1..101 (the seed credit is 1), and every balance_after is the running sum.
	var gaps int
	if err := e.pool.QueryRow(e.ctx, `
		SELECT count(*) FROM (
		  SELECT ledger_sequence, balance_after,
		         row_number() OVER (ORDER BY ledger_sequence) AS rn,
		         sum(amount) OVER (ORDER BY ledger_sequence) AS running
		    FROM ledger.journal_entries j JOIN ledger.ledgers l ON l.id = j.ledger_id
		   WHERE l.account_id = $1) s
		WHERE ledger_sequence <> rn OR balance_after <> running`, accountID).Scan(&gaps); err != nil {
		t.Fatal(err)
	}
	if gaps != 0 || e.count(`SELECT max(last_sequence) FROM ledger.ledgers WHERE account_id = $1`, accountID) != 101 {
		t.Fatalf("%d journal rows out of sequence or with a wrong balance_after", gaps)
	}
}

// TRD §13.3: 50 concurrent requests with one key → one transaction; the
// others get the stored response or 409.
func TestConcurrentSameKeyCreatesOneTransaction(t *testing.T) {
	e := newSvcEnv(t, 10*time.Second)
	accountID := e.openAccount("ir@example.com")
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		bodies  = map[string]int{}
		pending int
	)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := e.money(e.svc.Credit, "same", accountID, 7, "IDR")
			mu.Lock()
			defer mu.Unlock()
			switch {
			case errors.Is(err, service.ErrRequestInProgress):
				pending++
			case err != nil:
				t.Error(err)
			case r.Status != http.StatusCreated:
				t.Errorf("status %d", r.Status)
			default:
				bodies[string(r.Body)]++
			}
		}()
	}
	wg.Wait()
	if n := e.count(`SELECT count(*) FROM ledger.transactions WHERE account_id = $1`, accountID); n != 1 {
		t.Fatalf("transactions = %d, want 1", n)
	}
	if len(bodies) != 1 || pending+bodies[func() string {
		for b := range bodies {
			return b
		}
		return ""
	}()] != 50 {
		t.Fatalf("distinct bodies %d, pending %d", len(bodies), pending)
	}
	if bal := e.count(`SELECT balance FROM ledger.ledgers WHERE account_id = $1`, accountID); bal != 7 {
		t.Fatalf("balance = %d, want 7", bal)
	}
}
