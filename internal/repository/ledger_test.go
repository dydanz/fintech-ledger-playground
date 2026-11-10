//go:build integration

package repository_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/outbox"
	"fintech-ledger-playground/internal/repository"
	"fintech-ledger-playground/internal/testutil/pgtest"
)

const (
	traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	txWait  = 2 * time.Second
)

type env struct {
	t    *testing.T
	ctx  context.Context
	pool *pgxpool.Pool
	repo *repository.LedgerRepo
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := pgtest.New(t)
	return &env{t: t, ctx: context.Background(), pool: pool, repo: repository.NewLedgerRepo(pool, outbox.DefaultTopic, nil)}
}

func (e *env) user(email string) string {
	e.t.Helper()
	var id string
	if err := e.pool.QueryRow(e.ctx, `INSERT INTO users (email, password_hash) VALUES ($1, 'x') RETURNING id`, email).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *env) inTx(fn func(repository.LedgerTx) error) error {
	return e.repo.InTx(e.ctx, txWait, fn)
}

func (e *env) must(err error) {
	e.t.Helper()
	if err != nil {
		e.t.Fatal(err)
	}
}

// account opens an account with an IDR user ledger and returns both ids.
func (e *env) account(email string) (accountID, ledgerID string) {
	e.t.Helper()
	accountID, ledgerID = ledger.NewID(), ledger.NewID()
	userID := e.user(email)
	e.must(e.inTx(func(tx repository.LedgerTx) error {
		if err := tx.InsertAccount(e.ctx, repository.AccountRecord{ID: accountID, UserID: userID, Status: ledger.AccountStatusActive}); err != nil {
			return err
		}
		return tx.InsertLedger(e.ctx, repository.LedgerRecord{ID: ledgerID, Kind: repository.LedgerUser, AccountID: accountID, Currency: "IDR"})
	}))
	return accountID, ledgerID
}

func (e *env) systemLedger(code string) string {
	e.t.Helper()
	var id string
	e.must(e.inTx(func(tx repository.LedgerTx) error {
		sl, err := tx.GetSystemLedger(e.ctx, code, "IDR")
		id = sl.ID
		return err
	}))
	return id
}

func pendingTx(accountID, counterpartyID string, typ ledger.TransactionType, amount int64) repository.TransactionRecord {
	return repository.TransactionRecord{
		ID: ledger.NewID(), AccountID: accountID, CounterpartyLedgerID: counterpartyID,
		Type: typ, Status: ledger.StatusPending, Amount: amount, Currency: "IDR",
		RequestedBy: "payments", Version: 1, TraceID: traceID,
	}
}

func TestAccountsAndLedgers(t *testing.T) {
	e := newEnv(t)
	accountID, ledgerID := e.account("a@example.com")

	if _, err := e.repo.GetAccount(e.ctx, ledger.NewID()); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("GetAccount(missing) = %v, want ErrNotFound", err)
	}
	acct, err := e.repo.GetAccount(e.ctx, accountID)
	if err != nil || acct.Status != ledger.AccountStatusActive {
		t.Fatalf("GetAccount = %+v, %v", acct, err)
	}
	if got, err := e.repo.GetAccountByUserID(e.ctx, acct.UserID); err != nil || got.ID != accountID {
		t.Fatalf("GetAccountByUserID = %+v, %v", got, err)
	}

	err = e.inTx(func(tx repository.LedgerTx) error {
		return tx.InsertAccount(e.ctx, repository.AccountRecord{ID: ledger.NewID(), UserID: acct.UserID, Status: ledger.AccountStatusActive})
	})
	if !errors.Is(err, repository.ErrAccountExists) {
		t.Errorf("second account: %v", err)
	}
	err = e.inTx(func(tx repository.LedgerTx) error {
		return tx.InsertAccount(e.ctx, repository.AccountRecord{ID: ledger.NewID(), UserID: ledger.NewID(), Status: ledger.AccountStatusActive})
	})
	if !errors.Is(err, repository.ErrUserNotFound) {
		t.Errorf("unknown user: %v", err)
	}
	err = e.inTx(func(tx repository.LedgerTx) error {
		return tx.InsertLedger(e.ctx, repository.LedgerRecord{ID: ledger.NewID(), Kind: repository.LedgerUser, AccountID: accountID, Currency: "IDR"})
	})
	if !errors.Is(err, repository.ErrLedgerExists) {
		t.Errorf("second IDR ledger: %v", err)
	}

	ledgers, err := e.repo.ListUserLedgers(e.ctx, accountID)
	if err != nil || len(ledgers) != 1 || ledgers[0].ID != ledgerID || ledgers[0].Kind != repository.LedgerUser {
		t.Fatalf("ListUserLedgers = %+v, %v", ledgers, err)
	}
	if e.systemLedger(ledger.CounterpartyTopupClearing) == "" {
		t.Fatal("no seeded topup_clearing ledger")
	}
	err = e.inTx(func(tx repository.LedgerTx) error {
		_, err := tx.LockUserLedger(e.ctx, accountID, "USD")
		return err
	})
	if !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("missing USD ledger: %v", err)
	}
	if err := e.inTx(func(tx repository.LedgerTx) error {
		return tx.UpdateAccount(e.ctx, ledger.NewID(), ledger.AccountStatusFrozen, 2)
	}); !errors.Is(err, repository.ErrNotFound) {
		t.Errorf("update missing account: %v", err)
	}
}

func TestInTxRollsBackOnError(t *testing.T) {
	e := newEnv(t)
	userID := e.user("r@example.com")
	boom := errors.New("boom")
	err := e.inTx(func(tx repository.LedgerTx) error {
		if err := tx.InsertAccount(e.ctx, repository.AccountRecord{ID: ledger.NewID(), UserID: userID, Status: ledger.AccountStatusActive}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if _, err := e.repo.GetAccountByUserID(e.ctx, userID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("account survived rollback: %v", err)
	}
}

// TestCreditTx1Tx2 drives one credit through the repository the way the
// service will (TRD §7.1) and checks every table it touches.
func TestCreditTx1Tx2(t *testing.T) {
	e := newEnv(t)
	accountID, ledgerID := e.account("c@example.com")
	topup := e.systemLedger(ledger.CounterpartyTopupClearing)
	rec := pendingTx(accountID, topup, ledger.Credit, 150000)
	key := repository.IdempotencyKey{ClientID: "payments", KeyHash: strings.Repeat("a", 64), Fingerprint: strings.Repeat("b", 64),
		Operation: "credit", ResourceID: rec.ID, ExpiresAt: time.Now().Add(24 * time.Hour)}
	ev := func(v int64, p ledger.Payload) ledger.Event {
		return ledger.Event{ID: ledger.NewID(), StreamID: rec.ID, StreamVersion: v, AccountID: accountID, Payload: p, TraceID: traceID}
	}

	// Tx1: claim the key, record the request.
	e.must(e.inTx(func(tx repository.LedgerTx) error {
		if ok, err := tx.InsertIdempotencyKey(e.ctx, key); err != nil || !ok {
			t.Fatalf("claim key: %v, %v", ok, err)
		}
		if err := tx.InsertTransaction(e.ctx, rec); err != nil {
			return err
		}
		_, err := tx.AppendEvent(e.ctx, ev(1, ledger.TransactionRequested{Type: ledger.Credit, Amount: rec.Amount, Currency: "IDR",
			Counterparty: ledger.CounterpartyTopupClearing, RequestedBy: "payments"}), "")
		return err
	}))

	// Tx2: lock, decide, post, settle.
	e.must(e.inTx(func(tx repository.LedgerTx) error {
		locked, err := tx.LockTransaction(e.ctx, rec.ID)
		if err != nil || locked.Status != ledger.StatusPending {
			t.Fatalf("lock transaction: %+v, %v", locked, err)
		}
		acct, err := tx.LockAccount(e.ctx, accountID, repository.LockShare)
		if err != nil {
			return err
		}
		l, err := tx.LockUserLedger(e.ctx, accountID, "IDR")
		if err != nil {
			return err
		}
		if _, err := tx.AppendEvent(e.ctx, ev(2, ledger.TransactionProcessing{LedgerID: l.ID}), l.ID); err != nil {
			return err
		}
		d, err := ledger.Decide(ledger.DecideInput{
			Tx:            ledger.Transaction{ID: rec.ID, AccountID: accountID, Status: ledger.StatusPending, Type: ledger.Credit, Amount: rec.Amount, Currency: "IDR"},
			AccountStatus: acct.Status, LedgerFound: true, Balance: l.Balance, OverdraftLimit: l.OverdraftLimit,
		})
		if err != nil || !d.Settle {
			t.Fatalf("decide: %+v, %v", d, err)
		}
		seq := l.LastSequence + 1
		userLine := repository.JournalLine{ID: ledger.NewID(), TransactionID: rec.ID, LedgerID: l.ID, Amount: d.Delta,
			Currency: "IDR", LedgerSequence: &seq, BalanceAfter: &d.BalanceAfter, TraceID: traceID}
		sysLine := repository.JournalLine{ID: ledger.NewID(), TransactionID: rec.ID, LedgerID: topup, Amount: -d.Delta,
			Currency: "IDR", TraceID: traceID}
		if err := tx.PostJournal(e.ctx, userLine, sysLine); err != nil {
			return err
		}
		if err := tx.UpdateLedgerBalance(e.ctx, l.ID, d.BalanceAfter, seq); err != nil {
			return err
		}
		if _, err := tx.AppendEvent(e.ctx, ev(3, ledger.TransactionSettled{JournalEntryIDs: []string{userLine.ID, sysLine.ID},
			LedgerSequence: seq, BalanceAfter: d.BalanceAfter}), l.ID); err != nil {
			return err
		}
		updated, err := tx.UpdateTransaction(e.ctx, repository.TransactionUpdate{ID: rec.ID, Status: ledger.StatusSettled, Version: 3, LedgerID: l.ID, Settled: true})
		if err != nil {
			return err
		}
		if updated.Status != ledger.StatusSettled || updated.SettledAt == nil || updated.Version != 3 {
			t.Fatalf("UpdateTransaction returned %+v", updated)
		}
		done, err := tx.CompleteIdempotencyKey(e.ctx, key.ClientID, key.KeyHash, 201, []byte(`{"transaction":{"id":"`+rec.ID+`"}}`))
		if err == nil && (done.Fingerprint != key.Fingerprint || done.Operation != key.Operation || !done.ExpiresAt.Equal(key.ExpiresAt.Truncate(time.Microsecond))) {
			t.Fatalf("CompleteIdempotencyKey returned %+v, want the claimed row %+v", done, key)
		}
		return err
	}))

	got, err := e.repo.GetTransaction(e.ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ledger.StatusSettled || got.LedgerID != ledgerID || got.LedgerSequence != 1 ||
		got.BalanceAfter != 150000 || got.SettledAt == nil || got.Version != 3 || got.TraceID != traceID {
		t.Fatalf("GetTransaction = %+v", got)
	}
	ledgers, _ := e.repo.ListUserLedgers(e.ctx, accountID)
	if ledgers[0].Balance != 150000 || ledgers[0].LastSequence != 1 {
		t.Fatalf("ledger projection = %+v", ledgers[0])
	}

	// The projection equals the fold of the stored stream.
	events, err := e.repo.ListStreamEvents(e.ctx, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	folded, err := ledger.FoldTransaction(events)
	if err != nil {
		t.Fatal(err)
	}
	if folded.Status != got.Status || folded.Version != got.Version || folded.BalanceAfter != got.BalanceAfter ||
		folded.LedgerSequence != got.LedgerSequence || folded.LedgerID != got.LedgerID {
		t.Fatalf("fold %+v != projection %+v", folded, got)
	}

	// One outbox row per event, keyed by account, carrying the trace.
	rows, err := e.pool.Query(e.ctx, `SELECT o.msg_key, o.topic, o.payload, o.headers, ev.stream_version
		FROM ledger.outbox o JOIN ledger.events ev USING (event_id) WHERE ev.stream_id = $1 ORDER BY o.id`, rec.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var key, topic string
		var payload, headers []byte
		var version int64
		e.must(rows.Scan(&key, &topic, &payload, &headers, &version))
		var envlp outbox.Envelope
		e.must(json.Unmarshal(payload, &envlp))
		n++
		if key != accountID || topic != outbox.DefaultTopic || envlp.StreamVersion != version || envlp.TraceID != traceID ||
			!strings.Contains(string(headers), traceID) || envlp.OccurredAt.IsZero() {
			t.Errorf("outbox row %d: key %s topic %s envelope %+v headers %s", n, key, topic, envlp, headers)
		}
		if version >= 2 && envlp.LedgerID != ledgerID {
			t.Errorf("outbox row %d: ledger_id %q", n, envlp.LedgerID)
		}
	}
	if n != 3 {
		t.Fatalf("outbox rows = %d, want 3", n)
	}

	var k repository.IdempotencyKey
	e.must(e.inTx(func(tx repository.LedgerTx) error {
		var err error
		k, err = tx.GetIdempotencyKey(e.ctx, key.ClientID, key.KeyHash)
		return err
	}))
	if k.ResponseStatus != 201 || !strings.Contains(string(k.ResponseBody), rec.ID) || k.ResourceID != rec.ID {
		t.Fatalf("idempotency key = %+v", k)
	}
}

func TestUnbalancedJournalFailsAtCommit(t *testing.T) {
	e := newEnv(t)
	accountID, ledgerID := e.account("u@example.com")
	topup := e.systemLedger(ledger.CounterpartyTopupClearing)
	rec := pendingTx(accountID, topup, ledger.Credit, 10)
	seq, after := int64(1), int64(10)
	err := e.inTx(func(tx repository.LedgerTx) error {
		if err := tx.InsertTransaction(e.ctx, rec); err != nil {
			return err
		}
		return tx.PostJournal(e.ctx,
			repository.JournalLine{ID: ledger.NewID(), TransactionID: rec.ID, LedgerID: ledgerID, Amount: 10, Currency: "IDR", LedgerSequence: &seq, BalanceAfter: &after, TraceID: traceID},
			repository.JournalLine{ID: ledger.NewID(), TransactionID: rec.ID, LedgerID: topup, Amount: -9, Currency: "IDR", TraceID: traceID})
	})
	if err == nil || !strings.Contains(err.Error(), "unbalanced") {
		t.Fatalf("err = %v, want unbalanced at commit", err)
	}
	if _, err := e.repo.GetTransaction(e.ctx, rec.ID); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("transaction survived the failed commit: %v", err)
	}
}

// The relay is woken after a commit that wrote outbox rows, never from
// inside the write transaction (the NOTIFY lock serialized every commit), and
// not at all for a rollback or a transaction without outbox rows.
func TestOutboxCommitNotifiesRelay(t *testing.T) {
	e := newEnv(t)
	accountID, _ := e.account("n@example.com")
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	notifier := outbox.NewNotifier(e.pool, time.Millisecond)
	go notifier.Run(ctx)
	repo := repository.NewLedgerRepo(e.pool, outbox.DefaultTopic, notifier)

	conn, err := e.pool.Acquire(e.ctx)
	e.must(err)
	defer conn.Release()
	_, err = conn.Exec(e.ctx, "LISTEN "+outbox.NotifyChannel)
	e.must(err)
	wait := func(d time.Duration) error {
		wctx, cancel := context.WithTimeout(e.ctx, d)
		defer cancel()
		_, err := conn.Conn().WaitForNotification(wctx)
		return err
	}
	appendEvent := func(tx repository.LedgerTx, version int64) error {
		_, err := tx.AppendEvent(e.ctx, ledger.Event{StreamID: accountID, StreamVersion: version, AccountID: accountID,
			Payload: ledger.AccountOpened{UserID: "u"}, TraceID: traceID}, "")
		return err
	}

	rollback := errors.New("rollback")
	if err := repo.InTx(e.ctx, time.Second, func(tx repository.LedgerTx) error {
		e.must(appendEvent(tx, 1))
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatalf("rolled back tx: %v", err)
	}
	e.must(repo.InTx(e.ctx, time.Second, func(tx repository.LedgerTx) error {
		_, err := tx.GetAccountByUserID(e.ctx, "00000000-0000-0000-0000-000000000000")
		if errors.Is(err, repository.ErrNotFound) {
			return nil
		}
		return err
	}))
	if err := wait(200 * time.Millisecond); err == nil {
		t.Fatal("notified after a rollback or a transaction without outbox rows")
	}

	// Woken by the notification, the relay must already see the row: the
	// commit happens before the NOTIFY, and nothing else notifies here.
	committed := make(chan error, 1)
	go func() {
		committed <- repo.InTx(e.ctx, time.Second, func(tx repository.LedgerTx) error { return appendEvent(tx, 1) })
	}()
	if err := wait(2 * time.Second); err != nil {
		t.Fatalf("no notification after commit: %v", err)
	}
	var visible int
	e.must(e.pool.QueryRow(e.ctx, `SELECT count(*) FROM ledger.outbox WHERE msg_key = $1`, accountID).Scan(&visible))
	if visible != 1 {
		t.Fatalf("notified while the outbox row was not yet committed (%d visible)", visible)
	}
	e.must(<-committed)
}

func TestIdempotencyKeyLifecycle(t *testing.T) {
	e := newEnv(t)
	k := repository.IdempotencyKey{ClientID: "payments", KeyHash: strings.Repeat("c", 64), Fingerprint: strings.Repeat("d", 64),
		Operation: "debit", ResourceID: ledger.NewID(), ExpiresAt: time.Now().Add(time.Hour)}
	claim := func(k repository.IdempotencyKey) bool {
		var ok bool
		e.must(e.inTx(func(tx repository.LedgerTx) error {
			var err error
			ok, err = tx.InsertIdempotencyKey(e.ctx, k)
			return err
		}))
		return ok
	}
	get := func() repository.IdempotencyKey {
		var got repository.IdempotencyKey
		e.must(e.inTx(func(tx repository.LedgerTx) error {
			var err error
			got, err = tx.GetIdempotencyKey(e.ctx, k.ClientID, k.KeyHash)
			return err
		}))
		return got
	}
	complete := func() error {
		return e.inTx(func(tx repository.LedgerTx) error {
			_, err := tx.CompleteIdempotencyKey(e.ctx, k.ClientID, k.KeyHash, 422, []byte(`{"error":{}}`))
			return err
		})
	}

	if !claim(k) {
		t.Fatal("first claim failed")
	}
	if claim(k) {
		t.Fatal("second claim succeeded")
	}
	if got := get(); got.ResponseBody != nil || got.ResponseStatus != 0 || got.ResourceID != k.ResourceID {
		t.Fatalf("in-flight key = %+v", got)
	}
	e.must(complete())
	if err := complete(); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("second completion: %v", err)
	}
	if got := get(); got.ResponseStatus != 422 {
		t.Fatalf("completed key = %+v", got)
	}

	// An expired key is taken over by a new claim, with its response cleared.
	_, err := e.pool.Exec(e.ctx, `UPDATE ledger.idempotency_keys SET created_at = now() - interval '2 hours',
		expires_at = now() - interval '1 hour'`)
	e.must(err)
	k2 := k
	k2.ResourceID, k2.Operation = ledger.NewID(), "credit"
	if !claim(k2) {
		t.Fatal("expired key was not taken over")
	}
	if got := get(); got.ResourceID != k2.ResourceID || got.ResponseBody != nil || got.Operation != "credit" {
		t.Fatalf("taken-over key = %+v", got)
	}
}

func TestConcurrentIdempotencyClaimsHaveOneWinner(t *testing.T) {
	e := newEnv(t)
	k := repository.IdempotencyKey{ClientID: "payments", KeyHash: strings.Repeat("e", 64), Fingerprint: strings.Repeat("f", 64),
		Operation: "debit", ResourceID: ledger.NewID(), ExpiresAt: time.Now().Add(time.Hour)}
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		wins int
	)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := e.inTx(func(tx repository.LedgerTx) error {
				ok, err := tx.InsertIdempotencyKey(e.ctx, k)
				if ok {
					mu.Lock()
					wins++
					mu.Unlock()
				}
				return err
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("winners = %d, want 1", wins)
	}
}

// FOR SHARE (every Tx2) doesn't block another FOR SHARE, but blocks FOR
// UPDATE (freeze), which then times out with ErrTimeout (TRD §7.1).
func TestAccountLockModesAndTimeout(t *testing.T) {
	e := newEnv(t)
	accountID, _ := e.account("l@example.com")

	holder, err := e.pool.Begin(e.ctx)
	e.must(err)
	defer holder.Rollback(e.ctx)
	_, err = holder.Exec(e.ctx, `SELECT 1 FROM ledger.accounts WHERE id = $1 FOR SHARE`, accountID)
	e.must(err)

	if err := e.repo.InTx(e.ctx, 200*time.Millisecond, func(tx repository.LedgerTx) error {
		_, err := tx.LockAccount(e.ctx, accountID, repository.LockShare)
		return err
	}); err != nil {
		t.Fatalf("share lock blocked by another share lock: %v", err)
	}

	start := time.Now()
	err = e.repo.InTx(e.ctx, 200*time.Millisecond, func(tx repository.LedgerTx) error {
		_, err := tx.LockAccount(e.ctx, accountID, repository.LockUpdate)
		return err
	})
	if !errors.Is(err, repository.ErrTimeout) {
		t.Fatalf("update lock under a share lock: err = %v, want ErrTimeout", err)
	}
	if waited := time.Since(start); waited > 2*time.Second {
		t.Fatalf("timeout took %v", waited)
	}
}

func TestSecondActiveVoidIsRejected(t *testing.T) {
	e := newEnv(t)
	accountID, _ := e.account("v@example.com")
	settle := e.systemLedger(ledger.CounterpartySettlement)
	orig := pendingTx(accountID, settle, ledger.Debit, 5)
	e.must(e.inTx(func(tx repository.LedgerTx) error { return tx.InsertTransaction(e.ctx, orig) }))

	void := func() error {
		v := pendingTx(accountID, settle, ledger.Void, 5)
		v.OriginalTransactionID = orig.ID
		return e.inTx(func(tx repository.LedgerTx) error { return tx.InsertTransaction(e.ctx, v) })
	}
	e.must(void())
	if err := void(); !errors.Is(err, repository.ErrVoidExists) {
		t.Fatalf("second void: %v", err)
	}
}

func TestListTransactionsKeysetPagination(t *testing.T) {
	e := newEnv(t)
	accountID, _ := e.account("p@example.com")
	topup := e.systemLedger(ledger.CounterpartyTopupClearing)
	var ids []string
	for range 5 {
		rec := pendingTx(accountID, topup, ledger.Credit, 1)
		e.must(e.inTx(func(tx repository.LedgerTx) error { return tx.InsertTransaction(e.ctx, rec) }))
		ids = append([]string{rec.ID}, ids...) // newest first
	}

	var seen []string
	q := repository.TransactionQuery{AccountID: accountID, Limit: 2}
	for page := 0; page < 5; page++ {
		rows, err := e.repo.ListTransactions(e.ctx, q)
		e.must(err)
		for _, r := range rows {
			seen = append(seen, r.ID)
		}
		if len(rows) < q.Limit {
			break
		}
		last := rows[len(rows)-1]
		q.After = &repository.TransactionCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}
	if strings.Join(seen, ",") != strings.Join(ids, ",") {
		t.Fatalf("pages = %v, want %v", seen, ids)
	}

	q = repository.TransactionQuery{AccountID: accountID, Currency: "USD", Limit: 10}
	if rows, err := e.repo.ListTransactions(e.ctx, q); err != nil || len(rows) != 0 {
		t.Fatalf("USD filter = %d rows, %v", len(rows), err)
	}
}
