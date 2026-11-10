//go:build integration

package service_test

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"fintech-ledger-playground/internal/service"
)

// openUser opens an account for a new user and returns (user id, account id).
func (e *svcEnv) openUser(email, key string) (string, string) {
	e.t.Helper()
	uid := e.user(email)
	r, err := e.svc.OpenAccount(e.ctx, service.OpenAccountCommand{Request: req("onboarding", key), UserID: uid})
	if err != nil || r.Status != http.StatusCreated {
		e.t.Fatalf("open account: %d %s %v", r.Status, r.Body, err)
	}
	return uid, decode[accountBody](e.t, r).Account.ID
}

func (e *svcEnv) credit(key, accountID string, amount int64) string {
	e.t.Helper()
	r, err := e.money(e.svc.Credit, key, accountID, amount, "IDR")
	if err != nil || r.Status != http.StatusCreated {
		e.t.Fatalf("credit: %d %s %v", r.Status, r.Body, err)
	}
	return decode[txBody](e.t, r).Transaction.ID
}

func TestUserTransactionsPaginateStably(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	uid, acct := e.openUser("pager@x.io", "open-pager")
	var want []string // newest first
	for i := range 5 {
		want = append([]string{e.credit(fmt.Sprintf("cr-%d", i), acct, int64(100+i))}, want...)
	}

	var got []string
	cursor := ""
	for page := 0; ; page++ {
		p, err := e.svc.UserTransactions(e.ctx, uid, service.TransactionPageQuery{Limit: 2, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, v := range p.Transactions {
			got = append(got, v.ID)
			if v.BalanceAfter == nil || v.AccountID != acct {
				t.Fatalf("page %d: %+v", page, v)
			}
		}
		if page == 0 {
			// A write between pages lands before the cursor and must not
			// shift or repeat what the next pages return.
			e.credit("cr-late", acct, 1)
		}
		if p.NextCursor == "" {
			if len(p.Transactions) != 1 {
				t.Fatalf("last page has %d rows, want 1", len(p.Transactions))
			}
			break
		}
		cursor = p.NextCursor
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("pages = %v\nwant    %v", got, want)
	}

	// The first page now starts with the late credit.
	p, err := e.svc.UserTransactions(e.ctx, uid, service.TransactionPageQuery{Limit: 100})
	if err != nil || len(p.Transactions) != 6 || p.NextCursor != "" || p.Transactions[0].Amount != 1 {
		t.Fatalf("full page: %+v %v", p, err)
	}
	// An exact page leaves no cursor behind.
	if p, _ := e.svc.UserTransactions(e.ctx, uid, service.TransactionPageQuery{Limit: 6}); p.NextCursor != "" {
		t.Fatalf("exact page has next_cursor %q", p.NextCursor)
	}
	// The currency filter applies.
	if p, _ := e.svc.UserTransactions(e.ctx, uid, service.TransactionPageQuery{Currency: "USD"}); len(p.Transactions) != 0 {
		t.Fatalf("USD page: %+v", p)
	}
}

func TestUserReadsAreScopedToTheCaller(t *testing.T) {
	e := newSvcEnv(t, 2*time.Second)
	alice, aliceAcct := e.openUser("alice@x.io", "open-alice")
	bob, _ := e.openUser("bob@x.io", "open-bob")
	stranger := e.user("stranger@x.io") // a user with no ledger account
	txID := e.credit("cr-alice", aliceAcct, 500)

	if v, err := e.svc.UserTransaction(e.ctx, alice, txID); err != nil || v.ID != txID {
		t.Fatalf("own transaction: %+v %v", v, err)
	}
	for name, uid := range map[string]string{"other user": bob, "no account": stranger} {
		if _, err := e.svc.UserTransaction(e.ctx, uid, txID); !errors.Is(err, service.ErrTransactionNotFound) {
			t.Errorf("%s: err = %v, want ErrTransactionNotFound", name, err)
		}
	}
	if p, err := e.svc.UserTransactions(e.ctx, bob, service.TransactionPageQuery{}); err != nil || len(p.Transactions) != 0 {
		t.Fatalf("bob sees %+v %v", p, err)
	}
	if _, err := e.svc.UserTransactions(e.ctx, stranger, service.TransactionPageQuery{}); !errors.Is(err, service.ErrAccountNotFound) {
		t.Fatalf("no account: err = %v", err)
	}
	// Read-your-writes: the balance read straight after a credit includes it.
	v, err := e.svc.AccountByUser(e.ctx, alice)
	if err != nil || len(v.Ledgers) != 1 || v.Ledgers[0].Balance != 500 {
		t.Fatalf("balance: %+v %v", v, err)
	}
}
