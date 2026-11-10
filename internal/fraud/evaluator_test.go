package fraud_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/fraud"
	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/outbox"
)

func newEvaluator(t *testing.T) (*fraud.Evaluator, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	ev, err := fraud.NewEvaluator(fraud.NewRedisWindowStore(rdb), cfg())
	if err != nil {
		t.Fatal(err)
	}
	return ev, mr
}

func credit(id string, at time.Time) fraud.Settlement {
	return fraud.Settlement{EventID: "ev-" + id, AccountID: "acct-1", TransactionID: id, Type: ledger.Credit,
		Amount: 10, Currency: "IDR", OccurredAt: at}
}

func TestEvaluatorTopupsAndReplay(t *testing.T) {
	ev, mr := newEvaluator(t)
	ctx := context.Background()
	var fired []fraud.Decision
	run := func() {
		fired = nil
		for i, id := range []string{"a", "b", "c", "d"} {
			d, ok, err := ev.Evaluate(ctx, credit(id, t0.Add(time.Duration(i)*time.Minute)))
			if err != nil {
				t.Fatal(err)
			}
			if ok {
				fired = append(fired, d)
			}
		}
	}
	run()
	if len(fired) != 1 || fired[0].Rule != fraud.RuleTopupVelocity || fired[0].Count != 4 || !fired[0].WindowStart.Equal(t0) {
		t.Fatalf("decisions = %+v", fired)
	}
	first := fired[0]
	// Replaying the feed: the store already holds all four, so each replayed
	// event sees the full window and fires, always with the same window, so
	// the freeze idempotency key (rule, account, window start) is the same
	// and the freeze is replayed, not repeated.
	run()
	if len(fired) != 4 {
		t.Fatalf("replay decisions = %+v", fired)
	}
	for _, d := range fired {
		if !d.WindowStart.Equal(first.WindowStart) || !d.WindowEnd.Equal(first.WindowEnd) || d.Count != 4 {
			t.Fatalf("replay changed the decision: %+v vs %+v", d, first)
		}
	}
	if m, _ := mr.ZMembers("fraud:topup:acct-1"); len(m) != 4 {
		t.Fatalf("ZSET has %d members after replay, want 4", len(m))
	}
	if ttl := mr.TTL("fraud:topup:acct-1"); ttl != 10*time.Minute {
		t.Fatalf("TTL = %v, want 2× the window", ttl)
	}
}

func TestEvaluatorTrimsAndSeparatesKeys(t *testing.T) {
	ev, mr := newEvaluator(t)
	ctx := context.Background()
	ev.Evaluate(ctx, credit("old", t0))
	ev.Evaluate(ctx, credit("new", t0.Add(11*time.Minute))) // older than 2× window: trimmed
	if m, _ := mr.ZMembers("fraud:topup:acct-1"); len(m) != 1 {
		t.Fatalf("old point not trimmed: %d members", len(m))
	}
	debit := func(id, cur string, amount int64) (fraud.Decision, bool) {
		d, ok, err := ev.Evaluate(ctx, fraud.Settlement{EventID: id, AccountID: "acct-1", TransactionID: id, Type: ledger.Debit,
			Amount: amount, Currency: cur, OccurredAt: t0})
		if err != nil {
			t.Fatal(err)
		}
		return d, ok
	}
	debit("u1", "USD", 40_000_000)
	if _, ok := debit("i1", "IDR", 40_000_000); ok {
		t.Fatal("IDR fired on 40M; the USD debit must not count towards it")
	}
	if d, ok := debit("i2", "IDR", 10_000_001); !ok || d.Currency != "IDR" || d.Sum != 50_000_001 {
		t.Fatalf("IDR over the limit: %+v %v", d, ok)
	}
	if !mr.Exists("fraud:debit:acct-1:USD") || !mr.Exists("fraud:debit:acct-1:IDR") {
		t.Fatal("debit windows are not per currency")
	}
	// Voids are ignored.
	if _, ok, err := ev.Evaluate(ctx, fraud.Settlement{Type: ledger.Void, AccountID: "acct-1"}); ok || err != nil {
		t.Fatal("void evaluated")
	}
}

func TestEvaluatorRedisDown(t *testing.T) {
	ev, mr := newEvaluator(t)
	mr.Close()
	if _, _, err := ev.Evaluate(context.Background(), credit("x", t0)); err == nil {
		t.Fatal("no error with Redis down")
	}
}

func TestNewEvaluatorRejectsBadConfig(t *testing.T) {
	for name, mut := range map[string]func(*fraud.Config){
		"zero topup max":   func(c *fraud.Config) { c.TopupMax = 0 },
		"no topup window":  func(c *fraud.Config) { c.TopupWindow = 0 },
		"no debit window":  func(c *fraud.Config) { c.DebitWindow = 0 },
		"zero debit count": func(c *fraud.Config) { c.DebitMaxCount = 0 },
		"negative limit":   func(c *fraud.Config) { c.DebitLimits = map[string]int64{"IDR": -1} },
	} {
		c := cfg()
		mut(&c)
		if _, err := fraud.NewEvaluator(nil, c); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

// The parser reads exactly what the outbox writes.
func TestParseSettlement(t *testing.T) {
	enc := func(p ledger.Payload) []byte {
		t.Helper()
		m, err := outbox.Encode(ledger.Event{ID: "ev-1", StreamID: "tx-1", StreamVersion: 3, AccountID: "acct-1",
			Payload: p, TraceID: "4bf92f3577b34da6a3ce929d0e0e4736", OccurredAt: t0}, "ledger-1")
		if err != nil {
			t.Fatal(err)
		}
		return m.Value
	}
	s, ok, err := fraud.ParseSettlement(enc(ledger.TransactionSettled{LedgerSequence: 1, BalanceAfter: 10,
		Type: ledger.Credit, Amount: 10, Currency: "IDR"}))
	if err != nil || !ok || s.EventID != "ev-1" || s.TransactionID != "tx-1" || s.AccountID != "acct-1" ||
		s.Type != ledger.Credit || s.Amount != 10 || s.Currency != "IDR" || !s.OccurredAt.Equal(t0) || s.TraceID == "" {
		t.Fatalf("settled credit: %+v %v %v", s, ok, err)
	}
	for name, v := range map[string][]byte{
		"other event type":     enc(ledger.TransactionFailed{Reason: "insufficient_funds"}),
		"settled void":         enc(ledger.TransactionSettled{Type: ledger.Void, Amount: 10, Currency: "IDR"}),
		"pre-P4.1 settlement":  enc(ledger.TransactionSettled{LedgerSequence: 1, BalanceAfter: 10}),
		"account stream event": enc(ledger.AccountFrozen{Reason: "r", Actor: "ops"}),
	} {
		if _, ok, err := fraud.ParseSettlement(v); ok || err != nil {
			t.Errorf("%s: ok=%v err=%v", name, ok, err)
		}
	}
	if _, _, err := fraud.ParseSettlement([]byte("{not json")); !errors.Is(err, fraud.ErrMalformed) {
		t.Fatalf("malformed: %v", err)
	}
}
