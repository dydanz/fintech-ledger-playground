package fraud_test

import (
	"fmt"
	"testing"
	"time"

	"auth-api/internal/fraud"
)

var t0 = time.Date(2026, 11, 5, 10, 0, 0, 0, time.UTC)

func cfg() fraud.Config {
	return fraud.Config{TopupMax: 3, TopupWindow: 5 * time.Minute, DebitWindow: time.Hour, DebitMaxCount: 30,
		DebitLimits: map[string]int64{"IDR": 50_000_000}}
}

func pts(offsets ...time.Duration) []fraud.Point {
	var out []fraud.Point
	for i, o := range offsets {
		out = append(out, fraud.Point{TransactionID: fmt.Sprintf("tx-%d", i), At: t0.Add(o), Amount: 100})
	}
	return out
}

func TestTopupVelocityWindowEdges(t *testing.T) {
	m := time.Minute
	for _, tc := range []struct {
		name  string
		pts   []fraud.Point
		at    time.Duration
		fires bool
	}{
		{"exactly the max (3) does not fire", pts(0, m, 2*m), 2 * m, false},
		{"4 within 5m fires", pts(0, m, 2*m, 4*m), 4 * m, true},
		{"4th exactly 5m after the 1st: (end-5m, end] excludes it", pts(0, m, 2*m, 5*m), 5 * m, false},
		{"4th 1ms inside the window", pts(0, m, 2*m, 5*m-time.Millisecond), 5*m - time.Millisecond, true},
		{"spread over 15m never has 4 in 5m", pts(0, 4*m, 8*m, 12*m), 12 * m, false},
	} {
		d, ok := fraud.TopupVelocity("acct", tc.pts, t0.Add(tc.at), cfg())
		if ok != tc.fires {
			t.Errorf("%s: fired=%v (%+v)", tc.name, ok, d)
		}
	}
}

func TestTopupVelocityOutOfOrderAndDuplicates(t *testing.T) {
	m := time.Minute
	// Events at 0, 2m and 4m arrived; a late one at 1m completes the window
	// that ends at 4m, which is after the late event's own time.
	d, ok := fraud.TopupVelocity("acct", pts(0, 2*m, 4*m, m), t0.Add(m), cfg())
	if !ok || !d.WindowEnd.Equal(t0.Add(4*m)) || !d.WindowStart.Equal(t0) || d.Count != 4 {
		t.Fatalf("late event: %+v %v", d, ok)
	}
	// A late event older than every neighbour's window changes nothing.
	if d, ok := fraud.TopupVelocity("acct", pts(10*m, 11*m, 12*m, 0), t0, cfg()); ok {
		t.Fatalf("stale late event fired: %+v", d)
	}
	// Replays of one transaction count once.
	dup := pts(0, m, 2*m)
	dup = append(dup, dup[2], dup[2], dup[1])
	if d, ok := fraud.TopupVelocity("acct", dup, t0.Add(2*m), cfg()); ok {
		t.Fatalf("duplicates counted: %+v", d)
	}
	// Equal timestamps are all in the window.
	same := []fraud.Point{{"a", t0, 1}, {"b", t0, 1}, {"c", t0, 1}, {"d", t0, 1}}
	if d, ok := fraud.TopupVelocity("acct", same, t0, cfg()); !ok || d.Count != 4 {
		t.Fatalf("equal timestamps: %+v %v", d, ok)
	}
}

func TestDebitVolumeCountAndPerCurrencyLimits(t *testing.T) {
	m := time.Minute
	var many []fraud.Point
	for i := range 31 {
		many = append(many, fraud.Point{TransactionID: fmt.Sprint(i), At: t0.Add(time.Duration(i) * m), Amount: 1})
	}
	if d, ok := fraud.DebitVolume("acct", "USD", many, t0.Add(30*m), cfg()); !ok || d.Count != 31 || d.Currency != "USD" {
		t.Fatalf("31 debits in an hour: %+v %v", d, ok)
	}
	if _, ok := fraud.DebitVolume("acct", "USD", many[:30], t0.Add(29*m), cfg()); ok {
		t.Fatal("30 debits (the max) fired")
	}

	big := []fraud.Point{{"a", t0, 30_000_000}, {"b", t0.Add(10 * m), 20_000_000}}
	if _, ok := fraud.DebitVolume("acct", "IDR", big, t0.Add(10*m), cfg()); ok {
		t.Fatal("exactly the IDR limit (50,000,000) fired")
	}
	big = append(big, fraud.Point{"c", t0.Add(20 * m), 1})
	if d, ok := fraud.DebitVolume("acct", "IDR", big, t0.Add(20*m), cfg()); !ok || d.Sum != 50_000_001 {
		t.Fatalf("over the IDR limit: %+v %v", d, ok)
	}
	// USD has no configured limit: only the count applies.
	if _, ok := fraud.DebitVolume("acct", "USD", big, t0.Add(20*m), cfg()); ok {
		t.Fatal("USD fired on a sum with no USD limit")
	}
	// The same debits spread over more than the window don't add up.
	spread := []fraud.Point{{"a", t0, 30_000_000}, {"b", t0.Add(61 * m), 30_000_000}}
	if _, ok := fraud.DebitVolume("acct", "IDR", spread, t0.Add(61*m), cfg()); ok {
		t.Fatal("debits 61m apart summed in a 1h window")
	}
}
