package fraud

import (
	"sort"
	"time"
)

// Rule names, from the fixed set in the logging catalog (`rule`).
const (
	RuleTopupVelocity = "topup_velocity"
	RuleDebitVolume   = "debit_volume"
)

// Config holds the thresholds (TRD §16 Q7). A rule fires on more than Max
// events, or more than the currency's limit in sum, inside any window.
type Config struct {
	TopupMax      int              // FRAUD_TOPUP_MAX: more than this many top-ups...
	TopupWindow   time.Duration    // FRAUD_TOPUP_WINDOW: ...within this long
	DebitWindow   time.Duration    // FRAUD_DEBIT_WINDOW
	DebitMaxCount int              // FRAUD_DEBIT_MAX_COUNT
	DebitLimits   map[string]int64 // FRAUD_DEBIT_LIMITS: per currency, minor units; none = count only
}

// Point is one event in a window: its transaction, event time and amount.
type Point struct {
	TransactionID string
	At            time.Time
	Amount        int64
}

// Decision is a rule firing.
type Decision struct {
	Rule        string
	AccountID   string
	Currency    string    // debit_volume only
	WindowStart time.Time // event time of the earliest event in the offending window
	WindowEnd   time.Time
	Count       int
	Sum         int64
}

// limits for one evaluation: count > MaxCount, or sum > MaxSum when MaxSum > 0.
type limits struct {
	MaxCount int
	MaxSum   int64
}

// firstBreach finds the earliest-ending window of length w that contains t
// and breaks lim. Windows are half-open: (end − w, end]. Because events can
// arrive out of order, a late event at t can complete a window that ends at
// a later event, so every window ending in [t, t+w) is checked. points need
// not be sorted and may repeat a transaction (replays), which counts once.
func firstBreach(points []Point, t time.Time, w time.Duration, lim limits) (start, end time.Time, count int, sum int64, ok bool) {
	ps := dedupe(points)
	sort.Slice(ps, func(i, j int) bool {
		if !ps[i].At.Equal(ps[j].At) {
			return ps[i].At.Before(ps[j].At)
		}
		return ps[i].TransactionID < ps[j].TransactionID
	})
	lo := 0
	for hi, e := range ps {
		for !ps[lo].At.After(e.At.Add(-w)) { // drop points at or before end − w
			lo++
		}
		if e.At.Before(t) || !e.At.Before(t.Add(w)) {
			continue // this window doesn't contain t
		}
		if hi+1 < len(ps) && ps[hi+1].At.Equal(e.At) {
			continue // evaluate a timestamp once, with all its points
		}
		var s int64
		for _, p := range ps[lo : hi+1] {
			s += p.Amount
		}
		n := hi + 1 - lo
		if n > lim.MaxCount || (lim.MaxSum > 0 && s > lim.MaxSum) {
			return ps[lo].At, e.At, n, s, true
		}
	}
	return time.Time{}, time.Time{}, 0, 0, false
}

func dedupe(points []Point) []Point {
	seen := make(map[string]bool, len(points))
	out := make([]Point, 0, len(points))
	for _, p := range points {
		if !seen[p.TransactionID] {
			seen[p.TransactionID] = true
			out = append(out, p)
		}
	}
	return out
}

// TopupVelocity: more than cfg.TopupMax settled credits on one account
// within cfg.TopupWindow, in a window containing the new credit at t.
func TopupVelocity(accountID string, points []Point, t time.Time, cfg Config) (Decision, bool) {
	start, end, n, sum, ok := firstBreach(points, t, cfg.TopupWindow, limits{MaxCount: cfg.TopupMax})
	if !ok {
		return Decision{}, false
	}
	return Decision{Rule: RuleTopupVelocity, AccountID: accountID, WindowStart: start, WindowEnd: end, Count: n, Sum: sum}, true
}

// DebitVolume: within cfg.DebitWindow, more than cfg.DebitMaxCount settled
// debits, or more than the currency's limit in total, on one account.
func DebitVolume(accountID, currency string, points []Point, t time.Time, cfg Config) (Decision, bool) {
	start, end, n, sum, ok := firstBreach(points, t, cfg.DebitWindow,
		limits{MaxCount: cfg.DebitMaxCount, MaxSum: cfg.DebitLimits[currency]})
	if !ok {
		return Decision{}, false
	}
	return Decision{Rule: RuleDebitVolume, AccountID: accountID, Currency: currency, WindowStart: start, WindowEnd: end,
		Count: n, Sum: sum}, true
}
