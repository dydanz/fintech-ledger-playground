package console_test

import (
	"testing"
	"time"

	"fintech-ledger-playground/internal/console"
)

var t0 = time.Date(2026, 11, 11, 7, 11, 18, 0, time.UTC)

func settledAfter(d time.Duration) *time.Time { at := t0.Add(d); return &at }

// One case per label (spec/2026-11-10 Web Console Design D§6.2), plus the
// overlaps: recovery outranks a business refusal, and a timeout outranks
// recovery (the sweeper failing a transaction is what needs attention).
func TestLabelOf(t *testing.T) {
	now := t0.Add(10 * time.Minute)
	for _, c := range []struct {
		name      string
		tx        console.Tx
		recovered bool
		label     console.Label
		what      string
		attention bool
	}{
		{"normal credit", console.Tx{Type: "CREDIT", Status: "SETTLED", CreatedAt: t0, SettledAt: settledAfter(3 * time.Millisecond)},
			false, console.LabelNormal, "Settled in 3 ms", false},
		{"normal, under a millisecond", console.Tx{Type: "DEBIT", Status: "SETTLED", CreatedAt: t0, SettledAt: settledAfter(400 * time.Microsecond)},
			false, console.LabelNormal, "Settled in under 1 ms", false},
		{"recovered, then settled", console.Tx{Type: "CREDIT", Status: "SETTLED", CreatedAt: t0, SettledAt: settledAfter(47 * time.Second)},
			true, console.LabelRecovered, "Completed by the recovery job after the request couldn't finish on its own", false},
		{"recovered, then refused", console.Tx{Type: "DEBIT", Status: "FAILED", FailureReason: "insufficient_funds", CreatedAt: t0},
			true, console.LabelRecovered, "Completed by the recovery job, which refused it: insufficient funds, balance unchanged", false},
		{"timed out (the sweeper failed it)", console.Tx{Type: "DEBIT", Status: "FAILED", FailureReason: "processing_timeout", CreatedAt: t0},
			true, console.LabelTimedOut, "Couldn't complete in time; failed safely, nothing moved", true},
		{"refused: insufficient funds", console.Tx{Type: "DEBIT", Status: "FAILED", FailureReason: "insufficient_funds", CreatedAt: t0},
			false, console.LabelRejected, "Insufficient funds; balance unchanged", false},
		{"refused: frozen", console.Tx{Type: "DEBIT", Status: "FAILED", FailureReason: "account_frozen", CreatedAt: t0},
			false, console.LabelRejected, "Account frozen by the fraud engine or ops; spending blocked", false},
		{"refused: not voidable", console.Tx{Type: "VOID", Status: "FAILED", FailureReason: "not_voidable", CreatedAt: t0},
			false, console.LabelRejected, "Couldn't be reversed: already reversed, or never settled", false},
		{"reversed original", console.Tx{Type: "CREDIT", Status: "VOIDED", CreatedAt: t0, SettledAt: settledAfter(time.Millisecond)},
			false, console.LabelReversed, "Reversed by a later void; the original stays on record", false},
		{"the void itself", console.Tx{Type: "VOID", Status: "SETTLED", CreatedAt: t0, SettledAt: settledAfter(time.Millisecond)},
			false, console.LabelReversed, "A reversal: a reversing entry was added, nothing was deleted", false},
		{"in progress", console.Tx{Type: "CREDIT", Status: "PENDING", CreatedAt: now.Add(-5 * time.Second)},
			false, console.LabelInProgress, "Processing", false},
		{"stuck in progress", console.Tx{Type: "CREDIT", Status: "PENDING", CreatedAt: now.Add(-90 * time.Second)},
			false, console.LabelInProgress, "Still processing after 1m30s; the recovery job will complete it", true},
	} {
		t.Run(c.name, func(t *testing.T) {
			got := console.LabelOf(c.tx, c.recovered, now)
			if got.Label != c.label || got.What != c.what || got.Attention != c.attention {
				t.Fatalf("LabelOf = %+v, want %s %q attention=%v", got, c.label, c.what, c.attention)
			}
		})
	}
}
