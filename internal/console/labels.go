package console

import (
	"fmt"
	"strings"
	"time"
)

// Label is how a transaction was handled, in the words the Monitor uses
// (spec/2026-11-10 Web Console Design D§6.2).
type Label string

const (
	LabelNormal     Label = "normal"
	LabelRecovered  Label = "recovered"
	LabelRejected   Label = "rejected"
	LabelReversed   Label = "reversed"
	LabelTimedOut   Label = "timed_out"
	LabelInProgress Label = "in_progress"
)

// Tx is what the labels need from an ops transaction (ledger-api's view).
type Tx struct {
	ID            string     `json:"id"`
	Type          string     `json:"type"`
	Status        string     `json:"status"`
	FailureReason string     `json:"failure_reason,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	SettledAt     *time.Time `json:"settled_at,omitempty"`
}

// Labelled is a transaction's label, the one-line "what happened", and
// whether a person should look at it.
type Labelled struct {
	Label     Label  `json:"label"`
	What      string `json:"what"`
	Attention bool   `json:"attention"`
}

// stuckAfter is when an in-progress transaction is flagged (NFR-7: nothing
// PENDING longer than 60 s while Postgres is healthy).
const stuckAfter = 60 * time.Second

var refusalText = map[string]string{
	"insufficient_funds": "Insufficient funds; balance unchanged",
	"account_frozen":     "Account frozen by the fraud engine or ops; spending blocked",
	"not_voidable":       "Couldn't be reversed: already reversed, or never settled",
	"ledger_not_found":   "The account has no ledger in this currency",
}

// LabelOf labels one transaction. recovered says the sweeper completed it
// (a ledger.recover event names it). Order matters: a timeout (the sweeper
// failing a transaction) needs attention more than a recovery, and a
// recovery is the story even when the outcome was a refusal.
func LabelOf(t Tx, recovered bool, now time.Time) Labelled {
	switch {
	case t.Status == "FAILED" && t.FailureReason == "processing_timeout":
		return Labelled{LabelTimedOut, "Couldn't complete in time; failed safely, nothing moved", true}
	case recovered && t.Status == "FAILED":
		return Labelled{LabelRecovered, "Completed by the recovery job, which refused it: " + lowerFirst(refusal(t.FailureReason)), false}
	case recovered:
		return Labelled{LabelRecovered, "Completed by the recovery job after the request couldn't finish on its own", false}
	case t.Status == "FAILED":
		return Labelled{LabelRejected, refusal(t.FailureReason), false}
	case t.Status == "VOIDED":
		return Labelled{LabelReversed, "Reversed by a later void; the original stays on record", false}
	case t.Type == "VOID" && t.Status == "SETTLED":
		return Labelled{LabelReversed, "A reversal: a reversing entry was added, nothing was deleted", false}
	case t.Status == "PENDING" || t.Status == "PROCESSING":
		if age := now.Sub(t.CreatedAt); age > stuckAfter {
			return Labelled{LabelInProgress, fmt.Sprintf("Still processing after %s; the recovery job will complete it", age.Round(time.Second)), true}
		}
		return Labelled{LabelInProgress, "Processing", false}
	}
	took := "a moment"
	if t.SettledAt != nil {
		took = shortDuration(t.SettledAt.Sub(t.CreatedAt))
	}
	return Labelled{LabelNormal, "Settled in " + took, false}
}

func refusal(reason string) string {
	if s, ok := refusalText[reason]; ok {
		return s
	}
	return "Refused: " + strings.ReplaceAll(reason, "_", " ")
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + strings.ReplaceAll(s[1:], ";", ",")
}

// shortDuration renders 3 ms, 1.2 s or 47 s.
func shortDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%d ms", d.Milliseconds())
	case d < 10*time.Second:
		return fmt.Sprintf("%.1f s", d.Seconds())
	default:
		return fmt.Sprintf("%d s", int(d.Seconds()))
	}
}
