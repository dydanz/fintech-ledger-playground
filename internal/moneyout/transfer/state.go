// Package transfer is money-out-middleware's transfer state machine (P§3.4,
// spec/money-out TRD §6.1): the states, the transition table and Apply, the
// way internal/ledger guards the core's transaction states. It does no I/O.
package transfer

import (
	"errors"
	"fmt"
)

var ErrIllegalTransition = errors.New("transfer: illegal state transition")

// State is a transfer's state. PROCESSING and FAILED mean what they mean in
// the core.
type State string

const (
	Received          State = "RECEIVED"
	Routed            State = "ROUTED"
	Dispatching       State = "DISPATCHING"
	Accepted          State = "ACCEPTED"
	Processing        State = "PROCESSING"
	Unknown           State = "UNKNOWN"
	AttemptFailed     State = "ATTEMPT_FAILED"
	Succeeded         State = "SUCCEEDED"
	Failed            State = "FAILED"
	NeedsManualReview State = "NEEDS_MANUAL_REVIEW"
)

// States lists all ten.
var States = []State{Received, Routed, Dispatching, Accepted, Processing, Unknown, AttemptFailed, Succeeded, Failed, NeedsManualReview}

// transitions is P§3.4's diagram, plus three edges it implies but doesn't
// draw:
//   - DISPATCHING / ACCEPTED → SUCCEEDED and DISPATCHING → PROCESSING: a sync
//     bank answers with the final (or clearing) status in the response.
//   - ROUTED → FAILED: routing found no candidate at all.
//   - any open state → NEEDS_MANUAL_REVIEW: reconciliation flags it (P§3.7),
//     or a bank reports on an attempt that's no longer current.
//
// NEEDS_MANUAL_REVIEW → SUCCEEDED / FAILED is an ops resolution or a late
// bank answer (Q3, Q4). UNKNOWN never goes to DISPATCHING: an ambiguous
// attempt never fails over (P§3.2).
var transitions = map[State][]State{
	Received:          {Routed, NeedsManualReview},
	Routed:            {Dispatching, Failed, NeedsManualReview},
	Dispatching:       {Accepted, Processing, Succeeded, Unknown, AttemptFailed, NeedsManualReview},
	Accepted:          {Processing, Succeeded, Unknown, AttemptFailed, NeedsManualReview},
	Processing:        {Succeeded, Unknown, AttemptFailed, NeedsManualReview},
	Unknown:           {Processing, Succeeded, AttemptFailed, NeedsManualReview},
	AttemptFailed:     {Dispatching, Failed, NeedsManualReview},
	NeedsManualReview: {Succeeded, Failed},
}

// Apply returns nil when from → to is allowed, else ErrIllegalTransition.
func Apply(from, to State) error {
	for _, s := range transitions[from] {
		if s == to {
			return nil
		}
	}
	return fmt.Errorf("%w: %s → %s", ErrIllegalTransition, from, to)
}

func (s State) Valid() bool {
	for _, k := range States {
		if s == k {
			return true
		}
	}
	return false
}

// Terminal reports SUCCEEDED or FAILED: the states a result goes to the core
// from.
func (s State) Terminal() bool { return s == Succeeded || s == Failed }

// AttemptOpen reports the states in which an attempt is in flight: at most
// one per transfer (P§3.2).
func (s State) AttemptOpen() bool {
	return s == Dispatching || s == Accepted || s == Processing || s == Unknown
}
