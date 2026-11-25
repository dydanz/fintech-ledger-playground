package transfer_test

import (
	"errors"
	"testing"

	"fintech-ledger-playground/internal/moneyout/transfer"
)

// The arrows of P§3.4, plus the edges it implies (see state.go).
var legal = map[transfer.State][]transfer.State{
	transfer.Received:          {transfer.Routed, transfer.NeedsManualReview},
	transfer.Routed:            {transfer.Dispatching, transfer.Failed, transfer.NeedsManualReview},
	transfer.Dispatching:       {transfer.Accepted, transfer.Processing, transfer.Succeeded, transfer.Unknown, transfer.AttemptFailed, transfer.NeedsManualReview},
	transfer.Accepted:          {transfer.Processing, transfer.Succeeded, transfer.Unknown, transfer.AttemptFailed, transfer.NeedsManualReview},
	transfer.Processing:        {transfer.Succeeded, transfer.Unknown, transfer.AttemptFailed, transfer.NeedsManualReview},
	transfer.Unknown:           {transfer.Processing, transfer.Succeeded, transfer.AttemptFailed, transfer.NeedsManualReview},
	transfer.AttemptFailed:     {transfer.Dispatching, transfer.Failed, transfer.NeedsManualReview},
	transfer.NeedsManualReview: {transfer.Succeeded, transfer.Failed}, // Q3/Q4: an ops resolution or a late bank answer
}

func TestLegalTransitions(t *testing.T) {
	for from, tos := range legal {
		for _, to := range tos {
			if err := transfer.Apply(from, to); err != nil {
				t.Errorf("%s → %s: %v, want legal", from, to, err)
			}
		}
	}
}

// Every other (from, to) pair of the 10 × 10 table is refused.
func TestIllegalTransitionsRejected(t *testing.T) {
	if len(transfer.States) != 10 {
		t.Fatalf("%d states, want the brief's 10", len(transfer.States))
	}
	for _, from := range transfer.States {
		allowed := map[transfer.State]bool{}
		for _, to := range legal[from] {
			allowed[to] = true
		}
		for _, to := range transfer.States {
			if allowed[to] {
				continue
			}
			if err := transfer.Apply(from, to); !errors.Is(err, transfer.ErrIllegalTransition) {
				t.Errorf("%s → %s: %v, want ErrIllegalTransition", from, to, err)
			}
		}
	}
}

func TestTerminalStatesHaveNoExit(t *testing.T) {
	for _, s := range []transfer.State{transfer.Succeeded, transfer.Failed} {
		if !s.Terminal() {
			t.Errorf("%s not terminal", s)
		}
		for _, to := range transfer.States {
			if transfer.Apply(s, to) == nil {
				t.Errorf("%s → %s allowed", s, to)
			}
		}
	}
	// UNKNOWN never fails over by itself: it can't go to DISPATCHING.
	if transfer.Apply(transfer.Unknown, transfer.Dispatching) == nil {
		t.Error("UNKNOWN → DISPATCHING allowed: failover from an ambiguous attempt")
	}
}

func TestUnknownStateRejected(t *testing.T) {
	if err := transfer.Apply("SETTLED", transfer.Succeeded); !errors.Is(err, transfer.ErrIllegalTransition) {
		t.Fatalf("unknown state: %v", err)
	}
	if transfer.State("SETTLED").Valid() || !transfer.Received.Valid() {
		t.Fatal("Valid() wrong")
	}
}

// Attempt states are the open ones a dispatch can be in.
func TestOpenAttempt(t *testing.T) {
	for _, s := range []transfer.State{transfer.Dispatching, transfer.Accepted, transfer.Processing, transfer.Unknown} {
		if !s.AttemptOpen() {
			t.Errorf("%s: attempt not open", s)
		}
	}
	for _, s := range []transfer.State{transfer.Received, transfer.Routed, transfer.AttemptFailed, transfer.Succeeded, transfer.Failed, transfer.NeedsManualReview} {
		if s.AttemptOpen() {
			t.Errorf("%s: attempt open", s)
		}
	}
}
