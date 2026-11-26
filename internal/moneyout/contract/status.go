package contract

import (
	"errors"
	"fmt"
)

// Status is the canonical outcome a bank middleware reports (P§2.4).
type Status string

const (
	StatusAccepted   Status = "ACCEPTED"   // the bank took it; the result follows
	StatusProcessing Status = "PROCESSING" // clearing or settling
	StatusSucceeded  Status = "SUCCEEDED"  // the bank confirmed funds moved
	StatusFailed     Status = "FAILED"     // definitively rejected; funds did not move
	StatusUnknown    Status = "UNKNOWN"    // ambiguous: resolve by status inquiry, never fail over
)

func (s Status) Valid() bool {
	switch s {
	case StatusAccepted, StatusProcessing, StatusSucceeded, StatusFailed, StatusUnknown:
		return true
	}
	return false
}

// Terminal reports a definitive bank answer.
func (s Status) Terminal() bool { return s == StatusSucceeded || s == StatusFailed }

// Reason says why a transfer FAILED (P§2.4).
type Reason string

const (
	ReasonInvalidAccount          Reason = "INVALID_ACCOUNT"
	ReasonAccountClosed           Reason = "ACCOUNT_CLOSED"
	ReasonNameMismatch            Reason = "NAME_MISMATCH"
	ReasonLimitExceeded           Reason = "LIMIT_EXCEEDED"
	ReasonInsufficientSourceFunds Reason = "INSUFFICIENT_SOURCE_FUNDS"
	ReasonRailUnavailable         Reason = "RAIL_UNAVAILABLE"
	ReasonCutoff                  Reason = "CUTOFF"
	ReasonBankRejected            Reason = "BANK_REJECTED"
	ReasonAuthError               Reason = "AUTH_ERROR"
	ReasonOther                   Reason = "OTHER"
)

// Reasons is the full set, in the brief's order.
var Reasons = []Reason{ReasonInvalidAccount, ReasonAccountClosed, ReasonNameMismatch, ReasonLimitExceeded,
	ReasonInsufficientSourceFunds, ReasonRailUnavailable, ReasonCutoff, ReasonBankRejected, ReasonAuthError, ReasonOther}

func (r Reason) Valid() bool {
	for _, k := range Reasons {
		if r == k {
			return true
		}
	}
	return false
}

// DefaultRetryElsewhere is the default retry_elsewhere hint (T§3.6): a
// beneficiary problem won't be fixed by another bank; a problem on our side or
// this bank's may be. Each bank's mapping can override it per row.
func DefaultRetryElsewhere(r Reason) bool {
	switch r {
	case ReasonLimitExceeded, ReasonInsufficientSourceFunds, ReasonRailUnavailable, ReasonCutoff, ReasonAuthError:
		return true
	}
	return false
}

// BankResult is a bank middleware's answer, synchronous or inquired: the same
// model either way. BankReference and BankRawCode are opaque to MOM.
type BankResult struct {
	Status         Status `json:"status"`
	Reason         Reason `json:"reason,omitempty"`
	RetryElsewhere bool   `json:"retry_elsewhere"`
	// ReachedBank is false only when the request provably never reached the
	// bank (connection refused before the call, a refusal before the bank
	// call): the one case where MOM may retry the same node.
	ReachedBank   bool   `json:"reached_bank"`
	BankReference string `json:"bank_reference,omitempty"`
	BankRawCode   string `json:"bank_raw_code,omitempty"`
}

var ErrInvalidResult = errors.New("contract: invalid bank result")

// Validate checks the model's own rules: a canonical status, and a valid
// reason exactly when FAILED.
func (r BankResult) Validate() error {
	switch {
	case !r.Status.Valid():
		return fmt.Errorf("%w: status %q", ErrInvalidResult, r.Status)
	case r.Status == StatusFailed && !r.Reason.Valid():
		return fmt.Errorf("%w: FAILED needs a reason, got %q", ErrInvalidResult, r.Reason)
	case r.Status != StatusFailed && r.Reason != "":
		return fmt.Errorf("%w: reason %q on %s", ErrInvalidResult, r.Reason, r.Status)
	}
	return nil
}
