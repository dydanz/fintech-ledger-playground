package contract

import (
	"errors"
	"fmt"
	"slices"
)

// ErrInvalidRequest marks a MOM → bank middleware request that isn't valid.
var ErrInvalidRequest = errors.New("invalid bank middleware request")

var rails = []string{RailBIFAST, RailRTOL, RailSKNBI, RailRTGS, RailIntrabank}

// ValidRail reports whether r is one of the P§2.3 rails.
func ValidRail(r string) bool { return slices.Contains(rails, r) }

// Validate checks a request before a bank middleware acts on it.
func (r TransferRequest) Validate() error {
	switch {
	case !uuidPattern.MatchString(r.RequestID):
		return fmt.Errorf("%w: request_id must be a lowercase UUID", ErrInvalidRequest)
	case !uuidPattern.MatchString(r.TransferID):
		return fmt.Errorf("%w: transfer_id must be a lowercase UUID", ErrInvalidRequest)
	case r.AttemptNo < 1:
		return fmt.Errorf("%w: attempt_no must be 1 or more", ErrInvalidRequest)
	case !ValidRail(r.Rail):
		return fmt.Errorf("%w: unknown rail %q", ErrInvalidRequest, r.Rail)
	case !bankCodePattern.MatchString(r.Beneficiary.BankCode):
		return fmt.Errorf("%w: bank_code must be 3 digits", ErrInvalidRequest)
	case !accountNoPattern.MatchString(r.Beneficiary.AccountNo):
		return fmt.Errorf("%w: account_no must be 5-34 digits", ErrInvalidRequest)
	case r.IssuedAt.IsZero() || r.Nonce == "":
		return fmt.Errorf("%w: issued_at and nonce are required", ErrInvalidRequest)
	}
	if _, err := r.Amount.Money(); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidRequest, err)
	}
	return nil
}

// Validate checks a status inquiry.
func (q StatusInquiry) Validate() error {
	if !uuidPattern.MatchString(q.TransferID) || !uuidPattern.MatchString(q.RequestID) {
		return fmt.Errorf("%w: transfer_id and request_id must be lowercase UUIDs", ErrInvalidRequest)
	}
	return nil
}
