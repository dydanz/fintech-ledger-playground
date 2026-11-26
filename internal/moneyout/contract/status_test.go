package contract_test

import (
	"testing"

	"fintech-ledger-playground/internal/moneyout/contract"
)

// The default per reason (T§3.6): beneficiary problems never go elsewhere; our
// side or this bank's problems may.
func TestDefaultRetryElsewhere(t *testing.T) {
	for reason, want := range map[contract.Reason]bool{
		contract.ReasonInvalidAccount:          false,
		contract.ReasonAccountClosed:           false,
		contract.ReasonNameMismatch:            false,
		contract.ReasonLimitExceeded:           true,
		contract.ReasonInsufficientSourceFunds: true,
		contract.ReasonRailUnavailable:         true,
		contract.ReasonCutoff:                  true,
		contract.ReasonAuthError:               true,
		contract.ReasonBankRejected:            false,
		contract.ReasonOther:                   false,
	} {
		if got := contract.DefaultRetryElsewhere(reason); got != want {
			t.Errorf("DefaultRetryElsewhere(%s) = %v, want %v", reason, got, want)
		}
	}
}

func TestReasonsAreTheBriefsSet(t *testing.T) {
	if n := len(contract.Reasons); n != 10 {
		t.Fatalf("%d reasons, want the brief's 10 (P§2.4)", n)
	}
	for _, r := range contract.Reasons {
		if !r.Valid() {
			t.Errorf("%s not valid", r)
		}
	}
	if contract.Reason("TIMEOUT").Valid() {
		t.Error("an unknown reason is valid")
	}
}

func TestStatusValidity(t *testing.T) {
	for _, s := range []contract.Status{contract.StatusAccepted, contract.StatusProcessing, contract.StatusSucceeded, contract.StatusFailed, contract.StatusUnknown} {
		if !s.Valid() {
			t.Errorf("%s not valid", s)
		}
	}
	if contract.Status("SETTLED").Valid() {
		t.Error("a bank's own status passed as canonical")
	}
	if !contract.StatusSucceeded.Terminal() || !contract.StatusFailed.Terminal() || contract.StatusUnknown.Terminal() {
		t.Error("Terminal() wrong")
	}
}

// A FAILED result must say why; other statuses carry no reason.
func TestBankResultValidate(t *testing.T) {
	ok := []contract.BankResult{
		{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: "9981234"},
		{Status: contract.StatusFailed, Reason: contract.ReasonInvalidAccount, ReachedBank: true},
		{Status: contract.StatusUnknown, ReachedBank: true},
	}
	for _, r := range ok {
		if err := r.Validate(); err != nil {
			t.Errorf("%+v: %v", r, err)
		}
	}
	bad := []contract.BankResult{
		{Status: contract.StatusFailed},
		{Status: contract.StatusSucceeded, Reason: contract.ReasonOther},
		{Status: "SETTLED"},
		{Status: contract.StatusFailed, Reason: "NOPE"},
	}
	for _, r := range bad {
		if err := r.Validate(); err == nil {
			t.Errorf("%+v passed", r)
		}
	}
}
