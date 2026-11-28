package service

import (
	"testing"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/moneyout/bmwclient"
	"fintech-ledger-playground/internal/moneyout/breaker"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// Every dispatch outcome, and what it means for the breaker (P§3.3).
func TestBreakerKind(t *testing.T) {
	ok := func(r contract.BankResult) bmwclient.Outcome { return bmwclient.Outcome{Result: r, Reason: "ok"} }
	failed := func(r contract.Reason) bmwclient.Outcome { return ok(bankmw.Failed(r, true, true, "")) }
	for name, c := range map[string]struct {
		out  bmwclient.Outcome
		want breaker.Kind
	}{
		"succeeded":           {ok(contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true}), breaker.Success},
		"accepted":            {ok(contract.BankResult{Status: contract.StatusAccepted, ReachedBank: true}), breaker.Success},
		"processing":          {ok(contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true}), breaker.Success},
		"bank UNKNOWN":        {ok(bankmw.Unknown("")), breaker.Failure},
		"RAIL_UNAVAILABLE":    {failed(contract.ReasonRailUnavailable), breaker.Failure},
		"BANK_REJECTED":       {failed(contract.ReasonBankRejected), breaker.Failure},
		"AUTH_ERROR":          {failed(contract.ReasonAuthError), breaker.Failure},
		"INVALID_ACCOUNT":     {failed(contract.ReasonInvalidAccount), breaker.Success},
		"ACCOUNT_CLOSED":      {failed(contract.ReasonAccountClosed), breaker.Success},
		"LIMIT_EXCEEDED":      {failed(contract.ReasonLimitExceeded), breaker.Success},
		"INSUFFICIENT_FUNDS":  {failed(contract.ReasonInsufficientSourceFunds), breaker.Success},
		"CUTOFF":              {failed(contract.ReasonCutoff), breaker.Success},
		"connection refused":  {bmwclient.Outcome{Result: bankmw.Failed(contract.ReasonRailUnavailable, true, false, ""), Reason: "connection_refused"}, breaker.Failure},
		"no endpoint":         {bmwclient.Outcome{Result: bankmw.Failed(contract.ReasonRailUnavailable, true, false, ""), Reason: "no_endpoint"}, breaker.Failure},
		"middleware timeout":  {bmwclient.Outcome{Result: bankmw.Unknown(""), Reason: "timeout"}, breaker.Failure},
		"middleware 5xx":      {bmwclient.Outcome{Result: bankmw.Unknown(""), Reason: "http_5xx"}, breaker.Failure},
		"invalid signature":   {bmwclient.Outcome{Result: bankmw.Failed(contract.ReasonAuthError, true, false, ""), Reason: "invalid_signature"}, breaker.Failure},
		"MOM's invalid input": {bmwclient.Outcome{Result: bankmw.Failed(contract.ReasonOther, true, false, ""), Reason: "invalid_input"}, breaker.Ignore},
		"unreadable answer":   {bmwclient.Outcome{Result: bankmw.Unknown(""), Reason: "invalid_response"}, breaker.Ignore},
	} {
		if got := breakerKind(c.out); got != c.want {
			t.Errorf("%s: %v, want %v", name, got, c.want)
		}
	}
}
