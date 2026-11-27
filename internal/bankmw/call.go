package bankmw

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// Response is one bank API answer, or why there was none.
type Response struct {
	Status     int    // HTTP status; 0 when there was no response
	Body       []byte // up to 1 MiB
	Code       string // the bank's own code, as the caller's codeOf read it
	NotReached bool   // provably never sent: the connection was refused at dial
	Err        error  // transport error (a timeout means the bank may have acted)
}

// Do sends one request to the bank and logs one bank.call (T§9): the step,
// HTTP status, the bank's code and duration, with the account masked. It
// never logs headers or bodies, so no token, secret or signature.
func Do(ctx context.Context, client *http.Client, step string, hr *http.Request, req contract.TransferRequest, codeOf func([]byte) string) Response {
	start := time.Now()
	resp, err := client.Do(hr)
	r := Response{Err: err}
	if err == nil {
		r.Status = resp.StatusCode
		r.Body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		r.Code = codeOf(r.Body)
	} else {
		var op *net.OpError
		r.NotReached = errors.As(err, &op) && op.Op == "dial"
	}
	attrs := []any{"event", "bank.call", "step", step, "http_status", r.Status, "bank_raw_code", r.Code,
		"duration_ms", time.Since(start).Milliseconds()}
	if req.TransferID != "" {
		attrs = append(attrs, "transfer_id", req.TransferID, "request_id", req.RequestID)
	}
	if req.Beneficiary.AccountNo != "" {
		attrs = append(attrs, "account", Mask(req.Beneficiary.AccountNo))
	}
	log := logger.FromContext(ctx)
	switch {
	case err != nil:
		reason := "timeout"
		if r.NotReached {
			reason = "connection_refused"
		}
		log.WarnContext(ctx, "bank call failed", append(attrs, "outcome", "failure", "reason", reason)...)
	case r.Status/100 != 2:
		log.WarnContext(ctx, "bank call refused", append(attrs, "outcome", "failure", "reason", "http_"+strconv.Itoa(r.Status))...)
	default:
		log.InfoContext(ctx, "bank call", append(attrs, "outcome", "success")...)
	}
	return r
}

// Failed and Unknown build canonical results.
func Failed(reason contract.Reason, retry, reached bool, raw string) contract.BankResult {
	return contract.BankResult{Status: contract.StatusFailed, Reason: reason, RetryElsewhere: retry, ReachedBank: reached, BankRawCode: raw}
}

func Unknown(raw string) contract.BankResult {
	return contract.BankResult{Status: contract.StatusUnknown, ReachedBank: true, BankRawCode: raw}
}
