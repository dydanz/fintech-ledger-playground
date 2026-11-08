package fraud

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Freezer freezes an account in ledger-api for a decision.
type Freezer interface {
	// Freeze continues traceID as span spanID (the engine's span for the event).
	Freeze(ctx context.Context, d Decision, traceID, spanID string) error
}

// ErrFreezeRejected is ledger-api refusing the call for a reason retrying
// can't fix (bad token, forbidden operation, unknown account, invalid body).
var ErrFreezeRejected = errors.New("fraud: freeze rejected")

// HTTPFreezer calls POST /internal/v1/accounts/{id}/freeze as client
// fraud-engine (TRD §7.4).
type HTTPFreezer struct {
	baseURL string
	token   string
	http    *http.Client
}

func NewHTTPFreezer(baseURL, token string, timeout time.Duration) *HTTPFreezer {
	return &HTTPFreezer{baseURL: strings.TrimRight(baseURL, "/"), token: token, http: &http.Client{Timeout: timeout}}
}

// IdempotencyKey is fraud:<rule>:<account_id>:<window_start_ms>, plus
// :<currency> for debit_volume (its windows are per currency), with the
// rule's "_" as "-" (Idempotency-Key allows [A-Za-z0-9._:-]). A replayed
// decision reuses the key, so the freeze is replayed, not repeated.
func IdempotencyKey(d Decision) string {
	key := "fraud:" + strings.ReplaceAll(d.Rule, "_", "-") + ":" + d.AccountID + ":" + strconv.FormatInt(d.WindowStart.UnixMilli(), 10)
	if d.Rule == RuleDebitVolume {
		key += ":" + d.Currency
	}
	return key
}

// Reason is the freeze reason stored in AccountFrozen (at most 64 characters).
// It is built only from what IdempotencyKey encodes, so every decision for
// one window sends the same request body. As more events land in a window,
// its count and end grow; were they in the body, each later call would reuse
// the key with a different request and be refused as idempotency_key_reused.
// The count and sum are in the fraud.decision log line instead.
func Reason(d Decision) string {
	from := d.WindowStart.UTC().Format("2006-01-02T15:04:05.000Z")
	if d.Rule == RuleDebitVolume {
		return "debit_volume " + d.Currency + ": window from " + from
	}
	return "topup_velocity: window from " + from
}

func (f *HTTPFreezer) Freeze(ctx context.Context, d Decision, traceID, spanID string) error {
	body, _ := json.Marshal(map[string]string{"reason": Reason(d), "rule": d.Rule})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.baseURL+"/internal/v1/accounts/"+d.AccountID+"/freeze", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+f.token)
	req.Header.Set("Idempotency-Key", IdempotencyKey(d))
	if tp := Traceparent(traceID, spanID); tp != "" {
		req.Header.Set("traceparent", tp) // same trace as the triggering event, new span
	}
	resp, err := f.http.Do(req)
	if err != nil {
		return fmt.Errorf("freeze call: %w", err)
	}
	defer resp.Body.Close()
	var e struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = json.Unmarshal(raw, &e)
	switch {
	case resp.StatusCode == http.StatusOK:
		return nil
	case resp.StatusCode == http.StatusConflict || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		return fmt.Errorf("freeze call: %d %s", resp.StatusCode, e.Error.Code) // retriable
	default:
		return fmt.Errorf("%w: %d %s", ErrFreezeRejected, resp.StatusCode, e.Error.Code)
	}
}

// Traceparent is the W3C header for traceID and spanID ("" if either is
// malformed).
func Traceparent(traceID, spanID string) string {
	if _, err := hex.DecodeString(traceID); err != nil || len(traceID) != 32 {
		return ""
	}
	if _, err := hex.DecodeString(spanID); err != nil || len(spanID) != 16 {
		return ""
	}
	return "00-" + traceID + "-" + spanID + "-01"
}
