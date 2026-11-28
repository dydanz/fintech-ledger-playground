// Package bmwclient is MOM's side of the MOM → bank middleware call (spec/
// money-out TRD §3.5, §6.3): it signs each request, sends it to the bank
// middleware the routing picked, and turns whatever comes back, or doesn't,
// into a canonical result. It has no bank concept: endpoints are config.
package bmwclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/signing"
)

// Outcome is one call's result and how MOM got it.
type Outcome struct {
	Result contract.BankResult
	// NoSameNode: the middleware refused the request itself (a signature or
	// input bug, a missing endpoint). It never reached the bank, but sending
	// it again to the same node would fail the same way.
	NoSameNode bool
	HTTPStatus int
	// Reason is fixed: ok, connection_refused, no_endpoint, timeout,
	// http_5xx, invalid_signature, invalid_input, invalid_response.
	Reason   string
	Duration time.Duration
}

type Client struct {
	signer *signing.Signer
	urls   map[string]string // bank_middleware → base URL
	http   *http.Client
	now    func() time.Time
}

func New(signer *signing.Signer, urls map[string]string, httpc *http.Client) *Client {
	return &Client{signer: signer, urls: urls, http: httpc, now: time.Now}
}

// Transfer sends one attempt. The body carries the request's own nonce and
// issued_at, the ones it's signed with (T§3.5).
func (c *Client) Transfer(ctx context.Context, bmw string, req contract.TransferRequest) Outcome {
	nonce, err := signing.NewNonce()
	if err != nil {
		return notSent("no_endpoint", true)
	}
	req.IssuedAt, req.Nonce = c.now().UTC().Truncate(time.Second), nonce
	body, err := json.Marshal(req)
	if err != nil {
		return notSent("invalid_input", true)
	}
	return c.post(ctx, bmw, bankmw.PathTransfers, body, func(h *http.Header) error {
		var err error
		*h, err = c.signer.SignNonce(http.MethodPost, bankmw.PathTransfers, body, req.IssuedAt, nonce)
		return err
	})
}

// Inquire asks for an attempt's status.
func (c *Client) Inquire(ctx context.Context, bmw, transferID, requestID string) Outcome {
	body, err := json.Marshal(contract.StatusInquiry{TransferID: transferID, RequestID: requestID})
	if err != nil {
		return notSent("invalid_input", true)
	}
	return c.post(ctx, bmw, bankmw.PathStatus, body, func(h *http.Header) error {
		var err error
		*h, err = c.signer.Sign(http.MethodPost, bankmw.PathStatus, body, c.now())
		return err
	})
}

func notSent(reason string, noSameNode bool) Outcome {
	r := contract.ReasonRailUnavailable
	if reason == "invalid_input" {
		r = contract.ReasonOther
	}
	return Outcome{Result: bankmw.Failed(r, true, false, ""), NoSameNode: noSameNode, Reason: reason}
}

func (c *Client) post(ctx context.Context, bmw, path string, body []byte, sign func(*http.Header) error) Outcome {
	base, ok := c.urls[bmw]
	if !ok {
		return notSent("no_endpoint", true)
	}
	var hdr http.Header
	if err := sign(&hdr); err != nil {
		return notSent("no_endpoint", true)
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(body))
	if err != nil {
		return notSent("no_endpoint", true)
	}
	hr.Header = hdr
	hr.Header.Set("Content-Type", "application/json")
	start := time.Now()
	resp, err := c.http.Do(hr)
	d := time.Since(start)
	if err != nil {
		var op *net.OpError
		if errors.As(err, &op) && op.Op == "dial" {
			o := notSent("connection_refused", false) // provably not sent: retry on this node
			o.Duration = d
			return o
		}
		return Outcome{Result: bankmw.Unknown(""), Reason: "timeout", Duration: d} // it may have reached the bank
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	o := Outcome{HTTPStatus: resp.StatusCode, Duration: d}
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		var res contract.BankResult
		if json.Unmarshal(raw, &res) != nil || res.Validate() != nil {
			o.Result, o.Reason = bankmw.Unknown(""), "invalid_response"
			return o
		}
		if res.Status == contract.StatusUnknown {
			res.ReachedBank = true // UNKNOWN may have reached the bank, whatever the flag says
		}
		o.Result, o.Reason = res, "ok"
	case resp.StatusCode == http.StatusUnauthorized: // refused before the bank: a key or clock bug (T§6.3)
		o.Result, o.NoSameNode, o.Reason = bankmw.Failed(contract.ReasonAuthError, true, false, ""), true, "invalid_signature"
	case resp.StatusCode/100 == 4 && json.Unmarshal(raw, &envelope) == nil && envelope.Error.Code == "invalid_input":
		o.Result, o.NoSameNode, o.Reason = bankmw.Failed(contract.ReasonOther, true, false, ""), true, "invalid_input"
	case resp.StatusCode/100 == 5:
		o.Result, o.Reason = bankmw.Unknown(""), "http_5xx"
	default:
		o.Result, o.Reason = bankmw.Unknown(""), "invalid_response"
	}
	return o
}
