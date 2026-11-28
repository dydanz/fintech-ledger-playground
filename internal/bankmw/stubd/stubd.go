// Package stubd is bank-middleware-d's adapter for Bank-Stub-D, the
// multi-rail cash-management bank (bank-api-stubs S:D): kebab-case meta/data
// envelopes signed with HMAC-SHA512 over the minified body (T§12.1 #3),
// account validation, funding and rail check, then a single remittance that
// settles asynchronously. The BMW answers ACCEPTED and polls remit-status,
// publishing PROCESSING and the terminal status to money-out.bank-events.v1.
// The mapping is T§11.4; see README.md.
package stubd

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

type Config struct {
	BaseURL        string
	ClientKey      string // X-Client-Key
	Secret         string // the HMAC-SHA512 key
	FundingAccount string // funding-acct-no (T§12.1 #9)
	PollInterval   time.Duration
	PollFor        time.Duration // after this, the poller gives up and MOM's inquiry resolves the attempt
	Now            func() time.Time
}

// Rails are S:D's explicit rail-mode values.
var Rails = []string{contract.RailBIFAST, contract.RailSKNBI, contract.RailRTGS}

// reportTries is how often the poller tries to publish a terminal status.
const reportTries = 3

var wib = time.FixedZone("WIB", 7*60*60)

var (
	failed  = bankmw.Failed
	unknown = bankmw.Unknown
)

// Reporter publishes an asynchronous status: bankmw.Emitter.
type Reporter interface {
	Report(ctx context.Context, transferID, requestID string, res contract.BankResult) error
}

type Bank struct {
	cfg    Config
	client *http.Client
	rep    Reporter

	ctx    context.Context // the pollers'; canceled by Close
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	polling map[string]bool // request_id → a poller runs
	closed  bool
}

func New(cfg Config, client *http.Client, rep Reporter, log *slog.Logger) (*Bank, error) {
	switch {
	case cfg.BaseURL == "" || cfg.ClientKey == "" || cfg.Secret == "" || cfg.FundingAccount == "":
		return nil, errors.New("stubd: base URL, client key, secret and funding account are required")
	case rep == nil:
		return nil, errors.New("stubd: a reporter is required: bank D settles asynchronously (KAFKA_BROKERS)")
	case cfg.PollInterval <= 0 || cfg.PollFor <= 0:
		return nil, errors.New("stubd: poll interval and poll duration must be positive")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	ctx, cancel := context.WithCancel(logger.WithContext(context.Background(), log))
	return &Bank{cfg: cfg, client: client, rep: rep, ctx: ctx, cancel: cancel, polling: map[string]bool{}}, nil
}

// Close stops every poller and waits for them. Their attempts are resolved
// by MOM's status inquiry (T§3.6).
func (b *Bank) Close() {
	b.mu.Lock()
	b.closed = true
	b.mu.Unlock()
	b.cancel()
	b.wg.Wait()
}

// meta is S:D's response envelope header.
type meta struct {
	Code string `json:"status-code"`
	Text string `json:"status-text"`
}

func statusCode(body []byte) string {
	var r struct{ Meta meta }
	_ = json.Unmarshal(body, &r)
	return r.Meta.Code
}

// preRemit maps a failed call before remit-single: no remittance was sent,
// so funds can't have moved.
func preRemit(a bankmw.Response) contract.BankResult {
	if a.Status == http.StatusUnauthorized {
		return failed(contract.ReasonAuthError, true, false, a.Code)
	}
	return failed(contract.ReasonRailUnavailable, true, false, a.Code)
}

// Transfer: acct-validate → funding-check → remit-single (T§11.4).
func (b *Bank) Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult {
	if !slices.Contains(Rails, req.Rail) {
		return failed(contract.ReasonRailUnavailable, true, false, "")
	}
	money, err := req.Amount.Money()
	if err != nil || money.Currency != "IDR" {
		return failed(contract.ReasonOther, false, false, "")
	}

	a := b.call(ctx, "acct-validate", req, map[string]any{"dest-bank-id": req.Beneficiary.BankCode,
		"dest-acct-no": req.Beneficiary.AccountNo, "rail-mode": req.Rail})
	var acct struct {
		Data struct {
			Owner string `json:"registered-owner"`
		}
	}
	switch {
	case a.Status != http.StatusOK || json.Unmarshal(a.Body, &acct) != nil:
		return preRemit(a)
	case a.Code == "14": // (A) not found
		return failed(contract.ReasonInvalidAccount, false, true, a.Code)
	case a.Code != "00":
		return failed(contract.ReasonBankRejected, true, true, a.Code)
	}

	a = b.call(ctx, "funding-check", req, map[string]any{"funding-acct-no": b.cfg.FundingAccount,
		"requested-rail": req.Rail, "tx-amount": money.Amount})
	var fund struct {
		Data struct {
			Liquidity  *int64 `json:"available-liquidity"`
			WindowOpen *bool  `json:"rail-window-open"`
		}
	}
	switch {
	case a.Status != http.StatusOK || json.Unmarshal(a.Body, &fund) != nil || fund.Data.WindowOpen == nil || fund.Data.Liquidity == nil:
		return preRemit(a)
	case !*fund.Data.WindowOpen:
		return failed(contract.ReasonCutoff, true, true, a.Code)
	case *fund.Data.Liquidity < money.Amount:
		return failed(contract.ReasonInsufficientSourceFunds, true, true, a.Code)
	case a.Code != "00":
		return failed(contract.ReasonBankRejected, true, true, a.Code)
	}

	// pg-order-id is the request_id (A): unique per attempt, and all a later
	// remit-status needs, even after a restart.
	a = b.call(ctx, "remit-single", req, map[string]any{"pg-order-id": req.RequestID, "funding-acct-no": b.cfg.FundingAccount,
		"dest-bank-id": req.Beneficiary.BankCode, "dest-acct-no": req.Beneficiary.AccountNo, "dest-acct-name": acct.Data.Owner,
		"rail-mode": req.Rail, "tx-amount": money.Amount, "currency-iso": money.Currency, "payment-memo": "Payout"})
	var out remitAnswer
	switch {
	case a.NotReached:
		return failed(contract.ReasonRailUnavailable, true, false, "")
	case a.Status == http.StatusUnauthorized: // the signature was refused before anything was processed
		return failed(contract.ReasonAuthError, true, false, a.Code)
	case a.Status != http.StatusOK || json.Unmarshal(a.Body, &out) != nil || a.Code != "00":
		// A timeout, 5xx, a duplicate, or anything unmapped: only the
		// status inquiry can tell.
		return unknown(a.Code)
	}
	res := out.result()
	switch res.Status {
	case contract.StatusProcessing:
		res.Status = contract.StatusAccepted // the bank took it; the poller reports PROCESSING
		b.watch(req.TransferID, req.RequestID)
	case contract.StatusFailed:
		return unknown(out.Data.State) // a rejection in the submit answer isn't in the contract
	}
	return res
}

type remitAnswer struct {
	Data struct {
		RailRef string `json:"bank-rail-ref"`
		State   string `json:"tx-state"`
	}
}

// result maps a tx-state (T§11.4).
func (r remitAnswer) result() contract.BankResult {
	st, ref := r.Data.State, r.Data.RailRef
	switch st {
	case "IN_CLEARING":
		return contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true, BankReference: ref, BankRawCode: st}
	case "SETTLED":
		return contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: ref, BankRawCode: st}
	case "REJECTED", "RETURNED": // (A)
		r := failed(contract.ReasonBankRejected, true, true, st)
		r.BankReference = ref
		return r
	}
	return unknown(st)
}

// InquireStatus asks remit-status about the order sent as request_id.
func (b *Bank) InquireStatus(ctx context.Context, transferID, requestID string) contract.BankResult {
	a := b.call(ctx, "remit-status", contract.TransferRequest{TransferID: transferID, RequestID: requestID},
		map[string]any{"pg-order-id": requestID})
	if a.Status == http.StatusNotFound && a.Code == "14" {
		return failed(contract.ReasonOther, true, false, a.Code) // the bank has no such order
	}
	var out remitAnswer
	if a.Status != http.StatusOK || a.Code != "00" || json.Unmarshal(a.Body, &out) != nil {
		return unknown(a.Code)
	}
	return out.result()
}

// watch starts one poller per attempt; a repeated Transfer for the same
// request_id (the bank answered SUBMITTED again) doesn't start another.
func (b *Bank) watch(transferID, requestID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.polling[requestID] {
		return
	}
	b.polling[requestID] = true
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.poll(transferID, requestID)
		b.mu.Lock()
		delete(b.polling, requestID)
		b.mu.Unlock()
	}()
}

// poll asks remit-status every PollInterval. It reports PROCESSING once and
// the terminal status once, then stops; it gives up after PollFor. It logs
// one bank.poll when it stops.
//
// It reports only states the bank actually returned: after SUBMITTED, an
// "order not found" or an error is never reported as FAILED, because MOM
// would fail over and could pay twice. MOM's inquiry resolves those.
func (b *Bank) poll(transferID, requestID string) {
	ctx := b.ctx
	start := time.Now()
	deadline := start.Add(b.cfg.PollFor)
	tick := time.NewTicker(b.cfg.PollInterval)
	defer tick.Stop()
	polls, processing := 0, false
	end := func(outcome, reason string, status contract.Status) {
		attrs := []any{"event", "bank.poll", "outcome", outcome, "transfer_id", transferID, "request_id", requestID,
			"polls", polls, "duration_ms", time.Since(start).Milliseconds()}
		log := logger.FromContext(ctx)
		if outcome == "success" {
			log.InfoContext(ctx, "bank status polled to a terminal state", append(attrs, "status", string(status))...)
			return
		}
		log.WarnContext(ctx, "bank status polling stopped; MOM's inquiry resolves the attempt", append(attrs, "reason", reason)...)
	}
	for {
		select {
		case <-ctx.Done():
			end("failure", "shutdown", "")
			return
		case <-tick.C:
		}
		if time.Now().After(deadline) {
			end("failure", "deadline", "")
			return
		}
		polls++
		res := b.InquireStatus(ctx, transferID, requestID)
		switch {
		case res.Status == contract.StatusProcessing && !processing:
			processing = b.rep.Report(ctx, transferID, requestID, res) == nil
		case res.Status == contract.StatusSucceeded || (res.Status == contract.StatusFailed && res.ReachedBank):
			for try := 1; ; try++ {
				if b.rep.Report(ctx, transferID, requestID, res) == nil {
					end("success", "", res.Status)
					return
				}
				if try == reportTries {
					end("failure", "report_failed", "")
					return
				}
				select {
				case <-ctx.Done():
					end("failure", "shutdown", "")
					return
				case <-tick.C:
				}
			}
		}
	}
}

func (b *Bank) call(ctx context.Context, step string, req contract.TransferRequest, data map[string]any) bankmw.Response {
	raw, err := json.Marshal(map[string]any{
		"meta": map[string]string{"client-trace-id": step + ":" + req.RequestID, "sent-at": b.cfg.Now().In(wib).Format(time.RFC3339)},
		"data": data})
	if err != nil {
		return bankmw.Response{NotReached: true, Err: err}
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.BaseURL+"/api/v2/disbursement/"+step, bytes.NewReader(raw))
	if err != nil {
		return bankmw.Response{NotReached: true, Err: err}
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("X-Client-Key", b.cfg.ClientKey)
	hr.Header.Set("X-Signature-512", Signature(b.cfg.Secret, raw))
	return bankmw.Do(ctx, b.client, step, hr, req, statusCode)
}

// Signature is S:D's X-Signature-512 (T§12.1 #3): Base64(HMAC-SHA512(secret,
// minified body)). json.Marshal's output is already minified.
func Signature(secret string, minBody []byte) string {
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(minBody)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
