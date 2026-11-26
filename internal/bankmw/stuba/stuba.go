// Package stuba is bank-middleware-a's adapter for Bank-Stub-A, the BI-SNAP
// bank (bank-api-stubs S:A): an RSA-signed B2B token cached until
// expiresIn, then HMAC-SHA512-signed account inquiry, interbank transfer
// and status inquiry. The mapping to the canonical status is T§11.1; see
// README.md.
package stuba

import (
	"bytes"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// Config is the aggregator's credentials at S:A and its source account.
type Config struct {
	BaseURL       string
	ClientKey     string          // X-CLIENT-KEY
	PrivateKey    *rsa.PrivateKey // signs the token request (SHA256withRSA)
	ClientSecret  string          // HMAC-SHA512 key for transactional calls
	PartnerID     string          // X-PARTNER-ID
	ChannelID     string          // CHANNEL-ID
	SourceAccount string          // sourceAccountNo (T§12.1 #9)
	DeviceID      string          // additionalInfo.deviceId
	Now           func() time.Time
}

// Rails S:A offers (T§11.4, (A)).
var Rails = []string{contract.RailBIFAST, contract.RailRTOL}

// The token is refreshed this long before expiresIn.
const tokenMargin = 30 * time.Second

var wib = time.FixedZone("WIB", 7*60*60)

type Bank struct {
	cfg    Config
	client *http.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

func New(cfg Config, client *http.Client) (*Bank, error) {
	if cfg.BaseURL == "" || cfg.ClientKey == "" || cfg.PrivateKey == nil || cfg.ClientSecret == "" || cfg.PartnerID == "" ||
		cfg.ChannelID == "" || cfg.SourceAccount == "" {
		return nil, errors.New("stuba: base URL, client key, private key, client secret, partner id, channel id and source account are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Bank{cfg: cfg, client: client}, nil
}

// answer is one bank response, or why there was none.
type answer struct {
	status     int    // HTTP status; 0 when there was no response
	code       string // responseCode
	body       []byte
	notReached bool  // the request provably never left: dial failure, or the token failed
	tokenErr   bool  // the token request failed
	err        error // transport error
}

func failed(reason contract.Reason, retry, reached bool, raw string) contract.BankResult {
	return contract.BankResult{Status: contract.StatusFailed, Reason: reason, RetryElsewhere: retry, ReachedBank: reached, BankRawCode: raw}
}

// preBank maps a call that never reached the bank's transfer logic: no
// token, or a 401 that the retry didn't cure. ok is false otherwise.
func preBank(a answer) (contract.BankResult, bool) {
	switch {
	case a.tokenErr && a.status == 0: // the token endpoint didn't answer: the bank is unreachable
		return failed(contract.ReasonRailUnavailable, true, false, ""), true
	case a.tokenErr || a.status == http.StatusUnauthorized:
		return failed(contract.ReasonAuthError, true, false, a.code), true
	}
	return contract.BankResult{}, false
}

func unknown(raw string) contract.BankResult {
	return contract.BankResult{Status: contract.StatusUnknown, ReachedBank: true, BankRawCode: raw}
}

// Transfer: account inquiry, then the interbank transfer (T§11.1).
func (b *Bank) Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult {
	if !slices.Contains(Rails, req.Rail) {
		return failed(contract.ReasonRailUnavailable, true, false, "") // a routing error: nothing was sent
	}
	money, err := req.Amount.Money()
	if err != nil {
		return failed(contract.ReasonOther, false, false, "")
	}
	info := map[string]string{"deviceId": b.cfg.DeviceID, "channel": req.Rail}
	a := b.call(ctx, "inquiry", "/interbank/snap/v1.0/account-inquiry-external", req, map[string]any{
		"beneficiaryAccountCode": req.Beneficiary.BankCode, "beneficiaryAccountNo": req.Beneficiary.AccountNo, "additionalInfo": info})
	var inq struct {
		Name string `json:"beneficiaryAccountName"`
	}
	if r, ok := preBank(a); ok {
		return r
	}
	switch {
	case a.code == "4041611":
		return failed(contract.ReasonInvalidAccount, false, true, a.code)
	case a.code != "2001600" || json.Unmarshal(a.body, &inq) != nil:
		// No transfer was sent, so no money moved, whatever the inquiry's fate.
		return failed(contract.ReasonRailUnavailable, true, false, a.code)
	}

	a = b.call(ctx, "transfer", "/interbank/snap/v1.0/transfer-interbank", req, map[string]any{
		"partnerReferenceNo": req.RequestID, "amount": contract.AmountOf(money), "beneficiaryAccountName": inq.Name,
		"beneficiaryAccountNo": req.Beneficiary.AccountNo, "beneficiaryBankCode": req.Beneficiary.BankCode,
		"sourceAccountNo": b.cfg.SourceAccount, "transactionDate": b.cfg.Now().In(wib).Format(time.RFC3339), "additionalInfo": info})
	if r, ok := preBank(a); ok {
		return r
	}
	switch {
	case a.notReached:
		return failed(contract.ReasonRailUnavailable, true, false, "")
	case a.code == "2001800":
		var out struct {
			Ref string `json:"referenceNo"`
		}
		_ = json.Unmarshal(a.body, &out)
		return contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: out.Ref, BankRawCode: a.code}
	case a.code == "4031802":
		return failed(contract.ReasonLimitExceeded, true, true, a.code)
	case a.code == "4031814":
		return failed(contract.ReasonInsufficientSourceFunds, true, true, a.code)
	default:
		// 2021800, 4031816, 5xx, a timeout, or anything unmapped: only the
		// status inquiry can tell (P§2.4).
		return unknown(a.code)
	}
}

// InquireStatus asks S:A about the transfer sent as partnerReferenceNo =
// request_id (T§11.1 status rows).
func (b *Bank) InquireStatus(ctx context.Context, transferID, requestID string) contract.BankResult {
	a := b.call(ctx, "status", "/v1.0/transfer/status", contract.TransferRequest{TransferID: transferID, RequestID: requestID},
		map[string]any{"originalPartnerReferenceNo": requestID, "serviceCode": "18",
			"transactionDate": b.cfg.Now().In(wib).Format(time.RFC3339)})
	var out struct {
		Status string `json:"latestTransactionStatus"`
		Ref    string `json:"originalReferenceNo"`
	}
	if a.code != "2003600" || json.Unmarshal(a.body, &out) != nil {
		return unknown(a.code)
	}
	switch out.Status {
	case "00":
		return contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: out.Ref, BankRawCode: out.Status}
	case "01", "02", "03":
		return contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true, BankReference: out.Ref, BankRawCode: out.Status}
	case "06":
		r := failed(contract.ReasonBankRejected, true, true, out.Status)
		r.BankReference = out.Ref
		return r
	case "07":
		return failed(contract.ReasonOther, true, false, out.Status) // the bank has no such transfer
	default:
		return unknown(out.Status)
	}
}

// call sends one signed SNAP request. A 401 refreshes the token and retries
// once: the bank refused it before processing.
func (b *Bank) call(ctx context.Context, step, path string, req contract.TransferRequest, body any) answer {
	raw, err := json.Marshal(body) // already minified
	if err != nil {
		return answer{notReached: true, err: err}
	}
	a := b.send(ctx, step, path, req, raw, false)
	if a.status == http.StatusUnauthorized && !a.tokenErr {
		a = b.send(ctx, step, path, req, raw, true)
	}
	return a
}

func (b *Bank) send(ctx context.Context, step, path string, req contract.TransferRequest, raw []byte, refresh bool) answer {
	tok, tokAnswer := b.accessToken(ctx, refresh)
	if tok == "" {
		return tokAnswer
	}
	ts := b.cfg.Now().In(wib).Format(time.RFC3339)
	ext := make([]byte, 8)
	_, _ = rand.Read(ext)
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.BaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return answer{notReached: true, err: err}
	}
	hr.Header.Set("Authorization", "Bearer "+tok)
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("X-TIMESTAMP", ts)
	hr.Header.Set("X-SIGNATURE", Signature(b.cfg.ClientSecret, http.MethodPost, path, tok, raw, ts))
	hr.Header.Set("X-PARTNER-ID", b.cfg.PartnerID)
	hr.Header.Set("CHANNEL-ID", b.cfg.ChannelID)
	hr.Header.Set("X-EXTERNAL-ID", b.cfg.Now().In(wib).Format("20060102150405")+strconv.FormatUint(uint64(ext[0])<<16|uint64(ext[1])<<8|uint64(ext[2]), 10))
	return b.do(ctx, step, hr, req)
}

// do sends hr and logs one bank.call.
func (b *Bank) do(ctx context.Context, step string, hr *http.Request, req contract.TransferRequest) answer {
	start := time.Now()
	resp, err := b.client.Do(hr)
	a := answer{err: err}
	if err == nil {
		a.status = resp.StatusCode
		a.body, _ = io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		var rc struct {
			Code string `json:"responseCode"`
		}
		_ = json.Unmarshal(a.body, &rc)
		a.code = rc.Code
	} else {
		var op *net.OpError
		a.notReached = errors.As(err, &op) && op.Op == "dial"
	}
	attrs := []any{"event", "bank.call", "step", step, "http_status", a.status, "bank_raw_code", a.code,
		"duration_ms", time.Since(start).Milliseconds()}
	if req.TransferID != "" {
		attrs = append(attrs, "transfer_id", req.TransferID, "request_id", req.RequestID)
	}
	if req.Beneficiary.AccountNo != "" {
		attrs = append(attrs, "account", bankmw.Mask(req.Beneficiary.AccountNo))
	}
	log := logger.FromContext(ctx)
	switch {
	case err != nil:
		reason := "timeout"
		if a.notReached {
			reason = "connection_refused"
		}
		log.WarnContext(ctx, "bank call failed", append(attrs, "outcome", "failure", "reason", reason)...)
	case a.status/100 != 2:
		log.WarnContext(ctx, "bank call refused", append(attrs, "outcome", "failure", "reason", "http_"+strconv.Itoa(a.status))...)
	default:
		log.InfoContext(ctx, "bank call", append(attrs, "outcome", "success")...)
	}
	return a
}

// accessToken returns the cached token, or fetches one (S:A step 1). On
// failure it returns "" and the answer to report.
func (b *Bank) accessToken(ctx context.Context, refresh bool) (string, answer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	if !refresh && b.token != "" && now.Before(b.tokenExp.Add(-tokenMargin)) {
		return b.token, answer{}
	}
	ts := now.In(wib).Format(time.RFC3339)
	sum := sha256.Sum256([]byte(b.cfg.ClientKey + "|" + ts))
	sig, err := rsa.SignPKCS1v15(rand.Reader, b.cfg.PrivateKey, crypto.SHA256, sum[:])
	if err != nil {
		return "", answer{tokenErr: true, notReached: true, err: err}
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.BaseURL+"/v1.0/access-token/b2b",
		bytes.NewReader([]byte(`{"grantType":"client_credentials"}`)))
	if err != nil {
		return "", answer{tokenErr: true, notReached: true, err: err}
	}
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("X-TIMESTAMP", ts)
	hr.Header.Set("X-CLIENT-KEY", b.cfg.ClientKey)
	hr.Header.Set("X-SIGNATURE", base64.StdEncoding.EncodeToString(sig))
	a := b.do(ctx, "token", hr, contract.TransferRequest{})
	var out struct {
		Token     string `json:"accessToken"`
		ExpiresIn string `json:"expiresIn"`
	}
	if a.code != "2007300" || json.Unmarshal(a.body, &out) != nil || out.Token == "" {
		a.tokenErr, a.notReached = true, true
		return "", a
	}
	secs, _ := strconv.Atoi(out.ExpiresIn) // unparseable: refresh on the next call
	b.token, b.tokenExp = out.Token, now.Add(time.Duration(secs)*time.Second)
	return b.token, answer{}
}

// Signature is S:A's X-SIGNATURE: Base64(HMAC-SHA512(secret,
// METHOD:path:token:lowercase(hex(SHA-256(minified body))):timestamp)).
func Signature(secret, method, path, token string, minBody []byte, ts string) string {
	sum := sha256.Sum256(minBody)
	mac := hmac.New(sha512.New, []byte(secret))
	fmt.Fprintf(mac, "%s:%s:%s:%s:%s", method, path, token, hex.EncodeToString(sum[:]), ts)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
