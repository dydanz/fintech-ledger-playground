// Package stubb is bank-middleware-b's adapter for Bank-Stub-B, the
// corporate host-to-host bank (bank-api-stubs S:B): an OAuth Basic token
// cached until expires_in, then X-BCA-* HMAC-SHA256-signed beneficiary
// inquiry, single transfer and status. The mapping is T§11.2; see README.md.
package stubb

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/moneyout/contract"
)

type Config struct {
	BaseURL                string
	ClientID, ClientSecret string // OAuth Basic
	APIKey, APISecret      string // X-BCA-Key and the HMAC key
	CorporateID            string
	SourceAccount          string // SourceAccountNumber (T§12.1 #9)
	Now                    func() time.Time
}

// BankCode is bank B's own code: S:B's single transfer is BCA-to-BCA and
// carries no beneficiary bank code (T§12.1 #8).
const BankCode = "014"

const (
	tokenMargin     = 30 * time.Second
	timestampLayout = "2006-01-02T15:04:05.000Z07:00"
)

var wib = time.FixedZone("WIB", 7*60*60)

var (
	failed  = bankmw.Failed
	unknown = bankmw.Unknown
)

// errorCodes are S:B's ErrorCode values (A: the contract shows no error
// body) and what they mean. A code not listed is UNKNOWN on a transfer.
var errorCodes = map[string]struct {
	reason contract.Reason
	retry  bool
}{
	"ESB-82-001": {contract.ReasonInvalidAccount, false},
	"ESB-82-002": {contract.ReasonAccountClosed, false},
	"ESB-14-021": {contract.ReasonInsufficientSourceFunds, true},
	"ESB-14-022": {contract.ReasonLimitExceeded, true},
	"ESB-99-009": {contract.ReasonRailUnavailable, true},
}

// failureReasons map a REJECTED status's FailureReason (A).
var failureReasons = map[string]contract.Reason{
	"ACCOUNT_CLOSED":  contract.ReasonAccountClosed,
	"ACCOUNT_DORMANT": contract.ReasonAccountClosed,
	"INVALID_ACCOUNT": contract.ReasonInvalidAccount,
}

type Bank struct {
	cfg    Config
	client *http.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

func New(cfg Config, client *http.Client) (*Bank, error) {
	if cfg.BaseURL == "" || cfg.ClientID == "" || cfg.ClientSecret == "" || cfg.APIKey == "" || cfg.APISecret == "" ||
		cfg.CorporateID == "" || cfg.SourceAccount == "" {
		return nil, errors.New("stubb: base URL, OAuth client, API key and secret, corporate id and source account are required")
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Bank{cfg: cfg, client: client}, nil
}

// answer is one call's outcome.
type answer struct {
	bankmw.Response
	tokenErr bool
}

func (a answer) preBank() (contract.BankResult, bool) {
	switch {
	case a.tokenErr && a.Status == 0: // the bank is unreachable
		return failed(contract.ReasonRailUnavailable, true, false, ""), true
	case a.tokenErr || a.Status == http.StatusUnauthorized:
		return failed(contract.ReasonAuthError, true, false, a.Code), true
	}
	return contract.BankResult{}, false
}

// Transfer: beneficiary inquiry, then the single corporate transfer (T§11.2).
func (b *Bank) Transfer(ctx context.Context, req contract.TransferRequest) contract.BankResult {
	// Only an intrabank transfer to bank B can be addressed: any other would
	// credit the same account number at bank B (T§12.1 #8).
	if req.Rail != contract.RailIntrabank || req.Beneficiary.BankCode != BankCode {
		return failed(contract.ReasonRailUnavailable, true, false, "")
	}
	money, err := req.Amount.Money()
	if err != nil {
		return failed(contract.ReasonOther, false, false, "")
	}
	a := b.call(ctx, "inquiry", http.MethodPost, "/banking/v3/corporates/transfers/inquiry", req, map[string]string{
		"CorpCode": b.cfg.CorporateID, "InquiryRef": TransactionID(req.RequestID), "TargetBank": req.Beneficiary.BankCode,
		"DestAcctNo": req.Beneficiary.AccountNo})
	if r, ok := a.preBank(); ok {
		return r
	}
	var inq struct{ AccountStatus string }
	switch {
	case a.Status == http.StatusOK && json.Unmarshal(a.Body, &inq) == nil && inq.AccountStatus == "ACTIVE":
	case a.Status == http.StatusOK && (inq.AccountStatus == "CLOSED" || inq.AccountStatus == "DORMANT"):
		return failed(contract.ReasonAccountClosed, false, true, inq.AccountStatus)
	case a.Status == http.StatusOK:
		return failed(contract.ReasonInvalidAccount, false, true, inq.AccountStatus)
	default:
		if m, ok := errorCodes[a.Code]; ok && (m.reason == contract.ReasonInvalidAccount || m.reason == contract.ReasonAccountClosed) {
			return failed(m.reason, m.retry, true, a.Code)
		}
		return failed(contract.ReasonRailUnavailable, true, false, a.Code) // no transfer was sent
	}

	txID := TransactionID(req.RequestID)
	a = b.call(ctx, "transfer", http.MethodPost, "/banking/corporates/transfers", req, map[string]string{
		"CorporateID": b.cfg.CorporateID, "SourceAccountNumber": b.cfg.SourceAccount, "TransactionID": txID,
		"TransactionDate": TransactionDate(req.RequestID, b.cfg.Now()), "ReferenceID": req.RequestID, "CurrencyCode": money.Currency,
		"Amount": contract.AmountOf(money).Value, "BeneficiaryAccountNumber": req.Beneficiary.AccountNo,
		"Remark1": "Payout", "Remark2": "Online Transfer"})
	if r, ok := a.preBank(); ok {
		return r
	}
	var out struct{ Status string }
	switch {
	case a.NotReached:
		return failed(contract.ReasonRailUnavailable, true, false, "")
	case a.Status == http.StatusOK && json.Unmarshal(a.Body, &out) == nil && out.Status == "Success":
		return contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: txID, BankRawCode: out.Status}
	case a.Status/100 == 4 && a.Status != http.StatusConflict:
		if m, ok := errorCodes[a.Code]; ok {
			return failed(m.reason, m.retry, true, a.Code)
		}
	}
	// A timeout, 5xx (including 504 "in progress"), a duplicate, or anything
	// unmapped: only the status inquiry can tell.
	return unknown(a.Code)
}

// InquireStatus asks about the transfer sent as TransactionID(request_id).
func (b *Bank) InquireStatus(ctx context.Context, transferID, requestID string) contract.BankResult {
	a := b.call(ctx, "status", http.MethodPost, "/banking/v3/corporates/transfers/status",
		contract.TransferRequest{TransferID: transferID, RequestID: requestID}, map[string]string{
			"CorporateID": b.cfg.CorporateID, "OriginalTransactionID": TransactionID(requestID), "OriginalReferenceID": requestID,
			"TransactionDate": TransactionDate(requestID, b.cfg.Now())})
	if a.Status == http.StatusNotFound && a.Code == "ESB-82-404" {
		return failed(contract.ReasonOther, true, false, a.Code) // the bank has no such transfer
	}
	var out struct {
		SettlementState, BankSettlementRef string
		FailureReason                      *string
	}
	if a.Status != http.StatusOK || json.Unmarshal(a.Body, &out) != nil {
		return unknown(a.Code)
	}
	switch out.SettlementState {
	case "SETTLED":
		return contract.BankResult{Status: contract.StatusSucceeded, ReachedBank: true, BankReference: out.BankSettlementRef, BankRawCode: out.SettlementState}
	case "PENDING":
		return contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true, BankReference: out.BankSettlementRef, BankRawCode: out.SettlementState}
	case "REJECTED":
		fr := ""
		if out.FailureReason != nil {
			fr = *out.FailureReason
		}
		reason, ok := failureReasons[fr]
		if !ok {
			reason = contract.ReasonBankRejected
		}
		r := failed(reason, contract.DefaultRetryElsewhere(reason), true, fr)
		r.BankReference = out.BankSettlementRef
		return r
	}
	return unknown(out.SettlementState)
}

// TransactionID is the 8-character S:B TransactionID for an attempt (A):
// base36 of SHA-256(request_id), so a stateless BMW can ask about it later.
// About 2.8e12 values: a collision would be refused as a duplicate (409,
// UNKNOWN), but its status inquiry would read the other transfer (see
// README).
func TransactionID(requestID string) string {
	sum := sha256.Sum256([]byte(requestID))
	n := binary.BigEndian.Uint64(sum[:8]) % 2821109907456 // 36^8
	const digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	out := make([]byte, 8)
	for i := 7; i >= 0; i-- {
		out[i], n = digits[n%36], n/36
	}
	return string(out)
}

// TransactionDate is the attempt's date in WIB: from a UUIDv7 request_id's
// timestamp (MOM's), so it's the same at transfer and at a later status
// inquiry; otherwise now.
func TransactionDate(requestID string, now time.Time) string {
	if id, err := uuid.Parse(requestID); err == nil && id.Version() == 7 {
		sec, nsec := id.Time().UnixTime()
		return time.Unix(sec, nsec).In(wib).Format("2006-01-02")
	}
	return now.In(wib).Format("2006-01-02")
}

func (b *Bank) call(ctx context.Context, step, method, path string, req contract.TransferRequest, body any) answer {
	raw, err := json.Marshal(body)
	if err != nil {
		return answer{Response: bankmw.Response{NotReached: true, Err: err}}
	}
	a := b.send(ctx, step, method, path, req, raw, false)
	if a.Status == http.StatusUnauthorized && !a.tokenErr {
		a = b.send(ctx, step, method, path, req, raw, true)
	}
	return a
}

func (b *Bank) send(ctx context.Context, step, method, path string, req contract.TransferRequest, raw []byte, refresh bool) answer {
	tok, failure := b.accessToken(ctx, refresh)
	if tok == "" {
		return failure
	}
	ts := b.cfg.Now().In(wib).Format(timestampLayout)
	hr, err := http.NewRequestWithContext(ctx, method, b.cfg.BaseURL+path, bytes.NewReader(raw))
	if err != nil {
		return answer{Response: bankmw.Response{NotReached: true, Err: err}}
	}
	hr.Header.Set("Authorization", "Bearer "+tok)
	hr.Header.Set("Content-Type", "application/json")
	hr.Header.Set("X-BCA-Key", b.cfg.APIKey)
	hr.Header.Set("X-BCA-Timestamp", ts)
	hr.Header.Set("X-BCA-Signature", Signature(b.cfg.APISecret, method, path, tok, raw, ts))
	return answer{Response: bankmw.Do(ctx, b.client, step, hr, req, errorCode)}
}

func errorCode(body []byte) string {
	var e struct{ ErrorCode string }
	_ = json.Unmarshal(body, &e)
	return e.ErrorCode
}

func (b *Bank) accessToken(ctx context.Context, refresh bool) (string, answer) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.cfg.Now()
	if !refresh && b.token != "" && now.Before(b.tokenExp.Add(-tokenMargin)) {
		return b.token, answer{}
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.BaseURL+"/api/oauth/token",
		strings.NewReader(url.Values{"grant_type": {"client_credentials"}}.Encode()))
	if err != nil {
		return "", answer{Response: bankmw.Response{NotReached: true, Err: err}, tokenErr: true}
	}
	hr.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	hr.SetBasicAuth(b.cfg.ClientID, b.cfg.ClientSecret)
	r := bankmw.Do(ctx, b.client, "token", hr, contract.TransferRequest{}, errorCode)
	var out struct {
		Token     string `json:"access_token"`
		ExpiresIn int    `json:"expires_in"`
	}
	if r.Status != http.StatusOK || json.Unmarshal(r.Body, &out) != nil || out.Token == "" {
		return "", answer{Response: r, tokenErr: true}
	}
	b.token, b.tokenExp = out.Token, now.Add(time.Duration(out.ExpiresIn)*time.Second)
	return b.token, answer{}
}

// Signature is S:B's X-BCA-Signature (T§12.1 #2): Base64(HMAC-SHA256(
// apiSecret, METHOD:RelativeUrl:AccessToken:lowercase(hex(SHA-256(minified
// body))):Timestamp)).
func Signature(secret, method, relativeURL, token string, minBody []byte, ts string) string {
	sum := sha256.Sum256(minBody)
	mac := hmac.New(sha256.New, []byte(secret))
	fmt.Fprintf(mac, "%s:%s:%s:%s:%s", method, relativeURL, token, hex.EncodeToString(sum[:]), ts)
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
