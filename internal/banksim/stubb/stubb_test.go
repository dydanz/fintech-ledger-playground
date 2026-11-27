package stubb_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/banksim/stubb"
	"fintech-ledger-playground/internal/logger"
)

const (
	clientID, clientSecret = "bmw-b", "oauth-secret"
	apiKey, apiSecret      = "a16c5bb4-49d1-4a12-9194-db3df367d893", "secretB"
	corp                   = "CORPAGG01"
	acct                   = "0201245681"
	tsLayout               = "2006-01-02T15:04:05.000Z07:00"
)

type env struct {
	t     *testing.T
	h     http.Handler
	token string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sim := banksim.New(stubb.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "bank-simulator"), sim)
	if err != nil {
		t.Fatal(err)
	}
	stubb.Register(r, sim, stubb.Config{ClientID: clientID, ClientSecret: clientSecret, APIKey: apiKey, APISecret: apiSecret,
		CorporateID: corp, TokenTTL: time.Hour})
	return &env{t: t, h: r}
}

func (e *env) serve(req *http.Request) (int, map[string]any) {
	e.t.Helper()
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

func (e *env) tokenReq(id, secret string) *http.Request {
	req := httptest.NewRequest("POST", "/api/oauth/token", strings.NewReader("grant_type=client_credentials"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(id, secret)
	return req
}

func (e *env) login() {
	e.t.Helper()
	code, m := e.serve(e.tokenReq(clientID, clientSecret))
	if code != 200 || m["token_type"] != "bearer" || m["expires_in"] != 3600.0 || m["access_token"] == "" {
		e.t.Fatalf("token: %d %v", code, m)
	}
	e.token = m["access_token"].(string)
}

func (e *env) signed(method, path, body string, tamper bool) *http.Request {
	e.t.Helper()
	ts := time.Now().Format(tsLayout)
	min, _ := banksim.Minify([]byte(body))
	sum := sha256.Sum256(min)
	mac := hmac.New(sha256.New, []byte(apiSecret))
	mac.Write([]byte(method + ":" + path + ":" + e.token + ":" + hex.EncodeToString(sum[:]) + ":" + ts))
	if tamper {
		body = strings.Replace(body, "750965.00", "950965.00", 1)
	}
	var rd *strings.Reader
	if body != "" {
		rd = strings.NewReader(body)
	} else {
		rd = strings.NewReader("")
	}
	req := httptest.NewRequest(method, path, rd)
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-BCA-Key", apiKey)
	req.Header.Set("X-BCA-Timestamp", ts)
	req.Header.Set("X-BCA-Signature", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return req
}

func (e *env) fault(body string) {
	e.t.Helper()
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, httptest.NewRequest("POST", "/_sim/faults", strings.NewReader(body)))
	if w.Code != 201 {
		e.t.Fatalf("fault: %d %s", w.Code, w.Body)
	}
}

const (
	balancePath  = "/banking/v3/corporates/" + corp + "/accounts/0201245680"
	inquiryPath  = "/banking/v3/corporates/transfers/inquiry"
	transferPath = "/banking/corporates/transfers"
	statusPath   = "/banking/v3/corporates/transfers/status"
)

func inquiryBody() string {
	return `{"CorpCode":"` + corp + `","InquiryRef":"INQ1","TargetBank":"014","DestAcctNo":"` + acct + `"}`
}

func transferBody(id string) string {
	return `{
  "CorporateID": "` + corp + `", "SourceAccountNumber": "0201245680", "TransactionID": "` + id + `",
  "TransactionDate": "2026-11-25", "ReferenceID": "2026-11-25-PO-105", "CurrencyCode": "IDR",
  "Amount": "750965.00", "BeneficiaryAccountNumber": "` + acct + `", "Remark1": "Payout Settlement", "Remark2": "Online Transfer"
}`
}

func statusBody(id string) string {
	return `{"CorporateID":"` + corp + `","OriginalTransactionID":"` + id + `","OriginalReferenceID":"2026-11-25-PO-105","TransactionDate":"2026-11-25"}`
}

// The contract's flow: token → balance → beneficiary inquiry → transfer → status.
func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	e.login()
	code, m := e.serve(e.signed("GET", balancePath, "", false))
	ok := code == 200 && len(m["AccountDetailDataSuccess"].([]any)) == 1
	if !ok {
		t.Fatalf("balance: %d %v", code, m)
	}
	if bal := m["AccountDetailDataSuccess"].([]any)[0].(map[string]any); bal["AccountNumber"] != "0201245680" || bal["Currency"] != "IDR" {
		t.Fatalf("balance = %v", bal)
	}
	code, m = e.serve(e.signed("POST", inquiryPath, inquiryBody(), false))
	if code != 200 || m["AccountStatus"] != "ACTIVE" || m["DestAcctNo"] != acct || m["AccountHolderName"] == "" {
		t.Fatalf("inquiry: %d %v", code, m)
	}
	code, m = e.serve(e.signed("POST", transferPath, transferBody("00000105"), false))
	if code != 200 || m["Status"] != "Success" || m["TransactionID"] != "00000105" || m["ReferenceID"] != "2026-11-25-PO-105" {
		t.Fatalf("transfer: %d %v", code, m)
	}
	code, m = e.serve(e.signed("POST", statusPath, statusBody("00000105"), false))
	if code != 200 || m["SettlementState"] != "SETTLED" || m["BankSettlementRef"] == "" || m["FailureReason"] != nil {
		t.Fatalf("status: %d %v", code, m)
	}
}

func TestTokenRejectsWrongCredentials(t *testing.T) {
	e := newEnv(t)
	if code, m := e.serve(e.tokenReq(clientID, "wrong")); code != 401 || m["ErrorCode"] != "ESB-14-001" {
		t.Fatalf("wrong secret: %d %v", code, m)
	}
}

func TestSignedCallRejections(t *testing.T) {
	e := newEnv(t)
	e.login()
	if code, m := e.serve(e.signed("POST", transferPath, transferBody("00000106"), true)); code != 401 || m["ErrorCode"] != "ESB-14-002" {
		t.Fatalf("tampered: %d %v", code, m)
	}
	req := e.signed("POST", transferPath, transferBody("00000106"), false)
	req.Header.Set("X-BCA-Key", "other-key")
	if code, m := e.serve(req); code != 401 || m["ErrorCode"] != "ESB-14-002" {
		t.Fatalf("other key: %d %v", code, m)
	}
	req = e.signed("POST", transferPath, transferBody("00000106"), false)
	req.Header.Set("Authorization", "Bearer unknown")
	if code, m := e.serve(req); code != 401 || m["ErrorCode"] != "ESB-14-001" {
		t.Fatalf("unknown token: %d %v", code, m)
	}
	req = e.signed("POST", transferPath, transferBody("00000106"), false)
	req.Header.Set("X-BCA-Timestamp", "yesterday")
	if code, m := e.serve(req); code != 400 || m["ErrorCode"] != "ESB-14-003" {
		t.Fatalf("bad timestamp: %d %v", code, m)
	}
	if code, m := e.serve(e.signed("POST", statusPath, statusBody("00000106"), false)); code != 404 || m["ErrorCode"] != "ESB-82-404" {
		t.Fatalf("a rejected transfer must not exist: %d %v", code, m)
	}
}

func TestTransferIdempotentOnTransactionID(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.serve(e.signed("POST", transferPath, transferBody("00000107"), false))
	if code, m := e.serve(e.signed("POST", transferPath, transferBody("00000107"), false)); code != 200 || m["Status"] != "Success" {
		t.Fatalf("repeat: %d %v", code, m)
	}
	if code, m := e.serve(e.signed("POST", transferPath, strings.Replace(transferBody("00000107"), "750965.00", "1.00", 1), false)); code != 409 || m["ErrorCode"] != "ESB-82-409" {
		t.Fatalf("same id, other amount: %d %v", code, m)
	}
}

func TestInvalidAmountIsRejected(t *testing.T) {
	e := newEnv(t)
	e.login()
	for _, v := range []string{"750965.5", "1e6", "-1.00"} {
		if code, m := e.serve(e.signed("POST", transferPath, strings.Replace(transferBody("00000108"), "750965.00", v, 1), false)); code != 400 || m["ErrorCode"] != "ESB-14-004" {
			t.Errorf("%s: %d %v", v, code, m)
		}
	}
}

// T§11.2 (A) rows: an injected AccountStatus, and an injected ErrorCode.
func TestStatusFaults(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"inquiry","account_no":"` + acct + `"},"action":"status","code":"CLOSED"}`)
	if code, m := e.serve(e.signed("POST", inquiryPath, inquiryBody(), false)); code != 200 || m["AccountStatus"] != "CLOSED" {
		t.Fatalf("inquiry: %d %v", code, m)
	}
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":400,"code":"ESB-14-021"}`)
	code, m := e.serve(e.signed("POST", transferPath, transferBody("00000109"), false))
	if code != 400 || m["ErrorCode"] != "ESB-14-021" || m["ErrorMessage"].(map[string]any)["English"] == "" {
		t.Fatalf("transfer: %d %v", code, m)
	}
}

func TestPendingThenFail(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_fail","polls":1,"code":"ACCOUNT_DORMANT"}`)
	if code, m := e.serve(e.signed("POST", transferPath, transferBody("00000110"), false)); code != 504 || m["ErrorCode"] != "ESB-99-504" {
		t.Fatalf("transfer: %d %v", code, m)
	}
	if _, m := e.serve(e.signed("POST", statusPath, statusBody("00000110"), false)); m["SettlementState"] != "PENDING" {
		t.Fatalf("poll 1 = %v", m)
	}
	if _, m := e.serve(e.signed("POST", statusPath, statusBody("00000110"), false)); m["SettlementState"] != "REJECTED" || m["FailureReason"] != "ACCOUNT_DORMANT" {
		t.Fatalf("poll 2 = %v", m)
	}
}

func TestPendingThenSuccess(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_success","polls":1}`)
	e.serve(e.signed("POST", transferPath, transferBody("00000111"), false))
	for _, want := range []string{"PENDING", "SETTLED"} {
		if _, m := e.serve(e.signed("POST", statusPath, statusBody("00000111"), false)); m["SettlementState"] != want {
			t.Fatalf("status = %v, want %s", m, want)
		}
	}
}

func TestTimeoutProcessesThenHolds(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"timeout","delay_ms":5000}`)
	req := e.signed("POST", transferPath, transferBody("00000112"), false)
	ctx, cancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer cancel()
	e.serve(req.WithContext(ctx))
	if _, m := e.serve(e.signed("POST", statusPath, statusBody("00000112"), false)); m["SettlementState"] != "SETTLED" {
		t.Fatalf("status = %v", m)
	}
}

// Known answer from openssl: printf '%s' "$STS" | openssl dgst -sha256 -hmac secretB -binary | base64
func TestSignatureKnownAnswer(t *testing.T) {
	got := stubb.Signature("secretB", "POST", "/banking/corporates/transfers", "tokB", []byte(`{"a":1}`), "2026-11-25T09:20:10.000+07:00")
	if want := "KGoMQWMNVUSeJiGBGPDpd6s5dXt4oIQPU63Dlf5ZK4s="; got != want {
		t.Fatalf("signature = %s, want %s", got, want)
	}
}
