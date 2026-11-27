package stuba_test

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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/banksim/stuba"
	"fintech-ledger-playground/internal/logger"
)

const (
	clientKey = "962489e9-de5d-4eb7-92a4-b07d44d64bf5"
	secret    = "secretA"
	partner   = "PG-AGG-001"
	acct      = "0201245681"
)

var key = func() *rsa.PrivateKey {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	return k
}()

type env struct {
	t     *testing.T
	h     http.Handler
	sim   *banksim.Sim
	token string
	ext   int
}

func newEnv(t *testing.T) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sim := banksim.New(stuba.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "bank-simulator"), sim)
	if err != nil {
		t.Fatal(err)
	}
	stuba.Register(r, sim, stuba.Config{ClientKey: clientKey, PartnerPublicKey: &key.PublicKey, ClientSecret: secret,
		PartnerID: partner, TokenTTL: 15 * time.Minute})
	return &env{t: t, h: r, sim: sim}
}

func (e *env) serve(req *http.Request) (int, map[string]any) {
	e.t.Helper()
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

func (e *env) tokenReq(signWith *rsa.PrivateKey) *http.Request {
	ts := time.Now().Format(time.RFC3339)
	sum := sha256.Sum256([]byte(clientKey + "|" + ts))
	sig, err := rsa.SignPKCS1v15(rand.Reader, signWith, crypto.SHA256, sum[:])
	if err != nil {
		e.t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1.0/access-token/b2b", strings.NewReader(`{"grantType":"client_credentials"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TIMESTAMP", ts)
	req.Header.Set("X-CLIENT-KEY", clientKey)
	req.Header.Set("X-SIGNATURE", base64.StdEncoding.EncodeToString(sig))
	return req
}

func (e *env) login() {
	e.t.Helper()
	code, m := e.serve(e.tokenReq(key))
	if code != 200 || m["responseCode"] != "2007300" || m["tokenType"] != "Bearer" || m["expiresIn"] != "900" {
		e.t.Fatalf("token: %d %v", code, m)
	}
	e.token = m["accessToken"].(string)
}

func sts(method, path, token string, body []byte, ts string) string {
	sum := sha256.Sum256(body)
	return method + ":" + path + ":" + token + ":" + hex.EncodeToString(sum[:]) + ":" + ts
}

// signed builds a transactional SNAP request; tamper changes the body after
// signing.
func (e *env) signed(path, body string, tamper bool) *http.Request {
	e.t.Helper()
	ts := time.Now().Format(time.RFC3339)
	min, err := banksim.Minify([]byte(body))
	if err != nil {
		e.t.Fatal(err)
	}
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write([]byte(sts("POST", path, e.token, min, ts)))
	if tamper {
		body = strings.Replace(body, "2500000.00", "9500000.00", 1)
	}
	e.ext++
	req := httptest.NewRequest("POST", path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+e.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-TIMESTAMP", ts)
	req.Header.Set("X-SIGNATURE", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	req.Header.Set("X-PARTNER-ID", partner)
	req.Header.Set("CHANNEL-ID", "95221")
	req.Header.Set("X-EXTERNAL-ID", "2026112509151000000"+string(rune('0'+e.ext%10)))
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
	inquiryPath  = "/interbank/snap/v1.0/account-inquiry-external"
	transferPath = "/interbank/snap/v1.0/transfer-interbank"
	statusPath   = "/v1.0/transfer/status"
)

func inquiryBody() string {
	return `{"beneficiaryAccountCode":"014","beneficiaryAccountNo":"` + acct + `","additionalInfo":{"deviceId":"pg-node-jkt-01","channel":"BIFAST"}}`
}

func transferBody(ref string) string {
	return `{
  "partnerReferenceNo": "` + ref + `",
  "amount": {"value": "2500000.00", "currency": "IDR"},
  "beneficiaryAccountName": "PT NUSANTARA LOGISTIK",
  "beneficiaryAccountNo": "` + acct + `",
  "beneficiaryBankCode": "014",
  "sourceAccountNo": "000101002345501",
  "transactionDate": "2026-11-25T09:15:10+07:00",
  "additionalInfo": {"deviceId": "pg-node-jkt-01", "channel": "BIFAST"}
}`
}

func statusBody(ref string) string {
	return `{"originalPartnerReferenceNo":"` + ref + `","serviceCode":"18","transactionDate":"2026-11-25T09:15:10+07:00"}`
}

// The contract's flow: token → inquiry → transfer → status.
func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	e.login()
	code, m := e.serve(e.signed(inquiryPath, inquiryBody(), false))
	if code != 200 || m["responseCode"] != "2001600" || m["beneficiaryAccountNo"] != acct || m["beneficiaryAccountName"] == "" {
		t.Fatalf("inquiry: %d %v", code, m)
	}
	code, m = e.serve(e.signed(transferPath, transferBody("PG-TRX-1"), false))
	if code != 200 || m["responseCode"] != "2001800" || m["partnerReferenceNo"] != "PG-TRX-1" || m["referenceNo"] == "" {
		t.Fatalf("transfer: %d %v", code, m)
	}
	if amt := m["amount"].(map[string]any); amt["value"] != "2500000.00" || amt["currency"] != "IDR" {
		t.Fatalf("amount = %v", amt)
	}
	code, m = e.serve(e.signed(statusPath, statusBody("PG-TRX-1"), false))
	if code != 200 || m["responseCode"] != "2003600" || m["latestTransactionStatus"] != "00" || m["originalReferenceNo"] == "" {
		t.Fatalf("status: %d %v", code, m)
	}
}

// The same partnerReferenceNo twice is one transfer: the bank's idempotency.
func TestTransferIsIdempotentOnPartnerReference(t *testing.T) {
	e := newEnv(t)
	e.login()
	_, first := e.serve(e.signed(transferPath, transferBody("PG-TRX-2"), false))
	code, again := e.serve(e.signed(transferPath, transferBody("PG-TRX-2"), false))
	if code != 200 || again["referenceNo"] != first["referenceNo"] {
		t.Fatalf("repeat: %d %v, first %v", code, again, first)
	}
	if code, m := e.serve(e.signed(transferPath, strings.Replace(transferBody("PG-TRX-2"), "2500000.00", "100.00", 1), false)); code != 409 {
		t.Fatalf("same reference, other amount: %d %v", code, m)
	}
}

func TestTokenRejectsWrongKeyAndClient(t *testing.T) {
	e := newEnv(t)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	if code, m := e.serve(e.tokenReq(other)); code != 401 || m["responseCode"] != "4017300" {
		t.Fatalf("other key: %d %v", code, m)
	}
	req := e.tokenReq(key)
	req.Header.Set("X-CLIENT-KEY", "someone-else")
	if code, _ := e.serve(req); code != 401 {
		t.Fatalf("other client: %d", code)
	}
	if n := callCount(e, "token"); n != 0 {
		t.Fatalf("rejected token requests counted: %d", n)
	}
}

func TestSignedCallRejections(t *testing.T) {
	e := newEnv(t)
	e.login()
	if code, m := e.serve(e.signed(transferPath, transferBody("PG-TRX-3"), true)); code != 401 || m["responseCode"] != "4011800" {
		t.Fatalf("tampered body: %d %v", code, m)
	}
	req := e.signed(transferPath, transferBody("PG-TRX-3"), false)
	req.Header.Set("Authorization", "Bearer not-issued")
	if code, m := e.serve(req); code != 401 || m["responseCode"] != "4011801" {
		t.Fatalf("unknown token: %d %v", code, m)
	}
	req = e.signed(transferPath, transferBody("PG-TRX-3"), false)
	req.Header.Del("X-EXTERNAL-ID")
	if code, m := e.serve(req); code != 400 || m["responseCode"] != "4001802" {
		t.Fatalf("missing header: %d %v", code, m)
	}
	req = e.signed(transferPath, transferBody("PG-TRX-3"), false)
	req.Header.Set("X-PARTNER-ID", "PG-OTHER")
	if code, _ := e.serve(req); code != 401 {
		t.Fatalf("other partner: %d", code)
	}
	if n := callCount(e, "transfer"); n != 0 {
		t.Fatalf("rejected transfers reached the handler: %d", n)
	}
	if code, m := e.serve(e.signed(statusPath, statusBody("PG-TRX-3"), false)); m["latestTransactionStatus"] != "07" {
		t.Fatalf("nothing was transferred, status = %d %v", code, m)
	}
}

func TestExpiredTokenIsRejected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sim := banksim.New(stuba.Steps)
	r, _ := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "s"), sim)
	now := time.Now()
	clock := func() time.Time { return now }
	stuba.Register(r, sim, stuba.Config{ClientKey: clientKey, PartnerPublicKey: &key.PublicKey, ClientSecret: secret,
		PartnerID: partner, TokenTTL: time.Minute, Now: clock})
	e := &env{t: t, h: r, sim: sim}
	code, m := e.serve(e.tokenReq(key))
	if code != 200 || m["expiresIn"] != "60" {
		t.Fatalf("token: %d %v", code, m)
	}
	e.token = m["accessToken"].(string)
	now = now.Add(61 * time.Second)
	if code, m := e.serve(e.signed(inquiryPath, inquiryBody(), false)); code != 401 || m["responseCode"] != "4011601" {
		t.Fatalf("expired token: %d %v", code, m)
	}
}

func TestInvalidAmountIsRejected(t *testing.T) {
	e := newEnv(t)
	e.login()
	for _, v := range []string{"2500000.5", "1e6", "-5.00", "abc"} {
		body := strings.Replace(transferBody("PG-TRX-4"), "2500000.00", v, 1)
		if code, m := e.serve(e.signed(transferPath, body, false)); code != 400 || m["responseCode"] != "4001801" {
			t.Errorf("amount %q: %d %v", v, code, m)
		}
	}
}

// T§11.1 (A) rows: the simulator answers whatever code the test injects.
func TestStatusFaultAnswersTheInjectedCode(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"inquiry","account_no":"` + acct + `"},"action":"status","http_status":404,"code":"4041611"}`)
	if code, m := e.serve(e.signed(inquiryPath, inquiryBody(), false)); code != 404 || m["responseCode"] != "4041611" {
		t.Fatalf("inquiry fault: %d %v", code, m)
	}
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":403,"code":"4031814"}`)
	if code, m := e.serve(e.signed(transferPath, transferBody("PG-TRX-5"), false)); code != 403 || m["responseCode"] != "4031814" {
		t.Fatalf("transfer fault: %d %v", code, m)
	}
	if _, m := e.serve(e.signed(statusPath, statusBody("PG-TRX-5"), false)); m["latestTransactionStatus"] != "07" {
		t.Fatalf("a refused transfer must not exist: %v", m)
	}
	e.fault(`{"match":{"step":"transfer"},"action":"status","http_status":500}`)
	if code, m := e.serve(e.signed(transferPath, transferBody("PG-TRX-6"), false)); code != 500 || m["responseCode"] != "5001800" {
		t.Fatalf("default code: %d %v", code, m)
	}
}

func TestPendingThenSuccess(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_success","polls":2}`)
	if code, m := e.serve(e.signed(transferPath, transferBody("PG-TRX-7"), false)); code != 202 || m["responseCode"] != "2021800" {
		t.Fatalf("transfer: %d %v", code, m)
	}
	for _, want := range []string{"03", "03", "00", "00"} {
		if _, m := e.serve(e.signed(statusPath, statusBody("PG-TRX-7"), false)); m["latestTransactionStatus"] != want {
			t.Fatalf("status = %v, want %s", m["latestTransactionStatus"], want)
		}
	}
}

func TestPendingThenFail(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_fail"}`)
	e.serve(e.signed(transferPath, transferBody("PG-TRX-8"), false))
	for _, want := range []string{"03", "06"} {
		if _, m := e.serve(e.signed(statusPath, statusBody("PG-TRX-8"), false)); m["latestTransactionStatus"] != want {
			t.Fatalf("status = %v, want %s", m["latestTransactionStatus"], want)
		}
	}
}

func TestPendingForever(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"pending_then_success","polls":-1}`)
	e.serve(e.signed(transferPath, transferBody("PG-TRX-9"), false))
	for i := 0; i < 5; i++ {
		if _, m := e.serve(e.signed(statusPath, statusBody("PG-TRX-9"), false)); m["latestTransactionStatus"] != "03" {
			t.Fatalf("poll %d = %v", i, m)
		}
	}
}

// A timeout still moves the money: the caller can't tell, so it must ask.
func TestTimeoutProcessesThenHolds(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.fault(`{"match":{"step":"transfer"},"action":"timeout","delay_ms":5000}`)
	req := e.signed(transferPath, transferBody("PG-TRX-10"), false)
	ctx, cancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	e.serve(req.WithContext(ctx))
	if time.Since(start) > 2*time.Second {
		t.Fatal("held past the caller's deadline")
	}
	if _, m := e.serve(e.signed(statusPath, statusBody("PG-TRX-10"), false)); m["latestTransactionStatus"] != "00" {
		t.Fatalf("status after timeout = %v, want 00 (processed)", m)
	}
}

func callCount(e *env, step string) int {
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, httptest.NewRequest("GET", "/_sim/calls", nil))
	var m map[string]int
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return m[step]
}

func TestCallsCountedPerStep(t *testing.T) {
	e := newEnv(t)
	e.login()
	e.serve(e.signed(transferPath, transferBody("PG-TRX-11"), false))
	e.serve(e.signed(statusPath, statusBody("PG-TRX-11"), false))
	if callCount(e, "token") != 1 || callCount(e, "transfer") != 1 || callCount(e, "status") != 1 {
		t.Fatal("calls not counted")
	}
}

// Known answer from openssl: printf '%s' "$STS" | openssl dgst -sha512 -hmac secretA -binary | base64
func TestSignatureKnownAnswer(t *testing.T) {
	got := stuba.Signature("secretA", "POST", transferPath, "tokA", []byte(`{"a":1}`), "2026-11-25T09:15:10+07:00")
	if want := "oOFENy3U1SRKHumZzxVnvPvaKGspBclzaI/d5LAzRoBXqwt1xBmFUE+RpvG8YYPA75fLr/nVRRH4lumgGFsKXg=="; got != want {
		t.Fatalf("signature = %s, want %s", got, want)
	}
}
