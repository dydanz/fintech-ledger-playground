package stubd_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/banksim/stubd"
	"fintech-ledger-playground/internal/logger"
)

const (
	clientKey = "ent_client_d_004"
	secret    = "secretD"
	acct      = "700192837400"
	funding   = "8800112233"
)

type env struct {
	t *testing.T
	h http.Handler
}

func newEnv(t *testing.T) *env {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sim := banksim.New(stubd.Steps)
	r, err := banksim.NewEngine(logger.New(&bytes.Buffer{}, slog.LevelDebug, "bank-simulator"), sim)
	if err != nil {
		t.Fatal(err)
	}
	stubd.Register(r, sim, stubd.Config{ClientKey: clientKey, Secret: secret, FundingAcct: funding, Liquidity: 950000000})
	return &env{t: t, h: r}
}

func (e *env) call(step, data string, tamper bool) (int, map[string]any, map[string]any) {
	e.t.Helper()
	body := `{"meta":{"client-trace-id":"TRACE-D-1","sent-at":"2026-11-25T09:40:00+07:00"},"data":` + data + `}`
	min, _ := banksim.Minify([]byte(body))
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write(min)
	if tamper {
		body = strings.Replace(body, "150000000", "950000000", 1)
	}
	req := httptest.NewRequest("POST", "/api/v2/disbursement/"+step, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Client-Key", clientKey)
	req.Header.Set("X-Signature-512", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return e.serve(req)
}

func (e *env) serve(req *http.Request) (int, map[string]any, map[string]any) {
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, req)
	var m struct{ Meta, Data map[string]any }
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m.Meta, m.Data
}

func (e *env) fault(body string) {
	e.t.Helper()
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, httptest.NewRequest("POST", "/_sim/faults", strings.NewReader(body)))
	if w.Code != 201 {
		e.t.Fatalf("fault: %d %s", w.Code, w.Body)
	}
}

const validate = `{"dest-bank-id":"022","dest-acct-no":"` + acct + `","rail-mode":"BIFAST"}`

func fundingCheck(rail string) string {
	return `{"funding-acct-no":"` + funding + `","requested-rail":"` + rail + `","tx-amount":150000000}`
}

func remit(order string) string {
	return `{"pg-order-id":"` + order + `","funding-acct-no":"` + funding + `","dest-bank-id":"022","dest-acct-no":"` + acct +
		`","dest-acct-name":"CV MAJU BERSAMA","rail-mode":"RTGS","tx-amount":150000000,"currency-iso":"IDR","payment-memo":"Invoice 991"}`
}

func status(order string) string { return `{"pg-order-id":"` + order + `"}` }

// The contract's flow, then the T§12.1 #4 single-order status poll.
func TestHappyPath(t *testing.T) {
	e := newEnv(t)
	if code, meta, data := e.call("acct-validate", validate, false); code != 200 || meta["status-code"] != "00" ||
		meta["status-text"] != "ACCOUNT_FOUND" || data["registered-owner"] == "" || data["bifast-proxy-eligible"] != true {
		t.Fatalf("acct-validate: %d %v %v", code, meta, data)
	}
	if code, meta, data := e.call("funding-check", fundingCheck("RTGS"), false); code != 200 || meta["status-text"] != "RAIL_AVAILABLE" ||
		data["rail-window-open"] != true || data["available-liquidity"] != 950000000.0 || data["estimated-fee"] != 30000.0 {
		t.Fatalf("funding-check: %d %v %v", code, meta, data)
	}
	code, meta, data := e.call("remit-single", remit("ORD-1"), false)
	if code != 200 || meta["status-text"] != "SUBMITTED" || data["tx-state"] != "IN_CLEARING" || data["rail-mode"] != "RTGS" ||
		!strings.HasPrefix(data["bank-rail-ref"].(string), "RTGS-") {
		t.Fatalf("remit-single: %d %v %v", code, meta, data)
	}
	for _, want := range []string{"IN_CLEARING", "SETTLED", "SETTLED"} {
		if _, meta, data := e.call("remit-status", status("ORD-1"), false); meta["status-text"] != "QUERY_OK" || data["tx-state"] != want {
			t.Fatalf("remit-status = %v %v, want %s", meta, data, want)
		}
	}
}

func TestSignatureAndKeyRequired(t *testing.T) {
	e := newEnv(t)
	if code, meta, _ := e.call("remit-single", remit("ORD-2"), true); code != 401 || meta["status-text"] != "INVALID_SIGNATURE" {
		t.Fatalf("tampered: %d %v", code, meta)
	}
	req := httptest.NewRequest("POST", "/api/v2/disbursement/remit-status", strings.NewReader(`{"meta":{},"data":{}}`))
	req.Header.Set("X-Client-Key", "someone-else")
	req.Header.Set("X-Signature-512", "x")
	if code, _, _ := e.serve(req); code != 401 {
		t.Fatalf("other client: %d", code)
	}
	if code, meta, _ := e.call("remit-status", status("ORD-2"), false); code != 404 || meta["status-text"] != "ORDER_NOT_FOUND" {
		t.Fatalf("a refused remit must not exist: %d %v", code, meta)
	}
}

func TestRemitValidationAndIdempotency(t *testing.T) {
	e := newEnv(t)
	_, _, first := e.call("remit-single", remit("ORD-3"), false)
	if code, _, again := e.call("remit-single", remit("ORD-3"), false); code != 200 || again["bank-rail-ref"] != first["bank-rail-ref"] {
		t.Fatalf("repeat: %d %v", code, again)
	}
	if code, _, _ := e.call("remit-single", strings.Replace(remit("ORD-3"), "150000000", "1", 1), false); code != 409 {
		t.Fatalf("same order, other amount: %d", code)
	}
	for name, data := range map[string]string{
		"fraction":     strings.Replace(remit("ORD-4"), "150000000", "150000000.5", 1),
		"negative":     strings.Replace(remit("ORD-4"), "150000000", "-1", 1),
		"string":       strings.Replace(remit("ORD-4"), "150000000", `"150000000"`, 1),
		"unknown rail": strings.Replace(remit("ORD-4"), `"RTGS"`, `"RTOL"`, 1),
		"currency":     strings.Replace(remit("ORD-4"), `"IDR"`, `"USD"`, 1),
	} {
		if code, meta, _ := e.call("remit-single", data, false); code != 400 || meta["status-text"] != "FORMAT_ERROR" {
			t.Errorf("%s: %d %v", name, code, meta)
		}
	}
	if code, _, _ := e.call("remit-status", `{"batch-ticket":"BLK-1"}`, false); code != 400 {
		t.Fatalf("bulk status is out of scope: %d", code)
	}
}

// T§11.4 (A) rows.
func TestStatusFaults(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"acct-validate","account_no":"` + acct + `"},"action":"status","code":"14"}`)
	if code, meta, _ := e.call("acct-validate", validate, false); code != 200 || meta["status-code"] != "14" || meta["status-text"] != "ACCOUNT_NOT_FOUND" {
		t.Fatalf("acct-validate: %d %v", code, meta)
	}
	e.fault(`{"match":{"step":"funding-check"},"action":"status","code":"WINDOW_CLOSED"}`)
	if _, _, data := e.call("funding-check", fundingCheck("RTGS"), false); data["rail-window-open"] != false {
		t.Fatalf("window: %v", data)
	}
	e.fault(`{"match":{"step":"funding-check"},"action":"status","code":"LOW_LIQUIDITY"}`)
	if _, _, data := e.call("funding-check", fundingCheck("RTGS"), false); data["available-liquidity"] != 0.0 {
		t.Fatalf("liquidity: %v", data)
	}
	e.fault(`{"match":{"step":"remit-single"},"action":"status","http_status":503}`)
	if code, meta, _ := e.call("remit-single", remit("ORD-5"), false); code != 503 || meta["status-code"] != "99" {
		t.Fatalf("remit 503: %d %v", code, meta)
	}
}

func TestPendingThenFailReturned(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_fail","polls":2,"code":"RETURNED"}`)
	e.call("remit-single", remit("ORD-6"), false)
	for _, want := range []string{"IN_CLEARING", "IN_CLEARING", "RETURNED"} {
		if _, _, data := e.call("remit-status", status("ORD-6"), false); data["tx-state"] != want {
			t.Fatalf("status = %v, want %s", data, want)
		}
	}
}

func TestPendingThenFailDefaultsToRejected(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_fail"}`)
	e.call("remit-single", remit("ORD-7"), false)
	e.call("remit-status", status("ORD-7"), false)
	if _, _, data := e.call("remit-status", status("ORD-7"), false); data["tx-state"] != "REJECTED" {
		t.Fatalf("status = %v", data)
	}
}

func TestPendingForever(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"remit-single"},"action":"pending_then_success","polls":-1}`)
	e.call("remit-single", remit("ORD-8"), false)
	for i := 0; i < 4; i++ {
		if _, _, data := e.call("remit-status", status("ORD-8"), false); data["tx-state"] != "IN_CLEARING" {
			t.Fatalf("poll %d = %v", i, data)
		}
	}
}

func TestTimeoutStillSubmits(t *testing.T) {
	e := newEnv(t)
	e.fault(`{"match":{"step":"remit-single"},"action":"timeout","delay_ms":5000}`)
	body := `{"meta":{"client-trace-id":"T","sent-at":"2026-11-25T09:40:00+07:00"},"data":` + remit("ORD-9") + `}`
	mac := hmac.New(sha512.New, []byte(secret))
	mac.Write([]byte(body))
	req := httptest.NewRequest("POST", "/api/v2/disbursement/remit-single", strings.NewReader(body))
	req.Header.Set("X-Client-Key", clientKey)
	req.Header.Set("X-Signature-512", base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	ctx, cancel := context.WithTimeout(req.Context(), 50*time.Millisecond)
	defer cancel()
	e.serve(req.WithContext(ctx))
	if code, _, data := e.call("remit-status", status("ORD-9"), false); code != 200 || data["tx-state"] != "IN_CLEARING" {
		t.Fatalf("status = %d %v", code, data)
	}
}

// Known answer from openssl: printf '%s' '{"a":1}' | openssl dgst -sha512 -hmac secretD -binary | base64
func TestSignatureKnownAnswer(t *testing.T) {
	if got := stubd.Signature("secretD", []byte(`{"a":1}`)); got != "KdFY0X/p0SCkgvyiG039E9I2KDqO43A5hzx1/x5ZDqNxpI+57PM1CAZxVlvimLMimlMldi+y6ms6xrrPlotyMA==" {
		t.Fatalf("signature = %s", got)
	}
}
