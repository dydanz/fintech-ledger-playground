package bankmw_test

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/signing"
)

func cfg(keys string) config.BankMiddlewareConfig {
	return config.BankMiddlewareConfig{ServiceName: "bank-middleware-a", PublicKeys: keys, SignatureSkew: 5 * time.Minute,
		NonceCacheSize: 10, BankTimeout: time.Second}
}

func TestSetupServesSignedRoutes(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	var gotEnv bankmw.Env
	h, cleanup, err := bankmw.Setup(cfg("mom-1="+signing.EncodeKey(pub)), logger.New(&bytes.Buffer{}, slog.LevelDebug, "b"),
		func(env bankmw.Env) (bankmw.Built, error) {
			gotEnv = env
			return bankmw.Built{Bank: &fakeBank{result: contract.BankResult{Status: contract.StatusSucceeded}}}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if gotEnv.Client == nil || gotEnv.Client.Timeout != time.Second || gotEnv.Emitter != nil {
		t.Fatalf("env = %+v: want the bank timeout, and no emitter without KAFKA_BROKERS", gotEnv)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, bankmw.PathStatus, strings.NewReader(`{}`)))
	if w.Code != 401 {
		t.Fatalf("unsigned: %d", w.Code)
	}
	body := []byte(`{"transfer_id":"01a1d000-0000-7000-8000-000000000001","request_id":"01a1d000-0000-7000-8000-0000000000aa"}`)
	hdr, _ := signing.NewSigner("mom-1", priv).Sign(http.MethodPost, bankmw.PathStatus, body, time.Now())
	req := httptest.NewRequest(http.MethodPost, bankmw.PathStatus, bytes.NewReader(body))
	for k := range hdr {
		req.Header.Set(k, hdr.Get(k))
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "SUCCEEDED") {
		t.Fatalf("signed: %d %s", w.Code, w.Body)
	}
}

func TestSetupRefusesBadKeysOrBank(t *testing.T) {
	ok := func(bankmw.Env) (bankmw.Built, error) { return bankmw.Built{Bank: &fakeBank{}}, nil }
	if _, _, err := bankmw.Setup(cfg("not-keys"), slog.Default(), ok); err == nil {
		t.Fatal("bad MOM_PUBLIC_KEYS accepted")
	}
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	if _, _, err := bankmw.Setup(cfg("mom-1="+signing.EncodeKey(pub)), slog.Default(),
		func(bankmw.Env) (bankmw.Built, error) { return bankmw.Built{}, errors.New("no creds") }); err == nil {
		t.Fatal("bank build error ignored")
	}
}

// Review Focus 5: past the soft cap the cache grows and the BMW logs it once.
func TestSetupLogsNonceCacheOverCap(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	var logs bytes.Buffer
	c := cfg("mom-1=" + signing.EncodeKey(pub))
	c.NonceCacheSize = 1
	h, cleanup, err := bankmw.Setup(c, logger.New(&logs, slog.LevelDebug, "b"),
		func(bankmw.Env) (bankmw.Built, error) {
			return bankmw.Built{Bank: &fakeBank{result: contract.BankResult{Status: contract.StatusSucceeded}}}, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	body := []byte(`{"transfer_id":"01a1d000-0000-7000-8000-000000000001","request_id":"01a1d000-0000-7000-8000-0000000000aa"}`)
	for i := 0; i < 3; i++ {
		hdr, _ := signing.NewSigner("mom-1", priv).Sign(http.MethodPost, bankmw.PathStatus, body, time.Now())
		req := httptest.NewRequest(http.MethodPost, bankmw.PathStatus, bytes.NewReader(body)).WithContext(context.Background())
		for k := range hdr {
			req.Header.Set(k, hdr.Get(k))
		}
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
	if n := strings.Count(logs.String(), `"event":"bank.nonce_cache"`); n != 1 {
		t.Fatalf("bank.nonce_cache logged %d times, want 1: %s", n, logs.String())
	}
}
