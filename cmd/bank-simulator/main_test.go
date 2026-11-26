package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fintech-ledger-playground/internal/moneyout/signing"
)

func TestKeygenWritesOnceAndKeepsExistingKeys(t *testing.T) {
	dir := t.TempDir()
	if err := keygen(dir); err != nil {
		t.Fatal(err)
	}
	priv, err := os.ReadFile(filepath.Join(dir, "bank-a-partner.key"))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := pem.Decode(priv)
	k, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil || k.(*rsa.PrivateKey).N.BitLen() != 2048 {
		t.Fatalf("private key: %v", err)
	}
	pub, err := loadRSAPublic(filepath.Join(dir, "bank-a-partner.pub"))
	if err != nil || !pub.Equal(&k.(*rsa.PrivateKey).PublicKey) {
		t.Fatalf("public key doesn't match: %v", err)
	}
	seed, _ := os.ReadFile(filepath.Join(dir, "mom-signing.key"))
	momPriv, err := signing.ParsePrivateKey(string(seed))
	if err != nil {
		t.Fatal(err)
	}
	pubs, _ := os.ReadFile(filepath.Join(dir, "mom-public.keys"))
	keys, err := signing.ParsePublicKeys(strings.TrimSpace(string(pubs)))
	if err != nil || !keys["mom-1"].Equal(momPriv.Public()) {
		t.Fatalf("MOM public keys %q don't match the signing key: %v", pubs, err)
	}
	if err := keygen(dir); err != nil {
		t.Fatal(err)
	}
	again, _ := os.ReadFile(filepath.Join(dir, "bank-a-partner.key"))
	seedAgain, _ := os.ReadFile(filepath.Join(dir, "mom-signing.key"))
	if string(again) != string(priv) || string(seedAgain) != string(seed) {
		t.Fatal("keygen replaced an existing key")
	}
}

func TestBuildNeedsItsSecrets(t *testing.T) {
	for _, bank := range []string{"a", "b", "c", "d"} {
		t.Setenv("SIM_BANK", bank)
		if _, err := build(nil); err == nil {
			t.Errorf("bank %s built without its secrets", bank)
		}
	}
	t.Setenv("SIM_BANK", "e")
	if _, err := build(nil); err == nil {
		t.Error("unknown bank built")
	}
}

func TestBuildEachBank(t *testing.T) {
	dir := t.TempDir()
	if err := keygen(dir); err != nil {
		t.Fatal(err)
	}
	env := map[string]map[string]string{
		"a": {"SIM_A_CLIENT_KEY": "k", "SIM_A_CLIENT_SECRET": "s", "SIM_A_PARTNER_ID": "p", "SIM_A_PARTNER_PUBLIC_KEY_FILE": filepath.Join(dir, "bank-a-partner.pub")},
		"b": {"SIM_B_CLIENT_ID": "i", "SIM_B_CLIENT_SECRET": "s", "SIM_B_API_KEY": "k", "SIM_B_API_SECRET": "s", "SIM_B_CORPORATE_ID": "c"},
		"c": {"SIM_C_API_KEY": "k", "SIM_C_AAA_SECRET": "s"},
		"d": {"SIM_D_CLIENT_KEY": "k", "SIM_D_SECRET": "s", "SIM_D_FUNDING_ACCT": "f"},
	}
	for bank, vars := range env {
		t.Run(bank, func(t *testing.T) {
			t.Setenv("SIM_BANK", bank)
			for k, v := range vars {
				t.Setenv(k, v)
			}
			h, err := build(nil)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/_sim/calls", nil))
			if w.Code != 200 {
				t.Fatalf("control route: %d", w.Code)
			}
		})
	}
}
