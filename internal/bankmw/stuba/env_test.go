package stuba_test

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fintech-ledger-playground/internal/bankmw/stuba"
)

func TestConfigFromEnv(t *testing.T) {
	der, _ := x509.MarshalPKCS8PrivateKey(key)
	f := filepath.Join(t.TempDir(), "a.key")
	if err := os.WriteFile(f, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"BANK_A_BASE_URL": "http://bank-sim-a:8090", "BANK_A_CLIENT_KEY": clientKey, "BANK_A_PRIVATE_KEY_FILE": f,
		"BANK_A_CLIENT_SECRET": secret, "BANK_A_PARTNER_ID": partner, "BANK_A_SOURCE_ACCOUNT": "000101002345501"}
	cfg, err := stuba.ConfigFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ChannelID != "95221" || cfg.DeviceID != "bank-middleware-a" || !cfg.PrivateKey.Equal(key) || cfg.BaseURL != "http://bank-sim-a:8090" {
		t.Fatalf("cfg = %+v", cfg)
	}
	delete(env, "BANK_A_CLIENT_SECRET")
	env["BANK_A_PRIVATE_KEY_FILE"] = filepath.Join(t.TempDir(), "missing")
	_, err = stuba.ConfigFromEnv(func(k string) string { return env[k] })
	if err == nil || !strings.Contains(err.Error(), "BANK_A_CLIENT_SECRET") || !strings.Contains(err.Error(), "BANK_A_PRIVATE_KEY_FILE") {
		t.Fatalf("err = %v", err)
	}
}
