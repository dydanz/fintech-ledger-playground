package stubb_test

import (
	"strings"
	"testing"

	"fintech-ledger-playground/internal/bankmw/stubb"
)

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{"BANK_B_BASE_URL": "http://bank-sim-b:8090", "BANK_B_CLIENT_ID": clientID, "BANK_B_CLIENT_SECRET": clientSecret,
		"BANK_B_API_KEY": apiKey, "BANK_B_API_SECRET": apiSecret, "BANK_B_CORPORATE_ID": corp, "BANK_B_SOURCE_ACCOUNT": "0201245680"}
	cfg, err := stubb.ConfigFromEnv(func(k string) string { return env[k] })
	if err != nil || cfg.CorporateID != corp || cfg.APISecret != apiSecret {
		t.Fatalf("cfg = %+v, %v", cfg, err)
	}
	delete(env, "BANK_B_API_SECRET")
	if _, err := stubb.ConfigFromEnv(func(k string) string { return env[k] }); err == nil || !strings.Contains(err.Error(), "BANK_B_API_SECRET") {
		t.Fatalf("err = %v", err)
	}
}
