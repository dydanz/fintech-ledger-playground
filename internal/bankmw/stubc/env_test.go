package stubc_test

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/bankmw/stubc"
)

func baseEnv() map[string]string {
	return map[string]string{"BANK_C_BASE_URL": "http://bank-sim-c:8090", "BANK_C_API_KEY": apiKey, "BANK_C_AAA_SECRET": aaaSecret,
		"BANK_C_SOURCE_VAULT_ID": vault, "BANK_C_CALLBACK_BASE_URL": "http://bank-middleware-c:8086/callbacks/bank-c"}
}

func TestConfigFromEnv(t *testing.T) {
	env := baseEnv()
	get := func(k string) string { return env[k] }
	cfg, err := stubc.ConfigFromEnv(get)
	if err != nil || cfg.AAASecret != aaaSecret || cfg.SourceVault != vault || cfg.PollInterval != 5*time.Second || cfg.PollFor != 30*time.Minute {
		t.Fatalf("cfg = %+v, %v", cfg, err)
	}
	env["BANK_C_POLL_FOR"] = "never"
	if _, err := stubc.ConfigFromEnv(get); err == nil || !strings.Contains(err.Error(), "BANK_C_POLL_FOR") {
		t.Fatalf("err = %v", err)
	}
	delete(env, "BANK_C_POLL_FOR")
	delete(env, "BANK_C_CALLBACK_BASE_URL")
	if _, err := stubc.ConfigFromEnv(get); err == nil || !strings.Contains(err.Error(), "BANK_C_CALLBACK_BASE_URL") {
		t.Fatalf("err = %v", err)
	}
}

// Without KAFKA_BROKERS bank C's statuses could never reach MOM.
func TestBuildNeedsKafkaAndMountsWebhooks(t *testing.T) {
	env := baseEnv()
	get := func(k string) string { return env[k] }
	e := bankmw.Env{Log: slog.Default(), Client: http.DefaultClient}
	if _, err := stubc.Build(e, get); err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("err = %v", err)
	}
	e.Emitter = bankmw.NewEmitter("bank-middleware-c", nil, time.Now)
	built, err := stubc.Build(e, get)
	if err != nil || built.Bank == nil || built.Stop == nil || built.Routes == nil {
		t.Fatalf("built = %+v, %v", built, err)
	}
	built.Stop()
}
