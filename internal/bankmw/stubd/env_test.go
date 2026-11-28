package stubd_test

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/bankmw/stubd"
)

func TestConfigFromEnv(t *testing.T) {
	env := map[string]string{"BANK_D_BASE_URL": "http://bank-sim-d:8090", "BANK_D_CLIENT_KEY": clientKey, "BANK_D_SECRET": secret,
		"BANK_D_FUNDING_ACCOUNT": funding}
	get := func(k string) string { return env[k] }
	cfg, err := stubd.ConfigFromEnv(get)
	if err != nil || cfg.Secret != secret || cfg.FundingAccount != funding || cfg.PollInterval != 5*time.Second || cfg.PollFor != 30*time.Minute {
		t.Fatalf("cfg = %+v, %v", cfg, err)
	}
	env["BANK_D_POLL_INTERVAL"], env["BANK_D_POLL_FOR"] = "250ms", "2m"
	if cfg, err = stubd.ConfigFromEnv(get); err != nil || cfg.PollInterval != 250*time.Millisecond || cfg.PollFor != 2*time.Minute {
		t.Fatalf("overrides = %+v, %v", cfg, err)
	}
	env["BANK_D_POLL_INTERVAL"] = "soon"
	if _, err := stubd.ConfigFromEnv(get); err == nil || !strings.Contains(err.Error(), "BANK_D_POLL_INTERVAL") {
		t.Fatalf("err = %v", err)
	}
	delete(env, "BANK_D_POLL_INTERVAL")
	delete(env, "BANK_D_SECRET")
	if _, err := stubd.ConfigFromEnv(get); err == nil || !strings.Contains(err.Error(), "BANK_D_SECRET") {
		t.Fatalf("err = %v", err)
	}
}

// Without KAFKA_BROKERS there is no emitter, and bank D's statuses could
// never reach MOM: refuse to start. (A nil *Emitter in the Reporter
// interface would not be nil.)
func TestBuildNeedsKafka(t *testing.T) {
	env := map[string]string{"BANK_D_BASE_URL": "http://bank-sim-d:8090", "BANK_D_CLIENT_KEY": clientKey, "BANK_D_SECRET": secret,
		"BANK_D_FUNDING_ACCOUNT": funding}
	get := func(k string) string { return env[k] }
	e := bankmw.Env{Log: slog.Default(), Client: http.DefaultClient}
	if _, err := stubd.Build(e, get); err == nil || !strings.Contains(err.Error(), "KAFKA_BROKERS") {
		t.Fatalf("err = %v, want KAFKA_BROKERS required", err)
	}
	e.Emitter = bankmw.NewEmitter("bank-middleware-d", nil, time.Now)
	built, err := stubd.Build(e, get)
	if err != nil || built.Bank == nil || built.Stop == nil {
		t.Fatalf("built = %+v, %v", built, err)
	}
	built.Stop()
	delete(env, "BANK_D_SECRET")
	if _, err := stubd.Build(e, get); err == nil {
		t.Fatal("built without a secret")
	}
}
