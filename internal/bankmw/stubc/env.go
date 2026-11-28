package stubc

import (
	"errors"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// ConfigFromEnv reads S:C's credentials, the webhook address and the
// session timing. Secrets have no defaults.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	var errs []error
	need := func(k string) string {
		v := getenv(k)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
		return v
	}
	dur := func(k string, def time.Duration) time.Duration {
		v := getenv(k)
		if v == "" {
			return def
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			errs = append(errs, fmt.Errorf("%s must be a positive duration", k))
		}
		return d
	}
	cfg := Config{BaseURL: need("BANK_C_BASE_URL"), APIKey: need("BANK_C_API_KEY"), AAASecret: need("BANK_C_AAA_SECRET"),
		SourceVault: need("BANK_C_SOURCE_VAULT_ID"), CallbackBaseURL: need("BANK_C_CALLBACK_BASE_URL"),
		PollInterval: dur("BANK_C_POLL_INTERVAL", 5*time.Second), PollFor: dur("BANK_C_POLL_FOR", 30*time.Minute)}
	return cfg, errors.Join(errs...)
}

// Build is bank-middleware-c's bankmw build function: S:C settles
// asynchronously, so it needs the Kafka emitter (KAFKA_BROKERS), and it
// mounts the webhook routes.
func Build(env bankmw.Env, getenv func(string) string) (bankmw.Built, error) {
	if env.Emitter == nil {
		return bankmw.Built{}, errors.New("KAFKA_BROKERS is required: bank C reports its statuses on " + contract.TopicBankEvents)
	}
	cfg, err := ConfigFromEnv(getenv)
	if err != nil {
		return bankmw.Built{}, err
	}
	b, err := New(cfg, env.Client, env.Emitter, env.Log)
	if err != nil {
		return bankmw.Built{}, err
	}
	return bankmw.Built{Bank: b, Routes: b.Routes, Stop: b.Close}, nil
}
