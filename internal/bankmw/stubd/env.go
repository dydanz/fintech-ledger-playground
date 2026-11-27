package stubd

import (
	"errors"
	"fmt"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// ConfigFromEnv reads S:D's credentials and the poller's timing. Secrets have
// no defaults.
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
	cfg := Config{BaseURL: need("BANK_D_BASE_URL"), ClientKey: need("BANK_D_CLIENT_KEY"), Secret: need("BANK_D_SECRET"),
		FundingAccount: need("BANK_D_FUNDING_ACCOUNT"), PollInterval: dur("BANK_D_POLL_INTERVAL", 5*time.Second),
		PollFor: dur("BANK_D_POLL_FOR", 30*time.Minute)}
	return cfg, errors.Join(errs...)
}

// Build is bank-middleware-d's bankmw build function: S:D settles
// asynchronously, so it needs the Kafka emitter (KAFKA_BROKERS).
func Build(env bankmw.Env, getenv func(string) string) (bankmw.Built, error) {
	if env.Emitter == nil {
		return bankmw.Built{}, errors.New("KAFKA_BROKERS is required: bank D reports its statuses on " + contract.TopicBankEvents)
	}
	cfg, err := ConfigFromEnv(getenv)
	if err != nil {
		return bankmw.Built{}, err
	}
	b, err := New(cfg, env.Client, env.Emitter, env.Log)
	if err != nil {
		return bankmw.Built{}, err
	}
	return bankmw.Built{Bank: b, Stop: b.Close}, nil
}
