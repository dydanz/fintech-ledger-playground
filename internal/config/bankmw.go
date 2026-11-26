package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"
)

// BankMiddlewareConfig is what every bank middleware shares (spec/money-out
// TRD §3.5). Each bank's own credentials are read by its package.
type BankMiddlewareConfig struct {
	ServiceName     string        // BMW_NAME, e.g. bank-middleware-a
	HTTPAddr        string        // BMW_HTTP_ADDR
	PublicKeys      string        // MOM_PUBLIC_KEYS or the contents of MOM_PUBLIC_KEYS_FILE: "kid=base64,..."
	SignatureSkew   time.Duration // MOM_SIGNATURE_SKEW
	NonceCacheSize  int           // BMW_NONCE_CACHE_SIZE (a soft cap)
	BankTimeout     time.Duration // BMW_BANK_TIMEOUT: one call to the bank
	KafkaBrokers    []string      // KAFKA_BROKERS: only async banks publish; empty means no emitter
	LogLevel        slog.Level
	TrustedProxies  []string
	ShutdownTimeout time.Duration
}

func LoadBankMiddleware(defaultName string) (BankMiddlewareConfig, error) {
	var errs []error
	cfg := BankMiddlewareConfig{
		ServiceName:    getEnv("BMW_NAME", defaultName),
		HTTPAddr:       getEnv("BMW_HTTP_ADDR", ":8086"),
		PublicKeys:     strings.TrimSpace(os.Getenv("MOM_PUBLIC_KEYS")),
		KafkaBrokers:   splitList(os.Getenv("KAFKA_BROKERS")),
		TrustedProxies: splitList(os.Getenv("TRUSTED_PROXIES")),
	}
	if f := os.Getenv("MOM_PUBLIC_KEYS_FILE"); cfg.PublicKeys == "" && f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			errs = append(errs, fmt.Errorf("MOM_PUBLIC_KEYS_FILE: %w", err))
		}
		cfg.PublicKeys = strings.TrimSpace(string(b))
	}
	if cfg.PublicKeys == "" {
		errs = append(errs, errors.New("MOM_PUBLIC_KEYS (or MOM_PUBLIC_KEYS_FILE) is required"))
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	var err error
	if cfg.NonceCacheSize, err = getInt("BMW_NONCE_CACHE_SIZE", 100000); err != nil {
		errs = append(errs, err)
	} else if cfg.NonceCacheSize < 1 {
		errs = append(errs, errors.New("BMW_NONCE_CACHE_SIZE must be positive"))
	}
	for _, d := range []struct {
		dst *time.Duration
		key string
		def time.Duration
	}{
		{&cfg.SignatureSkew, "MOM_SIGNATURE_SKEW", 5 * time.Minute},
		{&cfg.BankTimeout, "BMW_BANK_TIMEOUT", 20 * time.Second},
		{&cfg.ShutdownTimeout, "SHUTDOWN_TIMEOUT", 15 * time.Second},
	} {
		if *d.dst, err = getDuration(d.key, d.def); err != nil {
			errs = append(errs, err)
		} else if *d.dst <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", d.key))
		}
	}
	return cfg, errors.Join(errs...)
}
