// Package config loads runtime configuration from environment variables.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type Config struct {
	ServiceName     string
	HTTPAddr        string
	DatabaseURL     string
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	BcryptCost      int
	LogLevel        slog.Level
	TrustedProxies  []string
	ShutdownTimeout time.Duration
}

func Load() (Config, error) {
	var errs []error

	cfg := Config{
		ServiceName:     getEnv("SERVICE_NAME", "auth-api"),
		HTTPAddr:        getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:     os.Getenv("DATABASE_URL"),
		RedisAddr:       getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:   os.Getenv("REDIS_PASSWORD"),
		TrustedProxies:  splitList(os.Getenv("TRUSTED_PROXIES")),
		ShutdownTimeout: 15 * time.Second,
	}

	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}

	var err error
	if cfg.RedisDB, err = getInt("REDIS_DB", 0); err != nil {
		errs = append(errs, err)
	}
	if cfg.BcryptCost, err = getInt("BCRYPT_COST", 12); err != nil {
		errs = append(errs, err)
	} else if cfg.BcryptCost < bcrypt.MinCost || cfg.BcryptCost > bcrypt.MaxCost {
		errs = append(errs, fmt.Errorf("BCRYPT_COST must be between %d and %d", bcrypt.MinCost, bcrypt.MaxCost))
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	if v := os.Getenv("SHUTDOWN_TIMEOUT"); v != "" {
		if cfg.ShutdownTimeout, err = time.ParseDuration(v); err != nil {
			errs = append(errs, fmt.Errorf("SHUTDOWN_TIMEOUT: %w", err))
		}
	}

	return cfg, errors.Join(errs...)
}

func getEnv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func getInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func splitList(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ClientOperations are the operations an internal caller can be granted in
// LEDGER_CLIENTS (TRD §6.1).
var ClientOperations = []string{"account_open", "credit", "debit", "void", "freeze", "unfreeze", "read"}

// ClientCredential is one internal caller of ledger-api: its id, the hex
// SHA-256 of its bearer token, and the operations it may perform.
type ClientCredential struct {
	ID         string
	TokenHash  string
	Operations []string
}

// LedgerConfig is the configuration of cmd/ledger-api.
type LedgerConfig struct {
	ServiceName      string
	HTTPAddr         string
	DatabaseURL      string
	RedisAddr        string // auth-api's session store, for the user routes
	RedisPassword    string
	RedisDB          int
	LogLevel         slog.Level
	TrustedProxies   []string
	ShutdownTimeout  time.Duration
	Clients          []ClientCredential
	OverdraftClients []string
	Currencies       []string
	MaxAmount        int64
	ProcessTimeout   time.Duration
	IdempotencyTTL   time.Duration
	EventsTopic      string
}

// LoadLedger reads the ledger-api environment (TRD §12.2).
func LoadLedger() (LedgerConfig, error) {
	var errs []error
	cfg := LedgerConfig{
		ServiceName:      getEnv("SERVICE_NAME", "ledger-api"),
		HTTPAddr:         getEnv("LEDGER_HTTP_ADDR", ":8081"),
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		RedisAddr:        getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:    os.Getenv("REDIS_PASSWORD"),
		TrustedProxies:   splitList(os.Getenv("TRUSTED_PROXIES")),
		OverdraftClients: splitList(os.Getenv("LEDGER_OVERDRAFT_CLIENTS")),
		Currencies:       splitList(getEnv("LEDGER_CURRENCIES", "IDR")),
		EventsTopic:      getEnv("KAFKA_TOPIC_EVENTS", "ledger.events.v1"),
	}
	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	var err error
	if cfg.RedisDB, err = getInt("REDIS_DB", 0); err != nil {
		errs = append(errs, err)
	}
	if cfg.ShutdownTimeout, err = getDuration("SHUTDOWN_TIMEOUT", 15*time.Second); err != nil {
		errs = append(errs, err)
	}
	if cfg.ProcessTimeout, err = getDuration("LEDGER_PROCESS_TIMEOUT", 2*time.Second); err != nil {
		errs = append(errs, err)
	}
	if cfg.IdempotencyTTL, err = getDuration("IDEMPOTENCY_TTL", 24*time.Hour); err != nil {
		errs = append(errs, err)
	}
	if v := getEnv("LEDGER_MAX_AMOUNT", "1000000000000"); v != "" {
		if cfg.MaxAmount, err = strconv.ParseInt(v, 10, 64); err != nil {
			errs = append(errs, fmt.Errorf("LEDGER_MAX_AMOUNT: %w", err))
		}
	}
	if cfg.Clients, err = ParseClients(os.Getenv("LEDGER_CLIENTS")); err != nil {
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

// ParseClients parses LEDGER_CLIENTS: "id:sha256hex:op,op;id2:...". It
// rejects unknown operations, malformed hashes, and duplicate ids or hashes.
// The error never contains the hash values.
func ParseClients(v string) ([]ClientCredential, error) {
	if strings.TrimSpace(v) == "" {
		return nil, errors.New("LEDGER_CLIENTS is required")
	}
	var out []ClientCredential
	ids, hashes := map[string]bool{}, map[string]bool{}
	for i, entry := range strings.Split(v, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 3 {
			return nil, fmt.Errorf("LEDGER_CLIENTS entry %d: want id:sha256hex:ops", i+1)
		}
		c := ClientCredential{ID: strings.TrimSpace(parts[0]), TokenHash: strings.ToLower(strings.TrimSpace(parts[1])), Operations: splitList(parts[2])}
		switch {
		case !clientIDPattern.MatchString(c.ID):
			return nil, fmt.Errorf("LEDGER_CLIENTS entry %d: client id must match %s", i+1, clientIDPattern)
		case !sha256Pattern.MatchString(c.TokenHash):
			return nil, fmt.Errorf("LEDGER_CLIENTS entry %d (%s): token hash must be 64 hex characters", i+1, c.ID)
		case ids[c.ID]:
			return nil, fmt.Errorf("LEDGER_CLIENTS: duplicate client id %s", c.ID)
		case hashes[c.TokenHash]:
			return nil, fmt.Errorf("LEDGER_CLIENTS entry %d (%s): token hash already used by another client", i+1, c.ID)
		case len(c.Operations) == 0:
			return nil, fmt.Errorf("LEDGER_CLIENTS entry %d (%s): no operations", i+1, c.ID)
		}
		for _, op := range c.Operations {
			if !slices.Contains(ClientOperations, op) {
				return nil, fmt.Errorf("LEDGER_CLIENTS entry %d (%s): unknown operation %q", i+1, c.ID, op)
			}
		}
		ids[c.ID], hashes[c.TokenHash] = true, true
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errors.New("LEDGER_CLIENTS is required")
	}
	return out, nil
}

var (
	clientIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	sha256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

func getDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
