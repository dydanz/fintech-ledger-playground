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
	UnfreezeClients  []string // the only clients LEDGER_CLIENTS may grant unfreeze (TRD §16 Q6)
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
		UnfreezeClients:  splitList(getEnv("LEDGER_UNFREEZE_CLIENTS", "ops")),
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
	} else if err := CheckUnfreezeGrants(cfg.Clients, cfg.UnfreezeClients); err != nil {
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

// WorkerConfig is the configuration of cmd/ledger-worker.
type WorkerConfig struct {
	ServiceName     string
	HTTPAddr        string // /healthz and /readyz only
	DatabaseURL     string
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	Currencies      []string
	MaxAmount       int64
	ProcessTimeout  time.Duration
	IdempotencyTTL  time.Duration
	EventsTopic     string
	SweepInterval   time.Duration // LEDGER_SWEEP_INTERVAL
	SweepAfter      time.Duration // LEDGER_SWEEP_AFTER
	PendingMaxAge   time.Duration // LEDGER_PENDING_MAX_AGE
	PurgeInterval   time.Duration // IDEMPOTENCY_PURGE_INTERVAL
	KafkaBrokers    []string      // KAFKA_BROKERS
	OutboxPoll      time.Duration // OUTBOX_POLL_INTERVAL
	OutboxBatch     int           // OUTBOX_BATCH_SIZE
	OutboxRetention time.Duration // OUTBOX_RETENTION
	OutboxPublish   time.Duration // OUTBOX_PUBLISH_TIMEOUT: bound on one batch
	ReconcileEvery  time.Duration // RECONCILE_INTERVAL
}

// LoadWorker reads the ledger-worker environment (TRD §12.2).
func LoadWorker() (WorkerConfig, error) {
	var errs []error
	cfg := WorkerConfig{
		ServiceName:  getEnv("SERVICE_NAME", "ledger-worker"),
		HTTPAddr:     getEnv("WORKER_HTTP_ADDR", ":8082"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		Currencies:   splitList(getEnv("LEDGER_CURRENCIES", "IDR")),
		EventsTopic:  getEnv("KAFKA_TOPIC_EVENTS", "ledger.events.v1"),
		KafkaBrokers: splitList(getEnv("KAFKA_BROKERS", "localhost:9094")),
	}
	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	var err error
	if cfg.OutboxBatch, err = getInt("OUTBOX_BATCH_SIZE", 500); err != nil {
		errs = append(errs, err)
	} else if cfg.OutboxBatch < 1 || cfg.OutboxBatch > 10000 {
		errs = append(errs, errors.New("OUTBOX_BATCH_SIZE must be between 1 and 10000"))
	}
	for _, d := range []struct {
		dst *time.Duration
		key string
		def time.Duration
	}{
		{&cfg.ShutdownTimeout, "SHUTDOWN_TIMEOUT", 15 * time.Second},
		{&cfg.ProcessTimeout, "LEDGER_PROCESS_TIMEOUT", 2 * time.Second},
		{&cfg.IdempotencyTTL, "IDEMPOTENCY_TTL", 24 * time.Hour},
		{&cfg.SweepInterval, "LEDGER_SWEEP_INTERVAL", 5 * time.Second},
		{&cfg.SweepAfter, "LEDGER_SWEEP_AFTER", 5 * time.Second},
		{&cfg.PendingMaxAge, "LEDGER_PENDING_MAX_AGE", 15 * time.Minute},
		{&cfg.PurgeInterval, "IDEMPOTENCY_PURGE_INTERVAL", time.Minute},
		{&cfg.OutboxPoll, "OUTBOX_POLL_INTERVAL", 200 * time.Millisecond},
		{&cfg.OutboxRetention, "OUTBOX_RETENTION", 72 * time.Hour},
		{&cfg.OutboxPublish, "OUTBOX_PUBLISH_TIMEOUT", 10 * time.Second},
		{&cfg.ReconcileEvery, "RECONCILE_INTERVAL", time.Minute},
	} {
		if *d.dst, err = getDuration(d.key, d.def); err != nil {
			errs = append(errs, err)
		}
	}
	if cfg.SweepInterval <= 0 || cfg.PurgeInterval <= 0 || cfg.OutboxPoll <= 0 || cfg.OutboxRetention <= 0 || cfg.OutboxPublish <= 0 || cfg.ReconcileEvery <= 0 {
		errs = append(errs, errors.New("LEDGER_SWEEP_INTERVAL, IDEMPOTENCY_PURGE_INTERVAL, OUTBOX_POLL_INTERVAL, OUTBOX_RETENTION, OUTBOX_PUBLISH_TIMEOUT and RECONCILE_INTERVAL must be positive"))
	}
	if len(cfg.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	// A sweep must not race the request path's own Tx2.
	if cfg.SweepAfter < cfg.ProcessTimeout {
		errs = append(errs, errors.New("LEDGER_SWEEP_AFTER must be at least LEDGER_PROCESS_TIMEOUT"))
	}
	if v := getEnv("LEDGER_MAX_AMOUNT", "1000000000000"); v != "" {
		if cfg.MaxAmount, err = strconv.ParseInt(v, 10, 64); err != nil {
			errs = append(errs, fmt.Errorf("LEDGER_MAX_AMOUNT: %w", err))
		}
	}
	return cfg, errors.Join(errs...)
}

// FraudConfig is the configuration of cmd/fraud-engine (TRD §12.2).
type FraudConfig struct {
	ServiceName     string
	HTTPAddr        string // /healthz and /readyz only
	LogLevel        slog.Level
	ShutdownTimeout time.Duration
	KafkaBrokers    []string
	EventsTopic     string
	Group           string
	RedisAddr       string
	RedisPassword   string
	RedisDB         int
	LedgerAPIURL    string
	ClientToken     string // FRAUD_CLIENT_TOKEN: this engine's ledger-api service token; never logged
	FreezeRetry     time.Duration
	TopupMax        int
	TopupWindow     time.Duration
	DebitWindow     time.Duration
	DebitMaxCount   int
	DebitLimits     map[string]int64
}

// LoadFraud reads the fraud-engine environment. The thresholds are TRD §16
// Q7; their production values are tracked in the P4 issue.
func LoadFraud() (FraudConfig, error) {
	var errs []error
	cfg := FraudConfig{
		ServiceName:   getEnv("SERVICE_NAME", "fraud-engine"),
		HTTPAddr:      getEnv("FRAUD_HTTP_ADDR", ":8083"),
		KafkaBrokers:  splitList(getEnv("KAFKA_BROKERS", "localhost:9094")),
		EventsTopic:   getEnv("KAFKA_TOPIC_EVENTS", "ledger.events.v1"),
		Group:         getEnv("FRAUD_CONSUMER_GROUP", "fraud-engine"),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		LedgerAPIURL:  getEnv("LEDGER_API_URL", "http://localhost:8081"),
		ClientToken:   os.Getenv("FRAUD_CLIENT_TOKEN"),
	}
	if err := cfg.LogLevel.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info"))); err != nil {
		errs = append(errs, fmt.Errorf("LOG_LEVEL: %w", err))
	}
	if cfg.ClientToken == "" {
		errs = append(errs, errors.New("FRAUD_CLIENT_TOKEN is required"))
	}
	if len(cfg.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS is required"))
	}
	var err error
	for _, n := range []struct {
		dst      *int
		key      string
		def, low int
	}{
		{&cfg.RedisDB, "REDIS_DB", 0, 0},
		{&cfg.TopupMax, "FRAUD_TOPUP_MAX", 3, 1},
		{&cfg.DebitMaxCount, "FRAUD_DEBIT_MAX_COUNT", 30, 1},
	} {
		if *n.dst, err = getInt(n.key, n.def); err != nil {
			errs = append(errs, err)
		} else if *n.dst < n.low {
			errs = append(errs, fmt.Errorf("%s must be at least %d", n.key, n.low))
		}
	}
	for _, d := range []struct {
		dst *time.Duration
		key string
		def time.Duration
	}{
		{&cfg.ShutdownTimeout, "SHUTDOWN_TIMEOUT", 15 * time.Second},
		{&cfg.FreezeRetry, "FRAUD_FREEZE_RETRY", 30 * time.Second},
		{&cfg.TopupWindow, "FRAUD_TOPUP_WINDOW", 5 * time.Minute},
		{&cfg.DebitWindow, "FRAUD_DEBIT_WINDOW", time.Hour},
	} {
		if *d.dst, err = getDuration(d.key, d.def); err != nil {
			errs = append(errs, err)
		} else if *d.dst <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", d.key))
		}
	}
	if cfg.DebitLimits, err = ParseCurrencyLimits(getEnv("FRAUD_DEBIT_LIMITS", "IDR:50000000")); err != nil {
		errs = append(errs, err)
	}
	return cfg, errors.Join(errs...)
}

// ParseCurrencyLimits parses FRAUD_DEBIT_LIMITS: "IDR:50000000,USD:5000",
// amounts in minor units. A currency may appear once; empty means no limits.
func ParseCurrencyLimits(v string) (map[string]int64, error) {
	out := map[string]int64{}
	for _, entry := range splitList(v) {
		cur, amount, ok := strings.Cut(entry, ":")
		cur = strings.TrimSpace(cur)
		n, err := strconv.ParseInt(strings.TrimSpace(amount), 10, 64)
		switch {
		case !ok || !currencyPattern.MatchString(cur):
			return nil, fmt.Errorf("FRAUD_DEBIT_LIMITS: %q must be CUR:amount with an ISO 4217 code", entry)
		case err != nil || n <= 0:
			return nil, fmt.Errorf("FRAUD_DEBIT_LIMITS: %s limit must be a positive integer (minor units)", cur)
		case out[cur] != 0:
			return nil, fmt.Errorf("FRAUD_DEBIT_LIMITS: %s appears twice", cur)
		}
		out[cur] = n
	}
	return out, nil
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

// CheckUnfreezeGrants rejects a LEDGER_CLIENTS that grants unfreeze to a
// client outside allowed, so lifting a freeze stays with operations staff.
func CheckUnfreezeGrants(clients []ClientCredential, allowed []string) error {
	for _, c := range clients {
		if slices.Contains(c.Operations, "unfreeze") && !slices.Contains(allowed, c.ID) {
			return fmt.Errorf("LEDGER_CLIENTS: %s may not be granted unfreeze; LEDGER_UNFREEZE_CLIENTS allows %v", c.ID, allowed)
		}
	}
	return nil
}

var (
	clientIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	sha256Pattern   = regexp.MustCompile(`^[0-9a-f]{64}$`)
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
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
