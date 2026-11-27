// Command bank-simulator is a test-only bank (spec/money-out TRD §4, plan
// M2.1): SIM_BANK=a|b|c|d picks which of the four contracts it serves, with
// fault injection on /_sim. Never deploy it next to real money.
//
//	bank-simulator               serve
//	bank-simulator keygen DIR    create the dev keys in DIR, if missing (S:A's RSA pair, MOM's Ed25519 pair)
//	bank-simulator healthcheck   the container healthcheck
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/banksim"
	"fintech-ledger-playground/internal/banksim/stuba"
	"fintech-ledger-playground/internal/banksim/stubb"
	"fintech-ledger-playground/internal/banksim/stubc"
	"fintech-ledger-playground/internal/banksim/stubd"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/signing"
	"fintech-ledger-playground/internal/startup"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "healthcheck":
			os.Exit(startup.Healthcheck("SIM_HTTP_ADDR", ":8090"))
		case "keygen":
			if len(os.Args) != 3 {
				fmt.Fprintln(os.Stderr, "usage: bank-simulator keygen DIR")
				os.Exit(2)
			}
			if err := keygen(os.Args[2]); err != nil {
				fmt.Fprintln(os.Stderr, "keygen:", err)
				os.Exit(1)
			}
			return
		}
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(getEnv("LOG_LEVEL", "info")))
	log := logger.New(os.Stdout, level, getEnv("SERVICE_NAME", "bank-simulator-"+os.Getenv("SIM_BANK")))
	slog.SetDefault(log)
	gin.SetMode(gin.ReleaseMode)
	h, err := build(log)
	if err != nil {
		log.Error("invalid configuration", "event", "app.lifecycle", "outcome", "failure", "reason", "config_invalid", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Addr: getEnv("SIM_HTTP_ADDR", ":8090"), Handler: h, ReadHeaderTimeout: 5 * time.Second,
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn)}
	if err := startup.Serve(ctx, log, srv, 10*time.Second); err != nil {
		log.Error("bank-simulator exited with error", "event", "app.lifecycle", "outcome", "failure", "reason", "serve_failed", "error", err)
		os.Exit(1)
	}
}

// build returns the simulator for SIM_BANK. Every secret comes from the
// environment; none has a default.
func build(log *slog.Logger) (http.Handler, error) {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	var e envErrs
	bank := os.Getenv("SIM_BANK")
	steps := map[string]banksim.Steps{"a": stuba.Steps, "b": stubb.Steps, "c": stubc.Steps, "d": stubd.Steps}[bank]
	if steps.All == nil {
		return nil, fmt.Errorf("SIM_BANK must be a, b, c or d, got %q", bank)
	}
	sim := banksim.New(steps)
	r, err := banksim.NewEngine(log, sim)
	if err != nil {
		return nil, err
	}
	switch bank {
	case "a":
		cfg := stuba.Config{ClientKey: e.need("SIM_A_CLIENT_KEY"), ClientSecret: e.need("SIM_A_CLIENT_SECRET"),
			PartnerID: e.need("SIM_A_PARTNER_ID"), TokenTTL: e.duration("SIM_A_TOKEN_TTL", 15*time.Minute)}
		if f := e.need("SIM_A_PARTNER_PUBLIC_KEY_FILE"); f != "" {
			if cfg.PartnerPublicKey, err = loadRSAPublic(f); err != nil {
				e = append(e, fmt.Errorf("SIM_A_PARTNER_PUBLIC_KEY_FILE: %w", err))
			}
		}
		if len(e) == 0 {
			stuba.Register(r, sim, cfg)
		}
	case "b":
		cfg := stubb.Config{ClientID: e.need("SIM_B_CLIENT_ID"), ClientSecret: e.need("SIM_B_CLIENT_SECRET"),
			APIKey: e.need("SIM_B_API_KEY"), APISecret: e.need("SIM_B_API_SECRET"), CorporateID: e.need("SIM_B_CORPORATE_ID"),
			TokenTTL: e.duration("SIM_B_TOKEN_TTL", time.Hour)}
		if len(e) == 0 {
			stubb.Register(r, sim, cfg)
		}
	case "c":
		cfg := stubc.Config{APIKey: e.need("SIM_C_API_KEY"), AAASecret: e.need("SIM_C_AAA_SECRET"), Fee: e.int("SIM_C_FEE", 2500),
			AckTTL: e.duration("SIM_C_ACK_TTL", 2*time.Minute), ExecTTL: e.duration("SIM_C_EXEC_TTL", 3*time.Minute),
			WebhookDelay: e.duration("SIM_C_WEBHOOK_DELAY", 200*time.Millisecond)}
		if len(e) == 0 {
			stubc.Register(r, sim, cfg)
		}
	case "d":
		cfg := stubd.Config{ClientKey: e.need("SIM_D_CLIENT_KEY"), Secret: e.need("SIM_D_SECRET"),
			FundingAcct: e.need("SIM_D_FUNDING_ACCT"), Liquidity: e.int("SIM_D_LIQUIDITY", 950000000)}
		if len(e) == 0 {
			stubd.Register(r, sim, cfg)
		}
	}
	if len(e) > 0 {
		return nil, errors.Join(e...)
	}
	return r, nil
}

type envErrs []error

func (e *envErrs) need(k string) string {
	v := os.Getenv(k)
	if v == "" {
		*e = append(*e, fmt.Errorf("%s is required", k))
	}
	return v
}

func (e *envErrs) duration(k string, def time.Duration) time.Duration {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < 0 {
		*e = append(*e, fmt.Errorf("%s: want a duration", k))
	}
	return d
}

func (e *envErrs) int(k string, def int64) int64 {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		*e = append(*e, fmt.Errorf("%s: want a non-negative integer", k))
	}
	return n
}

func getEnv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// keygen creates the dev keys in dir, each unless it exists: S:A's partner
// key pair (the aggregator's RSA-2048 key for the B2B token signature), and
// MOM's Ed25519 request-signing key (mom-signing.key, the MOM_SIGNING_KEY
// seed) with its public half (mom-public.keys, for MOM_PUBLIC_KEYS_FILE).
// Generated at stack start, never committed.
func keygen(dir string) error {
	if err := momKeygen(dir); err != nil {
		return err
	}
	priv, pub := filepath.Join(dir, "bank-a-partner.key"), filepath.Join(dir, "bank-a-partner.pub")
	if _, err := os.Stat(priv); err == nil {
		return nil
	}
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return err
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		return err
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	if err != nil {
		return err
	}
	// 0644: the bank middleware container runs as another uid. A dev volume only.
	if err := os.WriteFile(pub, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o644); err != nil {
		return err
	}
	return os.WriteFile(priv, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o644)
}

func momKeygen(dir string) error {
	seedFile, pubFile := filepath.Join(dir, "mom-signing.key"), filepath.Join(dir, "mom-public.keys")
	if _, err := os.Stat(seedFile); err == nil {
		return nil
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return err
	}
	if err := os.WriteFile(pubFile, []byte("mom-1="+signing.EncodeKey(pub)+"\n"), 0o644); err != nil {
		return err
	}
	return os.WriteFile(seedFile, []byte(base64.StdEncoding.EncodeToString(priv.Seed())+"\n"), 0o644)
}

func loadRSAPublic(path string) (*rsa.PublicKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b, _ := pem.Decode(raw)
	if b == nil {
		return nil, errors.New("not PEM")
	}
	k, err := x509.ParsePKIXPublicKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	rk, ok := k.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("not an RSA key")
	}
	return rk, nil
}
