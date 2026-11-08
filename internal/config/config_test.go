package config_test

import (
	"strings"
	"testing"
	"time"

	"auth-api/internal/config"
)

var (
	hashA = strings.Repeat("a", 64)
	hashB = strings.Repeat("b", 64)
)

func TestParseClients(t *testing.T) {
	got, err := config.ParseClients("payments:" + hashA + ":credit,debit,read; ops:" + strings.ToUpper(hashB) + ":freeze;")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "payments" || got[0].TokenHash != hashA || strings.Join(got[0].Operations, ",") != "credit,debit,read" ||
		got[1].ID != "ops" || got[1].TokenHash != hashB {
		t.Fatalf("clients = %+v", got)
	}
}

func TestParseClientsRejects(t *testing.T) {
	for name, v := range map[string]string{
		"empty":          "",
		"only separator": " ; ",
		"missing part":   "payments:" + hashA,
		"short hash":     "payments:abc:credit",
		"bad id":         "Payments:" + hashA + ":credit",
		"unknown op":     "payments:" + hashA + ":teleport",
		"no ops":         "payments:" + hashA + ":",
		"duplicate id":   "p:" + hashA + ":credit;p:" + hashB + ":debit",
		"shared hash":    "p:" + hashA + ":credit;q:" + hashA + ":debit",
	} {
		_, err := config.ParseClients(v)
		if err == nil {
			t.Errorf("%s: accepted", name)
			continue
		}
		if strings.Contains(err.Error(), hashA) || strings.Contains(err.Error(), hashB) {
			t.Errorf("%s: error leaks a token hash: %v", name, err)
		}
	}
}

func TestLoadLedgerDefaultsAndErrors(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("LEDGER_CLIENTS", "payments:"+hashA+":credit")
	cfg, err := config.LoadLedger()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "ledger-api" || cfg.HTTPAddr != ":8081" || strings.Join(cfg.Currencies, ",") != "IDR" ||
		cfg.MaxAmount != 1_000_000_000_000 || cfg.ProcessTimeout != 2*time.Second || cfg.IdempotencyTTL != 24*time.Hour ||
		cfg.EventsTopic != "ledger.events.v1" {
		t.Fatalf("defaults = %+v", cfg)
	}

	t.Setenv("LEDGER_CLIENTS", "")
	t.Setenv("LEDGER_PROCESS_TIMEOUT", "soon")
	t.Setenv("LEDGER_MAX_AMOUNT", "lots")
	_, err = config.LoadLedger()
	for _, want := range []string{"LEDGER_CLIENTS", "LEDGER_PROCESS_TIMEOUT", "LEDGER_MAX_AMOUNT"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not mention %s", err, want)
		}
	}
}

// TRD §16 Q6: only LEDGER_UNFREEZE_CLIENTS (default ops) may hold unfreeze.
func TestUnfreezeGrantIsRestricted(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("LEDGER_CLIENTS", "ops:"+hashA+":freeze,unfreeze;fraud-engine:"+hashB+":freeze")
	if _, err := config.LoadLedger(); err != nil {
		t.Fatalf("ops with unfreeze rejected: %v", err)
	}
	t.Setenv("LEDGER_CLIENTS", "ops:"+hashA+":freeze;fraud-engine:"+hashB+":freeze,unfreeze")
	if _, err := config.LoadLedger(); err == nil || !strings.Contains(err.Error(), "fraud-engine may not be granted unfreeze") {
		t.Fatalf("fraud-engine with unfreeze: err = %v", err)
	}
	t.Setenv("LEDGER_UNFREEZE_CLIENTS", "ops,fraud-engine")
	if _, err := config.LoadLedger(); err != nil {
		t.Fatalf("explicitly allowed: %v", err)
	}
}

func TestLoadWorkerDefaultsAndErrors(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	cfg, err := config.LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "ledger-worker" || cfg.HTTPAddr != ":8082" || cfg.SweepInterval != 5*time.Second ||
		cfg.SweepAfter != 5*time.Second || cfg.PendingMaxAge != 15*time.Minute || cfg.PurgeInterval != time.Minute ||
		cfg.ProcessTimeout != 2*time.Second || strings.Join(cfg.Currencies, ",") != "IDR" ||
		cfg.OutboxPoll != 200*time.Millisecond || cfg.OutboxBatch != 500 || cfg.OutboxRetention != 72*time.Hour ||
		strings.Join(cfg.KafkaBrokers, ",") != "localhost:9094" || cfg.ReconcileEvery != time.Minute {
		t.Fatalf("defaults = %+v", cfg)
	}
	t.Setenv("RECONCILE_INTERVAL", "0s")
	if _, err := config.LoadWorker(); err == nil || !strings.Contains(err.Error(), "RECONCILE_INTERVAL") {
		t.Fatalf("zero reconcile interval: %v", err)
	}
	t.Setenv("RECONCILE_INTERVAL", "")
	// The sweeper must not take over a Tx2 the request path may still finish.
	t.Setenv("LEDGER_SWEEP_AFTER", "1s")
	if _, err := config.LoadWorker(); err == nil || !strings.Contains(err.Error(), "LEDGER_SWEEP_AFTER") {
		t.Fatalf("sweep after < process timeout: %v", err)
	}
	t.Setenv("LEDGER_SWEEP_AFTER", "")
	t.Setenv("LEDGER_SWEEP_INTERVAL", "0s")
	if _, err := config.LoadWorker(); err == nil {
		t.Fatal("zero sweep interval accepted")
	}
}

func TestParseCurrencyLimits(t *testing.T) {
	got, err := config.ParseCurrencyLimits(" IDR:50000000 , USD:5000 ")
	if err != nil || got["IDR"] != 50_000_000 || got["USD"] != 5000 || len(got) != 2 {
		t.Fatalf("got %v %v", got, err)
	}
	if got, err := config.ParseCurrencyLimits(""); err != nil || len(got) != 0 {
		t.Fatalf("empty: %v %v", got, err)
	}
	for _, bad := range []string{"IDR", "idr:5", "IDR:0", "IDR:-1", "IDR:1.5", "IDR:5,IDR:6", "IDRX:5"} {
		if _, err := config.ParseCurrencyLimits(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestLoadFraud(t *testing.T) {
	t.Setenv("FRAUD_CLIENT_TOKEN", "tok")
	cfg, err := config.LoadFraud()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "fraud-engine" || cfg.Group != "fraud-engine" || cfg.TopupMax != 3 || cfg.TopupWindow != 5*time.Minute ||
		cfg.DebitWindow != time.Hour || cfg.DebitMaxCount != 30 || cfg.DebitLimits["IDR"] != 50_000_000 || cfg.FreezeRetry != 30*time.Second {
		t.Fatalf("defaults = %+v", cfg)
	}
	t.Setenv("FRAUD_CLIENT_TOKEN", "")
	t.Setenv("FRAUD_TOPUP_MAX", "0")
	t.Setenv("FRAUD_DEBIT_LIMITS", "IDR:x")
	_, err = config.LoadFraud()
	for _, want := range []string{"FRAUD_CLIENT_TOKEN", "FRAUD_TOPUP_MAX", "FRAUD_DEBIT_LIMITS"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("error %v does not mention %s", err, want)
		}
	}
	if err != nil && strings.Contains(err.Error(), "tok") {
		t.Error("error leaks the token")
	}
}

func TestLoadWorkerArchive(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://x")
	cfg, err := config.LoadWorker()
	if err != nil {
		t.Fatal(err)
	}
	if a := cfg.Archive; a.After != 61320*time.Hour || a.BucketURI != "" || a.HoldAll || len(a.Hold) != 0 ||
		!a.Secure || a.CreateBucket || a.RetentionDays != 36500 || a.Region != "us-east-1" {
		t.Fatalf("archive defaults = %+v", a)
	}
	t.Setenv("LEDGER_LEGAL_HOLD", "0192F3C4-0000-7000-8000-000000000001, 0192f3c4-0000-7000-8000-000000000002")
	if cfg, err = config.LoadWorker(); err != nil || len(cfg.Archive.Hold) != 2 || cfg.Archive.Hold[0] != "0192f3c4-0000-7000-8000-000000000001" {
		t.Fatalf("hold list: %+v %v", cfg.Archive.Hold, err)
	}
	t.Setenv("LEDGER_LEGAL_HOLD", "*")
	if cfg, err = config.LoadWorker(); err != nil || !cfg.Archive.HoldAll {
		t.Fatalf("hold all: %+v %v", cfg.Archive, err)
	}
	for key, val := range map[string]string{"LEDGER_LEGAL_HOLD": "alice", "ARCHIVE_BUCKET_URI": "s3://b",
		"ARCHIVE_RETENTION_DAYS": "0", "ARCHIVE_S3_SECURE": "maybe", "LEDGER_ARCHIVE_AFTER": "-1h"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, val)
			if _, err := config.LoadWorker(); err == nil || !strings.Contains(err.Error(), key) && !strings.Contains(err.Error(), "ARCHIVE") {
				t.Fatalf("%s=%s accepted: %v", key, val, err)
			}
		})
	}
}
