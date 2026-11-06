package service_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"auth-api/internal/logger"
	"auth-api/internal/repository"
	"auth-api/internal/service"
)

const (
	testTrace   = "4bf92f3577b34da6a3ce929d0e0e4736"
	testAccount = "0192f3c4-0000-7000-8000-000000000001"
)

// untouchable fails the test if validation lets a request reach the store.
type untouchable struct{ t *testing.T }

func (u untouchable) InTx(context.Context, time.Duration, func(repository.LedgerTx) error) error {
	u.t.Fatal("store.InTx called for a request that should fail validation")
	return nil
}
func (u untouchable) GetAccount(context.Context, string) (repository.AccountRecord, error) {
	u.t.Fatal("store.GetAccount called for a request that should fail validation")
	return repository.AccountRecord{}, nil
}
func (u untouchable) GetTransaction(context.Context, string) (repository.TransactionRecord, error) {
	u.t.Fatal("store.GetTransaction called")
	return repository.TransactionRecord{}, nil
}

func testConfig() service.LedgerConfig {
	return service.LedgerConfig{Currencies: []string{"IDR", "USD"}, MaxAmount: 1_000_000_000_000,
		ProcessTimeout: 2 * time.Second, IdempotencyTTL: 24 * time.Hour, OverdraftClients: []string{"ops"}}
}

func logCtx() (context.Context, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return logger.WithContext(context.Background(), logger.New(buf, slog.LevelDebug, "ledger-api")), buf
}

// records parses every JSON log line in buf.
func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			t.Fatalf("log line is not JSON: %q", sc.Text())
		}
		out = append(out, m)
	}
	return out
}

// wantEvent asserts exactly one record with event, and the given fields.
func wantEvent(t *testing.T, buf *bytes.Buffer, event string, fields map[string]any) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, r := range records(t, buf) {
		if r["event"] == event {
			found = append(found, r)
		}
	}
	if len(found) != 1 {
		t.Fatalf("want 1 %q record, got %d in:\n%s", event, len(found), buf)
	}
	for k, v := range fields {
		if found[0][k] != v {
			t.Errorf("%s.%s = %v, want %v (record %v)", event, k, found[0][k], v, found[0])
		}
	}
	return found[0]
}

func TestNewLedgerServiceValidatesConfig(t *testing.T) {
	for name, mutate := range map[string]func(*service.LedgerConfig){
		"no currencies":      func(c *service.LedgerConfig) { c.Currencies = nil },
		"bad currency":       func(c *service.LedgerConfig) { c.Currencies = []string{"idr"} },
		"zero max amount":    func(c *service.LedgerConfig) { c.MaxAmount = 0 },
		"huge max amount":    func(c *service.LedgerConfig) { c.MaxAmount = 1 << 62 },
		"no timeout":         func(c *service.LedgerConfig) { c.ProcessTimeout = 0 },
		"no idempotency TTL": func(c *service.LedgerConfig) { c.IdempotencyTTL = 0 },
	} {
		cfg := testConfig()
		mutate(&cfg)
		if _, err := service.NewLedgerService(untouchable{t}, cfg); err == nil {
			t.Errorf("%s: config accepted", name)
		}
	}
}

func TestMoneyValidationDeniesBeforeTouchingTheStore(t *testing.T) {
	svc, err := service.NewLedgerService(untouchable{t}, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	valid := func() service.MoneyCommand {
		return service.MoneyCommand{
			Request:   service.Request{ClientID: "payments", IdempotencyKey: "k-1", RequestID: "r", TraceID: testTrace},
			AccountID: testAccount, Amount: 100, Currency: "IDR",
		}
	}
	cases := []struct {
		name   string
		mutate func(*service.MoneyCommand)
		reason string
		err    error // nil = any *ValidationError
	}{
		{"missing key", func(c *service.MoneyCommand) { c.IdempotencyKey = "" }, "invalid_input", nil},
		{"bad key", func(c *service.MoneyCommand) { c.IdempotencyKey = "has space" }, "invalid_input", nil},
		{"no client", func(c *service.MoneyCommand) { c.ClientID = "" }, "invalid_input", nil},
		{"bad trace", func(c *service.MoneyCommand) { c.TraceID = "x" }, "invalid_input", nil},
		{"zero amount", func(c *service.MoneyCommand) { c.Amount = 0 }, "invalid_input", nil},
		{"amount over max", func(c *service.MoneyCommand) { c.Amount = 1_000_000_000_001 }, "invalid_input", nil},
		{"unsupported currency", func(c *service.MoneyCommand) { c.Currency = "EUR" }, "currency_unsupported", nil},
		{"long reference", func(c *service.MoneyCommand) { c.Reference = strings.Repeat("r", 65) }, "invalid_input", nil},
		{"metadata not an object", func(c *service.MoneyCommand) { c.Metadata = json.RawMessage(`[1]`) }, "invalid_input", nil},
		{"metadata too big", func(c *service.MoneyCommand) {
			c.Metadata = json.RawMessage(`{"x":"` + strings.Repeat("a", 4100) + `"}`)
		}, "invalid_input", nil},
		{"account id not a uuid", func(c *service.MoneyCommand) { c.AccountID = "nope" }, "account_not_found", service.ErrAccountNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, buf := logCtx()
			cmd := valid()
			cmd.Reference, cmd.Metadata = "SECRET-REF", json.RawMessage(`{"secret":"meta"}`)
			tc.mutate(&cmd)
			_, err := svc.Debit(ctx, cmd)
			var ve *service.ValidationError
			if tc.err != nil && !errors.Is(err, tc.err) || tc.err == nil && !errors.As(err, &ve) {
				t.Fatalf("err = %v", err)
			}
			rec := wantEvent(t, buf, "ledger.debit", map[string]any{"outcome": "denied", "reason": tc.reason, "level": "warn"})
			if rec["client_id"] != cmd.ClientID {
				t.Errorf("client_id = %v", rec["client_id"])
			}
			for _, secret := range []string{"SECRET-REF", "secret", cmd.IdempotencyKey} {
				if secret != "" && strings.Contains(buf.String(), secret) {
					t.Errorf("log contains %q: %s", secret, buf)
				}
			}
		})
	}
}

func TestOpenValidationDeniesBeforeTouchingTheStore(t *testing.T) {
	svc, _ := service.NewLedgerService(untouchable{t}, testConfig())
	req := service.Request{ClientID: "onboarding", IdempotencyKey: "k", TraceID: testTrace}

	ctx, buf := logCtx()
	if _, err := svc.OpenAccount(ctx, service.OpenAccountCommand{Request: req, UserID: "not-a-uuid"}); err == nil {
		t.Fatal("bad user id accepted")
	}
	wantEvent(t, buf, "ledger.account_open", map[string]any{"outcome": "denied", "reason": "invalid_input"})

	ctx, buf = logCtx()
	if _, err := svc.OpenLedger(ctx, service.OpenLedgerCommand{Request: req, AccountID: testAccount, Currency: "EUR"}); err == nil {
		t.Fatal("unsupported currency accepted")
	}
	wantEvent(t, buf, "ledger.account_open", map[string]any{"outcome": "denied", "reason": "currency_unsupported"})

	ctx, buf = logCtx()
	if _, err := svc.OpenLedger(ctx, service.OpenLedgerCommand{Request: req, AccountID: testAccount, Currency: "USD", OverdraftLimit: -1}); err == nil {
		t.Fatal("negative overdraft accepted")
	}
	wantEvent(t, buf, "ledger.account_open", map[string]any{"outcome": "denied", "reason": "invalid_input"})
}
