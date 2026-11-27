package bankmw_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

type fakePub struct {
	recs []kafka.Record
	err  error
}

func (p *fakePub) Publish(_ context.Context, recs []kafka.Record) error {
	if p.err != nil {
		return p.err
	}
	p.recs = append(p.recs, recs...)
	return nil
}

const tid, rid = "01a1d000-0000-7000-8000-000000000001", "01a1d000-0000-7000-8000-0000000000aa"

// T§3.6, T§7: BankStatusReported on money-out.bank-events.v1, keyed by
// transfer_id so one transfer's events stay in order on one partition.
func TestEmitterPublishesBankStatusReported(t *testing.T) {
	pub := &fakePub{}
	var logs bytes.Buffer
	at := time.Date(2026, 11, 25, 2, 30, 0, 0, time.UTC)
	em := bankmw.NewEmitter("bank-middleware-d", pub, func() time.Time { return at })
	ctx := logger.WithContext(context.Background(), logger.New(&logs, slog.LevelDebug, "bank-middleware-d"))
	res := contract.BankResult{Status: contract.StatusProcessing, ReachedBank: true, BankReference: "RTGS-20261125-4410", BankRawCode: "IN_CLEARING"}
	if err := em.Report(ctx, tid, rid, res); err != nil {
		t.Fatal(err)
	}
	if len(pub.recs) != 1 {
		t.Fatalf("records = %d", len(pub.recs))
	}
	r := pub.recs[0]
	if r.Topic != contract.TopicBankEvents || r.Key != tid || r.Headers["event_type"] != contract.EventBankStatusReported {
		t.Fatalf("record = %+v", r)
	}
	env, err := contract.DecodeEnvelope(r.Value)
	if err != nil {
		t.Fatal(err)
	}
	var p contract.BankStatusReported
	if err := env.DecodePayload(&p); err != nil {
		t.Fatal(err)
	}
	if env.StreamType != contract.StreamTransfer || env.StreamID != tid || env.StreamVersion != 0 || env.EventID == "" {
		t.Fatalf("envelope = %+v", env)
	}
	if p.TransferID != tid || p.RequestID != rid || p.BankMiddleware != "bank-middleware-d" || p.Result != res || !p.ReportedAt.Equal(at) {
		t.Fatalf("payload = %+v", p)
	}
	if !strings.Contains(logs.String(), `"event":"bank.status_report"`) || !strings.Contains(logs.String(), `"outcome":"success"`) {
		t.Fatalf("log = %s", logs.String())
	}
}

func TestEmitterRefusesAnInvalidResult(t *testing.T) {
	pub := &fakePub{}
	em := bankmw.NewEmitter("b", pub, time.Now)
	if err := em.Report(context.Background(), tid, rid, contract.BankResult{Status: contract.StatusFailed}); err == nil || len(pub.recs) != 0 {
		t.Fatalf("err %v, published %d", err, len(pub.recs))
	}
}

// A lost publish isn't retried here: MOM's inquiry resolves the attempt
// (T§3.6). The failure is returned and logged.
func TestEmitterReportsPublishFailure(t *testing.T) {
	pub := &fakePub{err: errors.New("broker down")}
	var logs bytes.Buffer
	ctx := logger.WithContext(context.Background(), logger.New(&logs, slog.LevelDebug, "b"))
	em := bankmw.NewEmitter("b", pub, time.Now)
	if err := em.Report(ctx, tid, rid, contract.BankResult{Status: contract.StatusSucceeded}); err == nil {
		t.Fatal("no error")
	}
	if !strings.Contains(logs.String(), `"reason":"kafka_error"`) || !strings.Contains(logs.String(), `"level":"error"`) {
		t.Fatalf("log = %s", logs.String())
	}
}
