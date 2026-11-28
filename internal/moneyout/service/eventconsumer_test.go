package service_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/service"
)

// The bank-events consumer is the commands consumer with its own names: a
// database failure is retried (never skipped), the batch is committed once
// handled, and an undecodable message is skipped under transfer.bank_event.
func TestBankEventsConsumer(t *testing.T) {
	ev, err := contract.NewEvent(contract.EventBankStatusReported, contract.StreamTransfer, "01a1d000-0000-7000-8000-000000000001", 0,
		time.Now(), "", contract.BankStatusReported{})
	if err != nil {
		t.Fatal(err)
	}
	msg, _ := contract.Encode(ev)
	p := &fakePoller{batches: [][]kafka.Message{{{Value: []byte("not json"), Partition: 3, Offset: 7}, {Value: msg.Value}}}}
	var mu sync.Mutex
	calls := 0
	handle := service.HandlerFunc(func(_ context.Context, e contract.Envelope) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return errors.New("postgres down")
		}
		return nil
	})
	var logs bytes.Buffer
	ctx, cancel := context.WithCancel(logger.WithContext(context.Background(), logger.New(&logs, slog.LevelDebug, "money-out-middleware")))
	c := service.NewConsumer(p, handle, time.Millisecond, 5*time.Millisecond).
		WithNames("money_out_bank_events_poll", "money_out_bank_events_commit", "transfer.bank_event")
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		n := len(p.committed)
		p.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	if calls != 2 || len(p.committed) != 1 || len(p.committed[0]) != 2 {
		t.Fatalf("calls %d, commits %v", calls, p.committed)
	}
	out := logs.String()
	if !strings.Contains(out, `"event":"transfer.bank_event"`) || strings.Contains(out, `"event":"transfer.receive"`) {
		t.Fatalf("undecodable message logged under the wrong event: %s", out)
	}
}
