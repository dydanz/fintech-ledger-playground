package service_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/service"
)

type fakePoller struct {
	mu        sync.Mutex
	batches   [][]kafka.Message
	committed [][]kafka.Message
}

func (f *fakePoller) Poll(ctx context.Context) ([]kafka.Message, error) {
	f.mu.Lock()
	if len(f.batches) > 0 {
		b := f.batches[0]
		f.batches = f.batches[1:]
		f.mu.Unlock()
		return b, nil
	}
	f.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (f *fakePoller) Commit(_ context.Context, msgs []kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.committed = append(f.committed, msgs)
	return nil
}

// fakeReceiver fails the first failN calls, then succeeds.
type fakeReceiver struct {
	mu    sync.Mutex
	calls int
	failN int
	seen  []string
}

func (f *fakeReceiver) Receive(_ context.Context, e contract.Envelope) (repository.Transfer, service.Outcome, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls <= f.failN {
		return repository.Transfer{}, "", errors.New("postgres down")
	}
	f.seen = append(f.seen, e.EventID)
	return repository.Transfer{}, service.Created, nil
}

func msg(t *testing.T, v string) kafka.Message {
	t.Helper()
	e, _ := contract.NewEvent(contract.EventTransferRequested, contract.StreamTransfer, v, 1, time.Now(), "t", map[string]string{})
	b, _ := json.Marshal(e)
	return kafka.Message{Topic: contract.TopicCommands, Key: v, Value: b}
}

// A batch is committed only after every message in it was handled; a store
// failure is retried, never skipped; an undecodable message is skipped.
func TestConsumerCommitsAfterHandlingAndRetries(t *testing.T) {
	good1, good2 := msg(t, "a"), msg(t, "b")
	junk := kafka.Message{Topic: contract.TopicCommands, Value: []byte("not json")}
	p := &fakePoller{batches: [][]kafka.Message{{good1, junk, good2}}}
	r := &fakeReceiver{failN: 2}
	logs := &bytes.Buffer{}
	ctx, cancel := context.WithCancel(logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "money-out-middleware")))
	c := service.NewConsumer(p, r, time.Millisecond, 5*time.Millisecond)
	done := make(chan struct{})
	go func() { _ = c.Run(ctx); close(done) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		n := len(p.committed)
		p.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if len(p.committed) != 1 || len(p.committed[0]) != 3 {
		t.Fatalf("commits = %v, want the whole batch once", p.committed)
	}
	if r.calls != 4 || len(r.seen) != 2 {
		t.Fatalf("receive calls = %d (seen %d), want 2 failures then 2 successes", r.calls, len(r.seen))
	}
	if !bytes.Contains(logs.Bytes(), []byte(`"reason":"invalid_input"`)) {
		t.Fatalf("the undecodable message wasn't logged:\n%s", logs)
	}
}
