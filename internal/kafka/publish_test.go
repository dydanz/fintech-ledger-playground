package kafka_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"auth-api/internal/kafka"
)

// stallingBroker is an in-process Kafka whose produce requests can be made
// to go unanswered, as when a broker dies after a request was sent. That is
// the case the chaos drill hit (spec/Chaos Runbook §6.1): the idempotent
// producer won't fail records it has sent, so cancelling the context alone
// doesn't stop the wait.
type stallingBroker struct {
	c        *kfake.Cluster
	stall    atomic.Bool
	release  chan struct{}
	once     sync.Once
	produced atomic.Int64 // produce requests the broker answered
}

const topic = "ledger.events.test"

func newStallingBroker(t *testing.T) *stallingBroker {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	if err != nil {
		t.Fatal(err)
	}
	b := &stallingBroker{c: c, release: make(chan struct{})}
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if b.stall.Load() {
			c.SleepControl(func() { <-b.release })
		}
		b.produced.Add(1)
		return nil, nil, false // let the cluster handle it
	})
	t.Cleanup(func() { b.unstall(); c.Close() })
	return b
}

func (b *stallingBroker) unstall() { b.once.Do(func() { close(b.release) }) }

// records returns how many records the topic holds.
func (b *stallingBroker) records(t *testing.T) int64 {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	var n int64
	ends.Each(func(o kadm.ListedOffset) { n += o.Offset })
	return n
}

func batch(prefix string, n int) []kafka.Record {
	recs := make([]kafka.Record, n)
	for i := range recs {
		recs[i] = kafka.Record{Topic: topic, Key: fmt.Sprintf("acct-%d", i), Value: []byte(fmt.Sprintf(`{"event_id":"%s-%d"}`, prefix, i))}
	}
	return recs
}

// publish runs p.Publish with a deadline of d and fails the test if it
// hasn't returned 3 s after that deadline.
func publish(t *testing.T, p *kafka.Producer, recs []kafka.Record, d time.Duration) (error, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- p.Publish(ctx, recs) }()
	select {
	case err := <-done:
		return err, time.Since(start)
	case <-time.After(d + 3*time.Second):
		t.Fatalf("Publish still blocked 3 s after its %v deadline: the relay would stall for the whole outage", d)
		return nil, 0
	}
}

func newProducer(t *testing.T, b *stallingBroker) *kafka.Producer {
	t.Helper()
	p, err := kafka.NewProducer(b.c.ListenAddrs(), topic)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	// A first batch while the broker answers: the producer has its
	// metadata and producer id, so the stalled batch is really sent.
	if err, _ := publish(t, p, batch("warm", 3), 5*time.Second); err != nil {
		t.Fatalf("warm-up publish: %v", err)
	}
	return p
}

// A broker that stops answering makes Publish return at its deadline, so the
// relay can log the failure, report its lag and retry.
func TestPublishReturnsAtTheDeadlineWhenTheBrokerStopsAnswering(t *testing.T) {
	b := newStallingBroker(t)
	p := newProducer(t, b)

	b.stall.Store(true)
	err, took := publish(t, p, batch("stalled", 10), 200*time.Millisecond)
	if err == nil {
		t.Fatal("Publish succeeded although the broker never answered")
	}
	if took > time.Second {
		t.Fatalf("Publish returned after %v, want about its 200ms deadline", took)
	}
}

// While an abandoned batch is still in flight, a retry doesn't buffer the
// records again: an outage costs at most one extra copy of one batch, not
// one per retry. Once the broker answers, publishing works again.
func TestPublishDoesNotRebufferWhileAnAbandonedBatchIsInFlight(t *testing.T) {
	b := newStallingBroker(t)
	p := newProducer(t, b)
	before := b.records(t)

	b.stall.Store(true)
	recs := batch("stalled", 10)
	for i := range 3 { // the relay retrying the same batch
		if err, _ := publish(t, p, recs, 200*time.Millisecond); err == nil {
			t.Fatalf("retry %d succeeded although the broker never answered", i)
		}
	}

	b.stall.Store(false)
	b.unstall()
	var err error
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if err, _ = publish(t, p, recs, time.Second); err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("Publish after the broker recovered: %v", err)
	}
	// The abandoned copy plus the successful retry: 2 × 10, not 4 × 10.
	if got := b.records(t) - before; got != 2*int64(len(recs)) {
		t.Fatalf("%d records written for a batch of %d retried 3 times during the stall, want %d", got, len(recs), 2*len(recs))
	}
}
