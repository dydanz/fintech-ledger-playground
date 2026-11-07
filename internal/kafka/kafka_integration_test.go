//go:build integration

package kafka_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"auth-api/internal/kafka"
	"auth-api/internal/testutil/kafkatest"
)

func TestProduceConsumeRoundTrip(t *testing.T) {
	brokers, topic := kafkatest.NewTopic(t, 12)
	p, err := kafka.NewProducer(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var recs []kafka.Record
	for i := range 20 { // two keys, interleaved
		key := []string{"acct-a", "acct-b"}[i%2]
		recs = append(recs, kafka.Record{Key: key, Value: []byte(fmt.Sprintf(`{"n":%d}`, i)),
			Headers: map[string]string{"event_type": "TransactionSettled", "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}})
	}
	if err := p.Publish(ctx, recs); err != nil {
		t.Fatal(err)
	}

	c, err := kafka.NewConsumer(brokers, "test-"+topic, topic)
	if err != nil {
		t.Fatal(err)
	}
	var got []kafka.Message
	for len(got) < len(recs) {
		msgs, err := c.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, msgs...)
	}
	partition := map[string]int32{}
	last := map[string]int{}
	for _, m := range got {
		if p, ok := partition[m.Key]; ok && p != m.Partition {
			t.Fatalf("key %s on partitions %d and %d", m.Key, p, m.Partition)
		}
		partition[m.Key] = m.Partition
		var n int
		fmt.Sscanf(string(m.Value), `{"n":%d}`, &n)
		if prev, ok := last[m.Key]; ok && n <= prev {
			t.Fatalf("key %s out of order: %d after %d", m.Key, n, prev)
		}
		last[m.Key] = n
		if m.Headers["event_type"] != "TransactionSettled" || m.Headers["traceparent"] == "" {
			t.Fatalf("headers = %v", m.Headers)
		}
	}
	if err := c.Commit(ctx, got); err != nil {
		t.Fatal(err)
	}
	c.Close()

	// The committed group resumes after the batch: nothing is re-delivered.
	if err := p.Publish(ctx, []kafka.Record{{Key: "acct-a", Value: []byte(`{"n":99}`)}}); err != nil {
		t.Fatal(err)
	}
	c2, err := kafka.NewConsumer(brokers, "test-"+topic, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	msgs, err := c2.Poll(ctx)
	if err != nil || len(msgs) != 1 || string(msgs[0].Value) != `{"n":99}` {
		t.Fatalf("after commit: %v %v", msgs, err)
	}
}

// Without a commit, a restarted consumer gets the batch again (at-least-once).
func TestUncommittedIsRedelivered(t *testing.T) {
	brokers, topic := kafkatest.NewTopic(t, 1)
	p, _ := kafka.NewProducer(brokers, topic)
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.Publish(ctx, []kafka.Record{{Key: "k", Value: []byte("one")}}); err != nil {
		t.Fatal(err)
	}
	for attempt := range 2 {
		c, err := kafka.NewConsumer(brokers, "redeliver-"+topic, topic)
		if err != nil {
			t.Fatal(err)
		}
		msgs, err := c.Poll(ctx)
		if err != nil || len(msgs) != 1 || string(msgs[0].Value) != "one" {
			t.Fatalf("attempt %d: %v %v", attempt, msgs, err)
		}
		c.Close() // no Commit
	}
}

func TestPublishToMissingTopicFails(t *testing.T) {
	brokers, _ := kafkatest.NewTopic(t, 1)
	p, _ := kafka.NewProducer(brokers, "does.not.exist", kgo.RecordDeliveryTimeout(3*time.Second))
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Publish(ctx, []kafka.Record{{Key: "k", Value: []byte("x")}}); err == nil {
		t.Fatal("published to a topic that doesn't exist (auto-create must be off)")
	}
}
