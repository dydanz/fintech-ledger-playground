// Package kafkatest gives integration tests a throwaway topic on the
// compose broker (TEST_KAFKA_BROKERS), deleted when the test ends.
package kafkatest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"auth-api/internal/kafka"
)

// NewTopic creates a topic with partitions and returns the brokers and its
// name. It skips the test if TEST_KAFKA_BROKERS is unset.
func NewTopic(t *testing.T, partitions int32) ([]string, string) {
	t.Helper()
	v := os.Getenv("TEST_KAFKA_BROKERS")
	if v == "" {
		t.Skip("TEST_KAFKA_BROKERS not set; run `make test-integration` with kafka up")
	}
	brokers := kafka.SplitBrokers(v)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(cl)
	name := strings.NewReplacer("/", ".", " ", "_").Replace(t.Name())
	topic := fmt.Sprintf("test.%s.%d", name, time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := adm.CreateTopic(ctx, partitions, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, topic)
		cl.Close()
	})
	return brokers, topic
}

// ReadN consumes topic from the start until want records arrive or timeout.
func ReadN(t *testing.T, brokers []string, topic string, want int, timeout time.Duration) []kafka.Message {
	t.Helper()
	c, err := kafka.NewConsumer(brokers, "read-"+topic+fmt.Sprint(time.Now().UnixNano()), topic)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var out []kafka.Message
	for len(out) < want {
		msgs, err := c.Poll(ctx)
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, msgs...)
	}
	return out
}
