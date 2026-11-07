// Package kafka wraps franz-go for the ledger's event feed (spec/TRD §7.3):
// a producer for the outbox relay and a consumer-group reader for the fraud
// engine. Kafka is a feed, not the archive; Postgres stays the authority.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"auth-api/internal/startup"
)

// Record is one message to publish. Key decides the partition: the relay
// keys by account_id so one account's events stay in order (§7.3).
type Record struct {
	Topic   string // "" for the producer's default topic
	Key     string
	Value   []byte
	Headers map[string]string
}

// Message is one consumed record.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
	raw       *kgo.Record
}

// SplitBrokers parses KAFKA_BROKERS ("host:port,host:port").
func SplitBrokers(v string) []string {
	var out []string
	for _, b := range strings.Split(v, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}

type Producer struct {
	cl *kgo.Client
}

// NewProducer produces to topic with idempotent writes (franz-go's
// default), acks=all and a 5 ms linger.
func NewProducer(brokers []string, topic string, opts ...kgo.Opt) (*Producer, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(5 * time.Millisecond),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl}, nil
}

// Publish produces recs and waits until the broker has acknowledged every
// one. It returns the first error; on error some records may have been
// written, so callers must treat the batch as unpublished and retry it
// (at-least-once, §7.3).
func (p *Producer) Publish(ctx context.Context, recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	krs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		kr := &kgo.Record{Topic: r.Topic, Key: []byte(r.Key), Value: r.Value}
		for k, v := range r.Headers {
			kr.Headers = append(kr.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
		}
		krs[i] = kr
	}
	return p.cl.ProduceSync(ctx, krs...).FirstErr()
}

// IsRejected reports whether err is the broker refusing records (a
// non-retriable Kafka error such as an oversized record or an unknown
// topic) rather than the broker being unreachable.
func IsRejected(err error) bool {
	var ke *kerr.Error
	return errors.As(err, &ke) && !ke.Retriable
}

// Ping checks a broker is reachable.
func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

func (p *Producer) Close() { p.cl.Close() }

// Connect pings until a broker answers, logging dependency.connect with
// dependency=kafka through startup.Retry.
func (p *Producer) Connect(ctx context.Context, log *slog.Logger) error {
	return startup.Retry(ctx, log, "kafka", p.Ping)
}

type Consumer struct {
	cl *kgo.Client
}

// NewConsumer joins group on topic. Offsets are committed only by Commit,
// after the caller has processed a batch, so a crash re-delivers rather
// than skips (consumers deduplicate on event_id).
func NewConsumer(brokers []string, group, topic string, opts ...kgo.Opt) (*Consumer, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{cl: cl}, nil
}

// Poll returns the next batch, blocking until records arrive or ctx ends.
func (c *Consumer) Poll(ctx context.Context) ([]Message, error) {
	fetches := c.cl.PollFetches(ctx)
	if fetches.IsClientClosed() {
		return nil, errors.New("kafka consumer closed")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var errs []error
	fetches.EachError(func(topic string, partition int32, err error) {
		errs = append(errs, fmt.Errorf("fetch %s/%d: %w", topic, partition, err))
	})
	var out []Message
	fetches.EachRecord(func(r *kgo.Record) {
		m := Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: r.Value,
			Headers: make(map[string]string, len(r.Headers)), raw: r}
		for _, h := range r.Headers {
			m.Headers[h.Key] = string(h.Value)
		}
		out = append(out, m)
	})
	return out, errors.Join(errs...)
}

// Commit marks msgs processed for the group.
func (c *Consumer) Commit(ctx context.Context, msgs []Message) error {
	recs := make([]*kgo.Record, 0, len(msgs))
	for _, m := range msgs {
		recs = append(recs, m.raw)
	}
	return c.cl.CommitRecords(ctx, recs...)
}

func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

func (c *Consumer) Close() { c.cl.Close() }
