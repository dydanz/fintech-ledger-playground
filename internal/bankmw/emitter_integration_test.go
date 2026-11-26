//go:build integration

package bankmw

import (
	"context"
	"testing"
	"time"

	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/testutil/kafkatest"
)

// Through a real broker: one transfer's statuses land on one partition, in
// order, as BankStatusReported envelopes MOM can decode.
func TestEmitterOnRealKafka(t *testing.T) {
	brokers, topic := kafkatest.NewTopic(t, 12)
	p, err := kafka.NewProducer(brokers, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	em := NewEmitter("bank-middleware-d", p, time.Now)
	em.topic = topic
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	const tid, rid = "01a1d000-0000-7000-8000-000000000001", "01a1d000-0000-7000-8000-0000000000aa"
	want := []contract.Status{contract.StatusAccepted, contract.StatusProcessing, contract.StatusSucceeded}
	for _, st := range want {
		if err := em.Report(ctx, tid, rid, contract.BankResult{Status: st, ReachedBank: true, BankReference: "RTGS-1"}); err != nil {
			t.Fatal(err)
		}
	}
	c, err := kafka.NewConsumer(brokers, "test-"+topic, topic)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var got []kafka.Message
	for len(got) < len(want) {
		msgs, err := c.Poll(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, msgs...)
	}
	for i, m := range got {
		if m.Key != tid || m.Partition != got[0].Partition || m.Headers["event_type"] != contract.EventBankStatusReported {
			t.Fatalf("message %d: key %s partition %d headers %v", i, m.Key, m.Partition, m.Headers)
		}
		env, err := contract.DecodeEnvelope(m.Value)
		if err != nil {
			t.Fatal(err)
		}
		var p contract.BankStatusReported
		if err := env.DecodePayload(&p); err != nil || p.Result.Status != want[i] || p.RequestID != rid {
			t.Fatalf("message %d: %+v %v, want %s", i, p, err, want[i])
		}
	}
	t.Logf("3 statuses on partition %d in order: %s, %s, %s", got[0].Partition, want[0], want[1], want[2])
}
