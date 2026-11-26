package bankmw

import (
	"context"
	"time"

	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// Publisher is the Kafka producer.
type Publisher interface {
	Publish(ctx context.Context, recs []kafka.Record) error
}

// Emitter publishes asynchronous statuses straight to Kafka: a bank
// middleware has no database, so there is no outbox. A lost publish is
// recovered by MOM's status inquiry (T§3.6).
type Emitter struct {
	name  string
	pub   Publisher
	now   func() time.Time
	topic string // always contract.TopicBankEvents outside tests
}

func NewEmitter(name string, pub Publisher, now func() time.Time) *Emitter {
	return &Emitter{name: name, pub: pub, now: now, topic: contract.TopicBankEvents}
}

// Report publishes BankStatusReported for one attempt, keyed by transfer_id.
// The envelope's stream_version is 0: a bank middleware holds no stream, and
// MOM orders bank events by its own attempt state.
func (e *Emitter) Report(ctx context.Context, transferID, requestID string, res contract.BankResult) error {
	if err := res.Validate(); err != nil {
		return err
	}
	at := e.now()
	env, err := contract.NewEvent(contract.EventBankStatusReported, contract.StreamTransfer, transferID, 0, at, "",
		contract.BankStatusReported{TransferID: transferID, RequestID: requestID, BankMiddleware: e.name, Result: res, ReportedAt: at})
	if err != nil {
		return err
	}
	msg, err := contract.Encode(env)
	if err != nil {
		return err
	}
	log := logger.FromContext(ctx)
	if err := e.pub.Publish(ctx, []kafka.Record{{Topic: e.topic, Key: transferID, Value: msg.Value, Headers: msg.Headers}}); err != nil {
		log.ErrorContext(ctx, "bank status not published", "event", "bank.status_report", "outcome", "failure", "reason", "kafka_error",
			"bank_middleware", e.name, "transfer_id", transferID, "request_id", requestID, "status", string(res.Status), "error", err)
		return err
	}
	log.InfoContext(ctx, "bank status published", "event", "bank.status_report", "outcome", "success", "bank_middleware", e.name,
		"transfer_id", transferID, "request_id", requestID, "status", string(res.Status), "event_id", env.EventID)
	return nil
}
