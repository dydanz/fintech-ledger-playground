// Package outbox encodes ledger events into the Kafka envelope written to
// ledger.outbox, and relays the rows to Kafka (spec/TRD §7.3).
package outbox

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"auth-api/internal/ledger"
)

const (
	// DefaultTopic is the single ledger topic, keyed by account_id.
	DefaultTopic = "ledger.events.v1"
	// NotifyChannel is the Postgres LISTEN/NOTIFY channel that wakes the relay.
	NotifyChannel = "ledger_outbox"
)

// Envelope is the JSON body of every message on the ledger topic. Consumers
// order and deduplicate on (stream_id, stream_version) and event_id.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	StreamType    string          `json:"stream_type"`
	StreamID      string          `json:"stream_id"`
	StreamVersion int64           `json:"stream_version"`
	AccountID     string          `json:"account_id"`
	LedgerID      string          `json:"ledger_id,omitempty"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}

// Message is one outbox row: the Kafka key, value and headers.
type Message struct {
	EventID string
	Key     string
	Value   []byte
	Headers map[string]string
}

// Encode builds the outbox message for e. ledgerID is the user ledger the
// event concerns, when known. e.OccurredAt must be the stored value.
func Encode(e ledger.Event, ledgerID string) (Message, error) {
	stream, err := e.Type().Stream()
	if err != nil {
		return Message{}, err
	}
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return Message{}, fmt.Errorf("outbox: marshal %s payload: %w", e.Type(), err)
	}
	value, err := json.Marshal(Envelope{
		EventID:       e.ID,
		EventType:     string(e.Type()),
		SchemaVersion: ledger.SchemaVersion,
		StreamType:    string(stream),
		StreamID:      e.StreamID,
		StreamVersion: e.StreamVersion,
		AccountID:     e.AccountID,
		LedgerID:      ledgerID,
		OccurredAt:    e.OccurredAt.UTC(),
		TraceID:       e.TraceID,
		Payload:       payload,
	})
	if err != nil {
		return Message{}, fmt.Errorf("outbox: marshal envelope: %w", err)
	}
	return Message{
		EventID: e.ID,
		Key:     e.AccountID,
		Value:   value,
		Headers: map[string]string{
			// Same trace, new span: the consumer continues the request's trace.
			"traceparent":    "00-" + e.TraceID + "-" + newSpanID() + "-01",
			"event_type":     string(e.Type()),
			"schema_version": strconv.Itoa(ledger.SchemaVersion),
		},
	}, nil
}

func newSpanID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
