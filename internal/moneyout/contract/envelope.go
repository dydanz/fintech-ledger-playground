package contract

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"fintech-ledger-playground/internal/outbox"
)

// SchemaVersion of every money-out event payload.
const SchemaVersion = 1

// Stream types: a transfer's history, and a breaker's.
const (
	StreamTransfer = "transfer"
	StreamBreaker  = "breaker"
)

// Envelope is the JSON body of every money-out message. It follows the core's
// envelope (internal/outbox, TRD L§7.3) field for field, without account_id
// and ledger_id. Consumers order on (stream_id, stream_version) and
// deduplicate on event_id.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	StreamType    string          `json:"stream_type"`
	StreamID      string          `json:"stream_id"`
	StreamVersion int64           `json:"stream_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}

// NewEvent builds an envelope with a new UUIDv7 event id.
func NewEvent(eventType, streamType, streamID string, version int64, at time.Time, traceID string, payload any) (Envelope, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, fmt.Errorf("contract: event id: %w", err)
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("contract: marshal %s payload: %w", eventType, err)
	}
	return Envelope{EventID: id.String(), EventType: eventType, SchemaVersion: SchemaVersion, StreamType: streamType,
		StreamID: streamID, StreamVersion: version, OccurredAt: at.UTC(), TraceID: traceID, Payload: b}, nil
}

// Encode builds the outbox row for e, keyed by its stream id (the transfer
// id), with the core's headers: the same trace, a new span.
func Encode(e Envelope) (outbox.Message, error) {
	value, err := json.Marshal(e)
	if err != nil {
		return outbox.Message{}, fmt.Errorf("contract: marshal envelope: %w", err)
	}
	span := make([]byte, 8)
	_, _ = rand.Read(span)
	return outbox.Message{EventID: e.EventID, Key: e.StreamID, Value: value, Headers: map[string]string{
		"traceparent":    "00-" + e.TraceID + "-" + hex.EncodeToString(span) + "-01",
		"event_type":     e.EventType,
		"schema_version": strconv.Itoa(e.SchemaVersion),
	}}, nil
}

// DecodeEnvelope parses a message value.
func DecodeEnvelope(b []byte) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(b, &e); err != nil {
		return Envelope{}, fmt.Errorf("contract: decode envelope: %w", err)
	}
	return e, nil
}

// DecodePayload decodes the payload into v, refusing unknown fields: a
// command carrying a field the contract doesn't define (a rail, a bank) is
// rejected, not ignored.
func (e Envelope) DecodePayload(v any) error {
	d := json.NewDecoder(bytes.NewReader(e.Payload))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		return fmt.Errorf("contract: decode %s payload: %w", e.EventType, err)
	}
	return nil
}
