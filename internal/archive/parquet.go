package archive

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/parquet-go/parquet-go"
)

// JournalRow is one ledger.journal_entries row as archived.
type JournalRow struct {
	ID             string    `parquet:"id"`
	TransactionID  string    `parquet:"transaction_id"`
	LedgerID       string    `parquet:"ledger_id"`
	Amount         int64     `parquet:"amount"`
	Currency       string    `parquet:"currency"`
	LedgerSequence *int64    `parquet:"ledger_sequence,optional"`
	BalanceAfter   *int64    `parquet:"balance_after,optional"`
	TraceID        string    `parquet:"trace_id"`
	CreatedAt      time.Time `parquet:"created_at,timestamp(microsecond)"`
}

// EventRow is one ledger.events row as archived. Payload is the JSON text.
type EventRow struct {
	EventID       string    `parquet:"event_id"`
	StreamType    string    `parquet:"stream_type"`
	StreamID      string    `parquet:"stream_id"`
	StreamVersion int64     `parquet:"stream_version"`
	EventType     string    `parquet:"event_type"`
	AccountID     string    `parquet:"account_id"`
	SchemaVersion int32     `parquet:"schema_version"`
	Payload       string    `parquet:"payload,json"`
	TraceID       string    `parquet:"trace_id"`
	OccurredAt    time.Time `parquet:"occurred_at,timestamp(microsecond)"`
}

// RowID is the archived row's primary id (journal id or event id).
func RowID(r any) string {
	switch v := r.(type) {
	case JournalRow:
		return v.ID
	case EventRow:
		return v.EventID
	}
	return ""
}

// Encode writes rows as one Parquet file.
func Encode[T JournalRow | EventRow](rows []T) ([]byte, error) {
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[T](&buf, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		return nil, fmt.Errorf("encode parquet: %w", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("encode parquet: %w", err)
	}
	return buf.Bytes(), nil
}

// Decode reads every row of a Parquet file written by Encode.
func Decode[T JournalRow | EventRow](data []byte) ([]T, error) {
	r := parquet.NewGenericReader[T](bytes.NewReader(data))
	defer r.Close()
	out := make([]T, r.NumRows())
	n, err := r.Read(out)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("decode parquet: %w", err)
	}
	if int64(n) != r.NumRows() {
		return nil, fmt.Errorf("decode parquet: read %d of %d rows", n, r.NumRows())
	}
	return out, nil
}

// Manifest describes one archived partition (TRD §7.8). It is written last,
// after every file it lists.
type Manifest struct {
	Table       string         `json:"table"`
	Month       string         `json:"month"` // YYYY-MM, UTC
	Partition   string         `json:"partition"`
	Rows        int64          `json:"rows"`
	MinID       string         `json:"min_id"`
	MaxID       string         `json:"max_id"`
	Files       []ManifestFile `json:"files"`
	Checkpoints []Checkpoint   `json:"checkpoints,omitempty"` // journal only
	CreatedAt   time.Time      `json:"created_at"`
}

type ManifestFile struct {
	Key    string `json:"key"`
	URI    string `json:"uri"`
	Rows   int64  `json:"rows"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
	MinID  string `json:"min_id"`
	MaxID  string `json:"max_id"`
}

// Checkpoint is a ledger's balance at the last sequence inside the partition.
type Checkpoint struct {
	LedgerID     string `json:"ledger_id"`
	AsOfSequence int64  `json:"as_of_sequence"`
	Balance      int64  `json:"balance"`
}

// SHA256 is the hex SHA-256 of data.
func SHA256(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}
