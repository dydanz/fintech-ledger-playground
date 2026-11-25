// Package service is money-out-middleware's business logic (spec/money-out
// TRD §6): it owns the transfer.* events, the way the core's service layer
// owns the ledger's.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"fmt"
	"regexp"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

// Outcome of receiving a command.
type Outcome string

const (
	Created   Outcome = "created"   // a new transfer, RECEIVED
	Duplicate Outcome = "duplicate" // the same transfer_id and payload, or the same event again: nothing changes
	Conflict  Outcome = "conflict"  // the same transfer_id with a different payload: journaled, nothing changes
	Invalid   Outcome = "invalid"   // not a valid do_transfer command: nothing stored
)

// TxStore is what Intake needs from the repository.
type TxStore interface {
	InTx(ctx context.Context, fn func(*repository.Tx) error) error
}

// Intake is the idempotency gate (P§3.2): it turns a do_transfer command into
// a RECEIVED transfer exactly once.
type Intake struct {
	store TxStore
	now   func() time.Time
}

func NewIntake(store TxStore) *Intake { return &Intake{store: store, now: time.Now} }

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Receive handles one command. Its error is only for failures worth retrying
// (the database); a bad command is Invalid with a nil error.
func (i *Intake) Receive(ctx context.Context, e contract.Envelope) (repository.Transfer, Outcome, error) {
	log := logger.FromContext(ctx)
	deny := func(detail string) (repository.Transfer, Outcome, error) {
		log.WarnContext(ctx, "transfer command refused", "event", "transfer.receive", "outcome", "denied",
			"reason", "invalid_input", "event_id", e.EventID, "detail", detail)
		return repository.Transfer{}, Invalid, nil
	}
	if e.EventType != contract.EventTransferRequested || !uuidPattern.MatchString(e.EventID) {
		return deny("not a TransferRequested event with a UUID event_id")
	}
	var p contract.TransferRequested
	if err := e.DecodePayload(&p); err != nil {
		return deny("payload doesn't match the contract")
	}
	if err := p.Validate(); err != nil {
		return deny(err.Error())
	}
	money, _ := p.Amount.Money() // Validate checked it
	hash, err := payloadHash(p)
	if err != nil {
		return deny("payload can't be hashed")
	}
	var meta struct {
		CoreTransactionID string `json:"core_transaction_id"`
	}
	_ = json.Unmarshal(p.Metadata, &meta)

	var (
		tr      repository.Transfer
		outcome Outcome
	)
	err = i.store.InTx(ctx, func(tx *repository.Tx) error {
		first, err := tx.MarkProcessed(ctx, e.EventID, contract.TopicCommands)
		if err != nil {
			return err
		}
		if !first { // the same event again
			tr, err = tx.LockTransfer(ctx, p.TransferID)
			outcome = Duplicate
			return err
		}
		tr = repository.Transfer{ID: p.TransferID, PayloadHash: hash, Amount: money.Amount, Currency: money.Currency,
			BankCode: p.Beneficiary.BankCode, AccountNo: p.Beneficiary.AccountNo, Metadata: p.Metadata,
			CoreTransactionID: meta.CoreTransactionID, State: transfer.Received, Version: 1, CoreAck: "none",
			TraceID: e.TraceID, RequestedAt: p.RequestedAt}
		received, err := contract.NewEvent(contract.EventTransferReceived, contract.StreamTransfer, tr.ID, 1, i.now(), e.TraceID,
			contract.JournalPayload{TransferID: tr.ID, ToState: string(transfer.Received), CoreTransactionID: tr.CoreTransactionID})
		if err != nil {
			return err
		}
		created, err := tx.InsertTransfer(ctx, tr, received)
		if err != nil || created {
			outcome = Created
			return err
		}
		existing, err := tx.LockTransfer(ctx, p.TransferID)
		if err != nil {
			return err
		}
		tr = existing
		if existing.PayloadHash == hash {
			outcome = Duplicate
			return nil
		}
		rejected, err := contract.NewEvent(contract.EventTransferRejected, contract.StreamTransfer, tr.ID, tr.Version+1, i.now(), e.TraceID,
			contract.JournalPayload{TransferID: tr.ID, FromState: string(tr.State), ToState: string(tr.State),
				Detail: json.RawMessage(`{"reason":"conflict","event_id":"` + e.EventID + `"}`)})
		if err != nil {
			return err
		}
		if tr.Version, err = tx.Record(ctx, tr.ID, tr.Version, rejected); err != nil {
			return err
		}
		outcome = Conflict
		return nil
	})
	attrs := []any{"event", "transfer.receive", "transfer_id", p.TransferID, "event_id", e.EventID,
		"amount", money.Amount, "currency", money.Currency}
	switch {
	case err != nil:
		log.ErrorContext(ctx, "transfer command not stored", append(attrs, "outcome", "failure", "reason", "pg_error", "error", err)...)
		return repository.Transfer{}, "", fmt.Errorf("receive %s: %w", p.TransferID, err)
	case outcome == Conflict:
		log.WarnContext(ctx, "transfer command conflicts with the stored transfer", append(attrs, "outcome", "denied", "reason", "conflict")...)
	case outcome == Duplicate:
		log.InfoContext(ctx, "transfer command already received", append(attrs, "outcome", "success", "duplicate", true)...)
	default:
		log.InfoContext(ctx, "transfer received", append(attrs, "outcome", "success", "duplicate", false)...)
		log.InfoContext(ctx, "transfer state changed", "event", "transfer.state_change", "outcome", "success",
			"transfer_id", tr.ID, "to_state", string(transfer.Received), "version", 1)
	}
	return tr, outcome, nil
}

// payloadHash is the SHA-256 of what makes two commands the same transfer:
// the amount in minor units, the beneficiary and the metadata. requested_at
// is left out, so the core re-sending a command is a duplicate.
func payloadHash(p contract.TransferRequested) (string, error) {
	money, err := p.Amount.Money()
	if err != nil {
		return "", err
	}
	var meta any
	if len(p.Metadata) > 0 {
		if err := json.Unmarshal(p.Metadata, &meta); err != nil {
			return "", err
		}
	}
	b, err := json.Marshal(struct {
		TransferID string `json:"transfer_id"`
		Amount     int64  `json:"amount"`
		Currency   string `json:"currency"`
		BankCode   string `json:"bank_code"`
		AccountNo  string `json:"account_no"`
		Metadata   any    `json:"metadata"` // maps marshal with sorted keys: canonical
	}{p.TransferID, money.Amount, money.Currency, p.Beneficiary.BankCode, p.Beneficiary.AccountNo, meta})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
