package service

import (
	"context"
	"encoding/json"
	"time"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

// change is one state transition, logged as transfer.state_change after its
// transaction commits.
type change struct {
	transferID string
	from, to   transfer.State
	version    int64
	attemptNo  int
}

func logChanges(ctx context.Context, cs []change) {
	log := logger.FromContext(ctx)
	for _, c := range cs {
		attrs := []any{"event", "transfer.state_change", "outcome", "success", "transfer_id", c.transferID,
			"from_state", string(c.from), "to_state", string(c.to), "version", c.version}
		if c.attemptNo > 0 {
			attrs = append(attrs, "attempt_no", c.attemptNo)
		}
		log.InfoContext(ctx, "transfer state changed", attrs...)
	}
}

// transition moves t to `to`, journaling eventType with payload p, and
// updates t.
func transition(ctx context.Context, tx *repository.Tx, t *repository.Transfer, to transfer.State, eventType string, at time.Time,
	p contract.JournalPayload) (change, error) {
	p.TransferID, p.FromState, p.ToState, p.CoreTransactionID = t.ID, string(t.State), string(to), t.CoreTransactionID
	ev, err := contract.NewEvent(eventType, contract.StreamTransfer, t.ID, t.Version+1, at, t.TraceID, p)
	if err != nil {
		return change{}, err
	}
	v, err := tx.Transition(ctx, t.ID, t.State, t.Version, to, ev)
	if err != nil {
		return change{}, err
	}
	c := change{transferID: t.ID, from: t.State, to: to, version: v, attemptNo: p.AttemptNo}
	t.State, t.Version = to, v
	return c, nil
}

// finish moves t to SUCCEEDED or FAILED and, in the same transaction,
// writes the one result the core gets (T§3.3) and marks the core's
// acknowledgement pending (P§3.4a #4).
func finish(ctx context.Context, tx *repository.Tx, t *repository.Transfer, at time.Time, r contract.TransferResult) (change, error) {
	to, eventType := transfer.Succeeded, contract.EventTransferSucceeded
	if r.Status == contract.StatusFailed {
		to, eventType = transfer.Failed, contract.EventTransferFailed
	}
	r.TransferID, r.CompletedAt = t.ID, at
	detail, err := json.Marshal(r)
	if err != nil {
		return change{}, err
	}
	c, err := transition(ctx, tx, t, to, eventType, at, contract.JournalPayload{AttemptNo: r.Attempts, Detail: detail})
	if err != nil {
		return change{}, err
	}
	result, err := contract.NewEvent(eventType, contract.StreamTransfer, t.ID, t.Version, at, t.TraceID, r)
	if err != nil {
		return change{}, err
	}
	if err := tx.Enqueue(ctx, contract.TopicResults, result); err != nil {
		return change{}, err
	}
	return c, tx.SetCoreAckPending(ctx, t.ID)
}
