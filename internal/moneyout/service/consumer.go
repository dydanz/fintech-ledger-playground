package service

import (
	"context"
	"time"

	"fintech-ledger-playground/internal/kafka"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
)

// Poller is the consumer side of the commands topic (kafka.Consumer).
type Poller interface {
	Poll(ctx context.Context) ([]kafka.Message, error)
	Commit(ctx context.Context, msgs []kafka.Message) error
}

// Receiver handles one command (Intake).
type Receiver interface {
	Receive(ctx context.Context, e contract.Envelope) (repository.Transfer, Outcome, error)
}

// Consumer reads money-out.commands.v1. A batch is committed only once every
// message in it is handled, so a crash redelivers rather than skips; Intake
// deduplicates. A database failure is retried with backoff, never skipped.
// An undecodable message is logged and skipped: it would never succeed.
type Consumer struct {
	poll       Poller
	recv       Receiver
	minBackoff time.Duration
	maxBackoff time.Duration
}

func NewConsumer(p Poller, r Receiver, minBackoff, maxBackoff time.Duration) *Consumer {
	return &Consumer{poll: p, recv: r, minBackoff: minBackoff, maxBackoff: maxBackoff}
}

// Run consumes until ctx ends.
func (c *Consumer) Run(ctx context.Context) error {
	log := logger.FromContext(ctx)
	for ctx.Err() == nil {
		msgs, err := c.poll.Poll(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			log.WarnContext(ctx, "commands topic poll failed", "event", "dependency.call", "outcome", "failure",
				"reason", "unknown", "dependency", "kafka", "operation", "money_out_poll", "error", err)
			sleep(ctx, c.maxBackoff)
			continue
		}
		for _, m := range msgs {
			e, err := contract.DecodeEnvelope(m.Value)
			if err != nil {
				log.WarnContext(ctx, "transfer command refused", "event", "transfer.receive", "outcome", "denied",
					"reason", "invalid_input", "detail", "not a JSON envelope", "partition", m.Partition, "offset", m.Offset)
				continue
			}
			for backoff := c.minBackoff; ; backoff = min(backoff*2, c.maxBackoff) {
				if _, _, err := c.recv.Receive(ctx, e); err == nil {
					break
				}
				if !sleep(ctx, backoff) {
					return nil // uncommitted: redelivered after a restart
				}
			}
		}
		if len(msgs) > 0 {
			if err := c.poll.Commit(ctx, msgs); err != nil && ctx.Err() == nil {
				log.WarnContext(ctx, "commands offset commit failed", "event", "dependency.call", "outcome", "failure",
					"reason", "unknown", "dependency", "kafka", "operation", "money_out_commit", "error", err)
			}
		}
	}
	return nil
}

// sleep waits d or until ctx ends, reporting whether the full wait elapsed.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
