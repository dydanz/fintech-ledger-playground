# ADR-003: Database and journal consistency

**Status:** accepted (2026-11-25) · **Context:** P§3.5, TRD §5, §7 · **Code:** `internal/moneyout/repository` (M1.3)

## Context

MOM keeps current state in Postgres and full history in the `money-out.journal.v1` topic. It also emits results to the core. A database commit and a Kafka write can't be atomic.

## Decision

The core's **transactional outbox** (TRD L§7.3), reused:
- **What's written together:** every state change is a compare-and-set on the transfer's `version`. In the **same transaction**, it inserts its journal event, and any result or command, into `outbox`.
- **The relay:** the core's `outbox.Relay`, run as a `worker.Runner` leader job, publishes rows in id order, at least once. `repository.OutboxRepo` adapts it to MOM's table.
- **Version numbering:** a transfer's `version` equals the journal `stream_version` of its last event. The store refuses an event that doesn't follow it (`ErrStreamVersion`), so the stream is gapless.
- **Consumers:** they deduplicate on `event_id`.
- **HTTP calls to a BMW** are made only after the transaction recording `DISPATCHING` has committed.

## Consequences

- If MOM crashes after a commit but before publishing, the relay publishes on restart. There's no gap in the journal, and consumers drop duplicates (P§5 #14).
- The database is the projection and the journal is the history. Reconciliation check 3 compares them (TRD §8).
