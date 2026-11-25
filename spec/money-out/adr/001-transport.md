# ADR-001: Transport between the layers

**Status:** accepted (2026-11-25, TRD #71) · **Context:** P§2.6, TRD §3, §10

## Context

Four hops have to be asynchronous and idempotent:
- core → money-out-middleware (MOM);
- MOM → bank middleware (BMW);
- BMW → MOM;
- MOM → core.

The core already publishes outward through Kafka (`ledger.events.v1`, from a transactional outbox) and takes commands over HTTP (`/internal/v1/*`). The fraud engine combines the two.

## Decision

| Hop | Transport | Why |
| :--- | :--- | :--- |
| Core (payout-adapter) → MOM | Kafka `money-out.commands.v1`, keyed by `transfer_id` | It's how the core already publishes. The adapter writes the command to its outbox in the same transaction as the debit's outcome. |
| MOM → core | Kafka `money-out.results.v1`, then `money-out.core-acks.v1` for the acknowledgement | Same. The result is written to MOM's outbox in the same transaction as the terminal state. |
| MOM → BMW | **HTTP**, signed (ADR-002) | Sync banks (A, B) give the final status in the response. A BMW stays stateless, with no consumer group or offsets. "Dispatch only after `DISPATCHING` commits" is a plain call after the commit. |
| BMW → MOM (async) | Kafka `money-out.bank-events.v1`, keyed by `transfer_id` | A BMW has no database, so it produces directly. A lost event costs nothing: MOM's status inquiry resolves the attempt. |

## Consequences

- A BMW must be reachable by MOM over HTTP. A BMW outage looks like connection refused, so it's retried on the same node (`reached_bank: false`).
- Every consumer deduplicates on `event_id` (MOM's `processed_events`), because publishing is at least once.
- Kafka over MOM → BMW was the alternative. It was rejected: every sync answer would cost two extra hops, and BMWs would need consumer state.
