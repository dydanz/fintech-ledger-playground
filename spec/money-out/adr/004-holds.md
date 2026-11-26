# ADR-004: Holding money without a hold primitive (H1)

**Status:** accepted (2026-11-25, Q1 answered in #71) · **Context:** P§2.1, P§3.4a, TRD §2.2–2.3

## Context

The brief asks the core to reserve the amount before `do_transfer`, finalize it on success, and release it on failure, using existing primitives. The core has no holds by design (core TRD §1.2). It has `DEBIT`, `VOID` (a compensating reversal) and SYSTEM counterparties.

## Decision

**H1:**
- **Before** the command is emitted, payout-adapter debits the user into a new SYSTEM ledger, `payout_clearing`. Adding it is an additive migration with no code change.
- **On `FAILED`,** it voids that debit.
- **On `SUCCEEDED`,** nothing is posted: the debit already was the final debit. The acknowledgement carries `core_entry: DEBIT_FINAL` and the debit's id.

## Consequences

- The amount can't be spent twice while the bank pays. `payout_clearing`'s balance is exactly the money in flight, and the core's reconciler keeps checking the double entry.
- P§3.4a #3's "success ↔ hold converted to final debit" has no separate journal entry. Reconciliation check 2 matches each terminal transfer to its debit or void through the acknowledgement instead.
- **Manual corrections** use new compensating entries:
  - a `VOID` of the debit;
  - a new `DEBIT` when a released transfer turns out to have been paid. That one can be refused for insufficient funds; the RUNBOOK escalates it (Q2).
- The core's code, schema and behaviour are unchanged. H2 (real holds) was rejected as a core change.
