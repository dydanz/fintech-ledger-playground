# Business Requirements Document (BRD): Immutable Ledger & Event-Sourced Balance System

## 1. Executive Summary

This document outlines the requirements for a highly accountable, real-time Balance System designed on strict financial journaling principles. To guarantee absolute data integrity, traceability, and auditability, the system will utilize an Event Sourcing architecture backed by an immutable, append-only ledger. It is designed to handle high-volume transactional workloads natively, employing asynchronous processing models to guarantee a 99.99% success ratio and zero data loss under concurrent load.

## 2. Business Objectives

* **Financial Integrity:** Prevent double-spending, ghost balances, and race conditions through strict concurrency controls and immutable journaling.
* **Traceability & Auditability:** Track the complete lifecycle of every balance mutation from the origin request to the final settled state using unified trace IDs.
* **Real-Time Visibility:** Provide instantaneous balance reflection for users and real-time data feeds for downstream fraud detection engines.
* **Accountability:** Ensure every operation affecting a balance is uniquely identifiable, strictly ordered, and fully recoverable.

## 3. Architectural Principles

* **1:1 Account Mapping:** A strict `1..1` relationship constraint between a User Identity and a Balance Account. Multi-currency or multi-wallet features will be handled via sub-ledgers tied to this singular root account.
* **Immutable Append-Only Ledger:** The core balance journal is write-only. Updates or deletions are physically prohibited at the database level.
* **Correction via Voiding:** Erroneous or reversed transactions are handled using compensating transactions (a "Void" or "Reversal" journal entry) that negate the original amount, preserving the historical trail.
* **Event Sourcing:** The system state is not stored as a mutable row, but calculated as the left-fold of all past events. Every state transition (e.g., `TOPUP_REQUESTED` -> `TOPUP_PROCESSING` -> `TOPUP_SETTLED`) is recorded as an immutable event.
* **Idempotency by Design:** All mutating endpoints require a client-generated `idempotency_key`. The system will safely reject or replay the exact same response for duplicate keys within a configurable TTL (e.g., 24 hours).

## 4. System States & Event Sourcing Lifecycle

Every feature impacting the balance (Top-up, Deduction, Reversal) operates as a distinct workflow with strict transactional states.

| State | Description | Event Sourced Record |
| --- | --- | --- |
| **PENDING** | Request received, trace-id assigned, idempotency checked. | `TransactionRequestedEvent` |
| **PROCESSING** | Funds are being verified or locked. | `TransactionProcessingEvent` |
| **SETTLED** | Journal entry written. Balance is officially mutated. | `TransactionSettledEvent` |
| **FAILED** | Transaction failed pre-flight checks (e.g., NSF - Non-Sufficient Funds). | `TransactionFailedEvent` |
| **VOIDED** | A previously settled transaction is negated via a compensating entry. | `TransactionVoidedEvent` |

## 5. Functional Requirements

### 5.1. Core Ledger Operations

* **FR-01:** The system shall support `CREDIT` (Top-up/Add) and `DEBIT` (Deduct) operations.
* **FR-02:** The system must enforce a hard constraint preventing a balance from falling below zero (unless specific overdraft products are authorized).
* **FR-03:** The system shall automatically calculate the current available balance by aggregating the immutable journal entries in real-time, optionally utilizing read-optimized materialized views or CQRS projections for read performance.
* **FR-04:** The system shall expose a `VOID` operation that accepts an original transaction ID and generates a new, mathematically opposing journal entry.

### 5.2. Traceability & Idempotency

* **FR-05:** The system must generate or accept a universal `trace_id` at the API gateway level, propagating it through all microservices, Kafka topics, and database records.
* **FR-06:** The system must validate the `idempotency_key` against a distributed cache (e.g., Redis) and persistent storage before initiating any state transition.

### 5.3. Real-Time Tracking & Fraud Detection

* **FR-07:** The system shall publish every state transition event to a real-time message broker (e.g., Apache Kafka) for downstream consumption.
* **FR-08:** The Fraud Detection Engine shall consume the event stream to evaluate velocity rules (e.g., >3 top-ups in 5 minutes, abnormal deduction volumes) in real-time.
* **FR-09:** The system must support a `FREEZE` state, triggered by the Fraud Engine, which instantly blocks all `DEBIT` transactions on the targeted balance account while allowing `CREDIT` transactions to settle.

## 6. Non-Functional Requirements (NFRs)

* **Throughput & Scalability:** The system must support continuous high-volume throughput, baselined at a minimum of +300 Transactions Per Second (TPS), utilizing asynchronous processing and horizontal database scaling strategies.
* **Consistency:** The system must favor Strict Consistency (ACID) over Availability for write operations to prevent race conditions during concurrent deduction requests.
* **Reliability:** The core journal must maintain a 99.99% success ratio. Event sourcing mechanisms must guarantee Exactly-Once processing semantics.
* **Data Retention:** Journal entries and event logs must be retained indefinitely in cold storage (e.g., Amazon S3/GCS) after 7 years for financial compliance, while keeping active ledgers highly available in the primary datastore (e.g., PostgreSQL).
