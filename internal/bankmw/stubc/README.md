# bank-middleware-c: Bank-Stub-C (async multi-ACK with AAA callback)

`cmd/bank-middleware-c` adapts S:C (`spec/2026-11-25 bank-api-stubs.md`) to the canonical money-out contract. Its internal routes accept only MOM-signed requests (`internal/bankmw`). It also listens for the bank's two webhooks. It has no database: each attempt's handshake lives in memory, in this process only.

MOM sees `ACCEPTED` (synchronous), then `PROCESSING` and the terminal status as `BankStatusReported` events on `money-out.bank-events.v1`.

## Flow

1. **Initiate.** `POST /v2/remit/async/initiate`, with `pg_trx_ref`, `source_vault_id`, `dest_bank_code`, `bank_acct_no`, `remit_amt` (a JSON integer of whole rupiah), `ccy` and `callback_base_url` (`BANK_C_CALLBACK_BASE_URL`, this BMW's `/callbacks/bank-c`). `202 INIT_ACCEPTED` is answered as `ACCEPTED`, with `bank_reference = bank_session_id`. The session exists *before* the call, because the bank can send ACK-1 before its 202 reaches us. The webhook waits up to 5 s for the initiate answer.
2. **ACK-1 webhook** (`POST /callbacks/bank-c/ack-1`). It is reported as `PROCESSING`. The BMW then computes the AAA digest:
   - `hex(SHA-256(values in canonical_fields_order, joined by "|"))`, then `Base64(HMAC-SHA256(BANK_C_AAA_SECRET, digest))`;
   - it sends both to `POST /v2/remit/async/aaa-verify`.
3. **ACK-2 webhook** (`POST /callbacks/bank-c/ack-2`). If it's valid, the BMW sends `POST /v2/remit/async/execute` with the single-use `execution_token`, **at most once**, before `execution_deadline`.
4. **Status poll** (`GET /v2/remit/async/status/{pg_trx_ref}`). It runs every `BANK_C_POLL_INTERVAL` for the whole session, as the fallback for a dropped webhook. MOM's `InquireStatus` uses the same call, and needs no session.

Every call carries `Authorization: Bearer BANK_C_API_KEY` and `X-PG-Request-Time` (WIB).

## Identity at the bank (A)

**`pg_trx_ref = request_id`.** It's unique per attempt, and a status poll after a restart needs nothing else.

## Webhook checks

S:C defines no webhook signature (T§12.1 #5, Q8). A webhook is accepted only when every check below passes. Otherwise it's refused and logged as `bank.webhook` `denied`, never with the body, nonce, token or account name.

| Check | Refusal |
| :--- | :--- |
| `X-BankC-Event` matches the route; the body parses | `400 invalid_input` |
| A **live session in this process** with that `pg_trx_ref`, and the same `bank_session_id` | `404 unknown_session` |
| ACK-1: `required_hash_alg` is `SHA-256`; every `canonical_fields_order` field is one the BMW knows (an unknown field would hash a value the bank didn't intend) | `400 invalid_input` |
| ACK-1: before `expires_at` | `410 expired` |
| ACK-2: after ACK-1 | `409 wrong_stage` |
| ACK-2 (valid): it carries a token and a deadline; its `bank_acct_no` and `remit_amt` are the ones initiated | `400 invalid_input` / `400 mismatch` (never executed) |

A repeated webhook is answered `200` and ignored (`duplicate: true`). A replayed ACK-2 never executes twice.

## Safety rules (BMW)

- **`execute` is sent at most once,** and only by the process that holds the token. It's never retried, not even after a timeout or 5xx, because the bank may have acted.
- **A dropped ACK-2 can't be executed:** the token exists only in that webhook. The poll keeps reading status until the bank reports `EXPIRED` (its deadline passed without an execute), and the result is then `FAILED OTHER` with `retry_elsewhere`. This is P§5 #9 as agreed on 2026-11-27. A dropped ACK-1 ends the same way.
- **After a restart, no session is left.** A webhook gets `404 unknown_session`, nothing is executed, and MOM's inquiry reads the bank's status: `AWAITING_*` is `PROCESSING` until `EXPIRED`, which is `FAILED` (P§5 #10).
- **A failed initiate leaves no session.** A late ACK-1 is refused, so the attempt can never be executed. A retry of the same `request_id` initiates again.
- **After initiate, the session never reports "not found" as `FAILED`:** MOM would fail over, and the transfer could be paid twice.
- **While a webhook is still expected,** a polled `FAILED` counts only after 3 polls in a row, so a refusing ACK-2 in flight gives the precise reason (`INVALID_ACCOUNT`, no retry elsewhere) rather than `BANK_REJECTED`.
- **`execution_deadline` has whole seconds.** The BMW doesn't execute at or after it, which leans to the safe side.
- **One `bank.session` per attempt,** when the session ends: `success` with `status`, or `failure` with `deadline`, `shutdown` or `report_failed`. In each failure case, MOM's inquiry resolves the attempt.
- **Ordering:** a bank event can reach MOM before MOM has recorded the synchronous `ACCEPTED` (M4 must not depend on the order).

## Status mapping (T§11.3)

**(A)** marks an assumption (Q7). **(BMW)** marks a rule this adapter adds.

| Step and bank answer | Canonical | `reason`, `retry_elsewhere`, `reached_bank` |
| :--- | :--- | :--- |
| Any rail but `BIFAST` (A) | FAILED | `RAIL_UNAVAILABLE`, true, false (no call made) |
| Initiate `202 INIT_ACCEPTED` | ACCEPTED | `bank_reference = bank_session_id` |
| Initiate: connection refused at dial | FAILED | `RAIL_UNAVAILABLE`, true, false |
| Initiate `401` | FAILED | `AUTH_ERROR`, true, false |
| Initiate: timeout, 5xx, `409` or anything else | UNKNOWN | — (no session: never executed; the inquiry decides) |
| ACK-1 accepted | PROCESSING (event) | — |
| aaa-verify `401 AAA_FAILED` **(BMW)** | FAILED | `AUTH_ERROR`, true, true (no token can be issued) |
| aaa-verify: timeout or 5xx | — | ACK-2 or the poll decides |
| ACK-2 `request_valid: false` (A) | FAILED | `BANK_REJECTED`, true, true |
| ACK-2 `acct_valid: false` (A) | FAILED | `INVALID_ACCOUNT` (`ACCOUNT_CLOSED` when `acct_status` says closed), false, true |
| ACK-2 valid, deadline passed before execute | FAILED | `OTHER`, true, true (`DEADLINE_PASSED`) |
| Execute `200 final_state: SETTLED` | SUCCEEDED | `bank_reference = core_settlement_id` |
| Execute `202 final_state: PROCESSING` (A) | PROCESSING | the poll decides |
| Execute `410 EXECUTION_EXPIRED` (A) | FAILED | `OTHER`, true, true |
| Execute: connection refused at dial | FAILED | `RAIL_UNAVAILABLE`, true, false (not sent, and never sent again) |
| Execute: timeout, 5xx, or a refused or used token | UNKNOWN (event) | the poll decides; never re-sent |
| Status `SETTLED` | SUCCEEDED | `bank_reference = core_settlement_id` |
| Status `PROCESSING`, `AWAITING_ACK_1`, `AWAITING_AAA_VERIFY`, `VALIDATING_ACCOUNT_FOR_ACK_2`, `AWAITING_EXECUTE` (A) | PROCESSING | — |
| Status `FAILED` (A) | FAILED | `BANK_REJECTED`, true, true |
| Status `EXPIRED` (A) | FAILED | `OTHER`, true, true (never executed) |
| Status `404` | FAILED (doesn't exist), but **only on MOM's inquiry**, never inside a session | `OTHER`, true, false |
| Status: any other answer, 5xx or timeout | UNKNOWN | — |

`bank_raw_code` carries the bank's `error`, `aaa_status`, `final_state` or `acct_status`, unchanged.

**Fee (T§12.1 #7, Q9).** S:C debits `remit_amt + fee_amt` from the source vault, and the beneficiary gets `remit_amt`. The BMW ignores `fee_amt` and `debited_amt`; who bears the fee is still open.

## Configuration

Required, with no defaults in code:
- `BANK_C_BASE_URL`;
- `BANK_C_API_KEY`, `BANK_C_AAA_SECRET`;
- `BANK_C_SOURCE_VAULT_ID` (T§12.1 #9);
- `BANK_C_CALLBACK_BASE_URL`, this BMW's address as the bank reaches it, ending in `/callbacks/bank-c`;
- `KAFKA_BROKERS`.

Optional:
- `BANK_C_POLL_INTERVAL`, default `5s`;
- `BANK_C_POLL_FOR`, default `30m`.

The shared settings are listed in `internal/config/bankmw.go`.
