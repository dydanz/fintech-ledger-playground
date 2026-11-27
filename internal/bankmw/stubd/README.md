# bank-middleware-d: Bank-Stub-D (multi-rail, sync submit, async settle)

`cmd/bank-middleware-d` adapts S:D (`spec/2026-11-25 bank-api-stubs.md`) to the canonical money-out contract. It accepts only MOM-signed requests (`internal/bankmw`) and has no database. Its only state is its in-memory pollers. The bulk and maker-checker routes aren't used (P§1.3).

## Flow

1. **Account validation.** `POST /api/v2/disbursement/acct-validate`, with `rail-mode` set to the request's rail.
2. **Funding and rail check.** `POST /api/v2/disbursement/funding-check`. It checks that the rail's window is open and that `available-liquidity` covers `tx-amount`.
3. **Single remittance.** `POST /api/v2/disbursement/remit-single`. A `SUBMITTED` answer with `tx-state: IN_CLEARING` is returned to MOM as `ACCEPTED`.
4. **Poller.** Every `BANK_D_POLL_INTERVAL`, it calls `POST /api/v2/disbursement/remit-status` with `pg-order-id`, and publishes `BankStatusReported` to `money-out.bank-events.v1`:
   - `PROCESSING` once, on the first `IN_CLEARING`;
   - the terminal status once, then it stops.
5. **Status** (`InquireStatus`, called by MOM): the same remit-status call, answered synchronously.

**Signing.** Every call carries `X-Client-Key` and `X-Signature-512`, where the signature is `Base64(HMAC-SHA512(secret, minified body))`. The contract doesn't define `canonicalBody`; the minified body is assumed (T§12.1 #3). The request envelope is `{"meta": {"client-trace-id", "sent-at"}, "data": {…}}`. `sent-at` is in WIB.

**Amount.** `tx-amount` is a JSON integer of whole rupiah. IDR has exponent 0 (L§4.1 A2), so `"150000000.00"` is sent as `150000000`.

## Identity at the bank (A)

The contract's `pg-order-id` has no stated format. This adapter sends **`pg-order-id = request_id`**. That makes it unique per attempt, and it's all a later `remit-status` needs, even after a restart. The contract shows `remit-status` only for a `batch-ticket`; the single-remittance form by `pg-order-id` is assumed (T§12.1 #4).

## The poller

- **One poller per attempt.** If the same `request_id` is sent again, the bank answers `SUBMITTED` again, and no second poller starts.
- **It reports only states the bank returned.** After `SUBMITTED`, an "order not found", a 5xx or a timeout is **never** reported as `FAILED`: MOM would fail over, and the transfer could be paid twice. The poller keeps asking.
- **It stops** on a terminal status, after `BANK_D_POLL_FOR`, when the process shuts down (`Built.Stop`), or after a terminal status fails to publish 3 times.
- **It logs one `bank.poll`** when it stops: `success` with `status`, or `failure` with reason `deadline`, `shutdown` or `report_failed`. In every failure case, MOM's status inquiry resolves the attempt (T§3.6). A restart loses the pollers in the same way, and is recovered the same way.
- **Ordering.** A bank event may reach MOM before MOM has recorded the synchronous `ACCEPTED`, because they travel separately. The poller waits one interval before its first poll, which makes this unlikely, but MOM must not depend on it (M4).

## Status mapping (T§11.4)

**(A)** marks an assumption: the contract shows only success responses (Q7). **(BMW)** marks a rule this adapter adds. Nothing is `FAILED` unless no money can have moved.

| Step and bank answer | Canonical | `reason`, `retry_elsewhere`, `reached_bank` |
| :--- | :--- | :--- |
| A rail other than `BIFAST`, `SKNBI` or `RTGS` | FAILED | `RAIL_UNAVAILABLE`, true, false (no call made) |
| acct-validate `00 ACCOUNT_FOUND` | continue | `registered-owner` becomes `dest-acct-name` |
| acct-validate `14` not found **(A)** | FAILED | `INVALID_ACCOUNT`, false, true |
| acct-validate: any other code **(BMW)** | FAILED | `BANK_REJECTED`, true, true (no remittance was sent) |
| funding-check `rail-window-open: false` | FAILED | `CUTOFF`, true, true |
| funding-check `available-liquidity < tx-amount` | FAILED | `INSUFFICIENT_SOURCE_FUNDS`, true, true |
| funding-check: any other code **(BMW)** | FAILED | `BANK_REJECTED`, true, true |
| `401` at acct-validate, funding-check or remit-single | FAILED | `AUTH_ERROR`, true, false (the signature was refused before anything was processed) |
| acct-validate or funding-check: 5xx, timeout, connection refused, or an unreadable answer **(BMW)** | FAILED | `RAIL_UNAVAILABLE`, true, false (no remittance was sent) |
| remit-single `00 SUBMITTED`, `tx-state: IN_CLEARING` | ACCEPTED, then PROCESSING by event | `bank_reference = bank-rail-ref` |
| remit-single `00`, `tx-state: SETTLED` **(BMW)** | SUCCEEDED | — |
| remit-single: connection refused at dial | FAILED | `RAIL_UNAVAILABLE`, true, false |
| remit-single: timeout, 5xx, `409` duplicate, or an unmapped answer | UNKNOWN | — (no poller; the status inquiry decides) |
| remit-status `SETTLED` | SUCCEEDED | `bank_reference = bank-rail-ref` |
| remit-status `IN_CLEARING` | PROCESSING | — |
| remit-status `REJECTED` / `RETURNED` **(A)** | FAILED | `BANK_REJECTED`, true, true |
| remit-status `404`, code `14` (order not found) **(A)** | FAILED (doesn't exist), but **only on MOM's inquiry**, never from the poller | `OTHER`, true, false |
| remit-status: any other answer, 5xx or timeout | UNKNOWN | — |

`bank_raw_code` carries the bank's `meta.status-code` or `tx-state`, unchanged.

## Configuration

Required, with no defaults in code:
- `BANK_D_BASE_URL`;
- `BANK_D_CLIENT_KEY`, `BANK_D_SECRET` (`X-Signature-512`);
- `BANK_D_FUNDING_ACCOUNT` (`funding-acct-no`, T§12.1 #9);
- `KAFKA_BROKERS`, shared with the other bank middlewares. Bank D can't start without it, because its statuses have nowhere else to go.

Optional:
- `BANK_D_POLL_INTERVAL`, default `5s`;
- `BANK_D_POLL_FOR`, default `30m`.

The shared settings are listed in `internal/config/bankmw.go`.
