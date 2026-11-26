# bank-middleware-b: Bank-Stub-B (corporate host-to-host, sync)

`cmd/bank-middleware-b` adapts S:B (`spec/2026-11-25 bank-api-stubs.md`) to the canonical money-out contract. It's stateless, and accepts only MOM-signed requests (`internal/bankmw`). The balance and bulk routes aren't used (P§1.3).

## Flow

1. **OAuth token.** `POST /api/oauth/token`, with Basic auth and `grant_type=client_credentials`. The token is cached until `expires_in` minus 30 s.
2. **Beneficiary inquiry.** `POST /banking/v3/corporates/transfers/inquiry`.
3. **Single transfer.** `POST /banking/corporates/transfers`. The amount is a decimal string with two places.
4. **Status** (`InquireStatus`): `POST /banking/v3/corporates/transfers/status`.

Calls 2 to 4 are signed with `X-BCA-Key`, `X-BCA-Timestamp` (WIB, milliseconds) and `X-BCA-Signature`. The signature is `Base64(HMAC-SHA256(apiSecret, METHOD:RelativeUrl:AccessToken:lowercase(hex(SHA-256(minified body))):Timestamp))`, BCA's published form; the contract doesn't define it (T§12.1 #2). A `401` refreshes the token and retries once.

## Addressing: intrabank to 014 only

S:B's single transfer has **no beneficiary bank code** (T§12.1 #8). A transfer always credits that account number at bank B.
- So this adapter takes only `rail = INTRABANK` with `bank_code = 014`.
- Anything else is refused before any call: FAILED `RAIL_UNAVAILABLE`, `retry_elsewhere` true, not reached.
- **This narrows T§11.4's draft, which also listed `RTOL` (A).** Sending RTOL here would pay the wrong account.

## Identity at the bank (A)

The BMW keeps no state, so it derives everything from `request_id`:

| S:B field | Value |
| :--- | :--- |
| `TransactionID` (8 characters) | base36 of SHA-256(`request_id`). About 2.8 × 10¹² values. |
| `ReferenceID` | `request_id` |
| `TransactionDate` | The date in WIB of the UUIDv7 `request_id`'s timestamp, so the transfer and a later status inquiry name the same day. Otherwise, today. |
| `InquiryRef` | The same as `TransactionID` |

**Residual risk:**
- Two attempts whose `TransactionID`s collide: the second transfer gets a duplicate refusal (409, so `UNKNOWN`).
- Its status inquiry would then read the *first* transfer.
- That's unlikely at this scale. A real integration would ask the bank for a longer ID field.

## Status mapping (T§11.2)

**(A)** marks an assumption: the contract shows no error body or failure codes (Q7). **(BMW)** marks a rule this adapter adds. Nothing is `FAILED` unless no money can have moved.

| Step and bank answer | Canonical | `reason`, `retry_elsewhere`, `reached_bank` |
| :--- | :--- | :--- |
| Inquiry `AccountStatus: ACTIVE` | continue | — |
| Inquiry `AccountStatus: CLOSED` **(A)**, `DORMANT` **(A)** | FAILED | `ACCOUNT_CLOSED`, false, true |
| Inquiry: any other `AccountStatus` **(BMW)** | FAILED | `INVALID_ACCOUNT`, false, true |
| Inquiry `ErrorCode` `ESB-82-001` / `ESB-82-002` **(A)** | FAILED | `INVALID_ACCOUNT` / `ACCOUNT_CLOSED`, false, true |
| Inquiry: any other error, 5xx or timeout **(BMW)** | FAILED | `RAIL_UNAVAILABLE`, true, false (no transfer was sent) |
| Transfer `Status: Success` | SUCCEEDED | `bank_reference = TransactionID` |
| Transfer 4xx `ErrorCode` **(A)**: `ESB-82-001` invalid account, `ESB-82-002` closed, `ESB-14-021` insufficient funds, `ESB-14-022` limit, `ESB-99-009` unavailable | FAILED | `INVALID_ACCOUNT` false; `ACCOUNT_CLOSED` false; `INSUFFICIENT_SOURCE_FUNDS` true; `LIMIT_EXCEEDED` true; `RAIL_UNAVAILABLE` true; reached |
| Transfer timeout, 5xx (incl. `504 ESB-99-504` in progress), `409` duplicate, or an unmapped code | UNKNOWN | — (the status inquiry decides) |
| `401` at any step | refresh the token, retry once | if it repeats: FAILED `AUTH_ERROR`, true, false |
| Token refused / token endpoint unreachable, or connection refused at the transfer | FAILED | `AUTH_ERROR` / `RAIL_UNAVAILABLE`, true, false |
| Status `SettlementState: SETTLED` | SUCCEEDED | `bank_reference = BankSettlementRef` |
| Status `PENDING` **(A)** | PROCESSING | — |
| Status `REJECTED` with `FailureReason` **(A)** | FAILED | `ACCOUNT_CLOSED`/`ACCOUNT_DORMANT` → `ACCOUNT_CLOSED`; `INVALID_ACCOUNT` → `INVALID_ACCOUNT`; anything else → `BANK_REJECTED`. `retry_elsewhere` is the T§3.6 default (false for all three). Reached. |
| Status `404 ESB-82-404` not found **(A)** | FAILED (doesn't exist) | `OTHER`, true, false |
| Status: any other answer, 5xx or timeout | UNKNOWN | — |

`bank_raw_code` carries `ErrorCode`, `AccountStatus`, `Status`, `SettlementState` or `FailureReason`, unchanged.

## Configuration

These are all required, with no defaults in code:
- `BANK_B_BASE_URL`;
- `BANK_B_CLIENT_ID`, `BANK_B_CLIENT_SECRET` (OAuth);
- `BANK_B_API_KEY`, `BANK_B_API_SECRET` (`X-BCA-*`);
- `BANK_B_CORPORATE_ID`;
- `BANK_B_SOURCE_ACCOUNT`.

The shared ones are listed in `internal/config/bankmw.go`.
