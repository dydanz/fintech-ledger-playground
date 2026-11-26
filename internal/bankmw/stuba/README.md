# bank-middleware-a: Bank-Stub-A (BI-SNAP, sync)

`cmd/bank-middleware-a` adapts S:A (`spec/2026-11-25 bank-api-stubs.md`) to the canonical money-out contract. It's stateless, and accepts only MOM-signed requests (`internal/bankmw`).

## Flow

1. **B2B token.** `POST /v1.0/access-token/b2b`, `X-SIGNATURE = Base64(SHA256withRSA(key, clientKey|timestamp))`. The token is cached until `expiresIn` minus 30 s.
2. **Account inquiry.** `POST /interbank/snap/v1.0/account-inquiry-external`.
3. **Interbank transfer.** `POST /interbank/snap/v1.0/transfer-interbank`, with `partnerReferenceNo = request_id`. The amount is a decimal string with two places.
4. **Status** (`InquireStatus`): `POST /v1.0/transfer/status`, by `originalPartnerReferenceNo = request_id`.

Calls 2 to 4 carry the seven SNAP headers. `X-SIGNATURE = Base64(HMAC-SHA512(clientSecret, METHOD:path:token:lowercase(hex(SHA-256(minified body))):timestamp))`, and the timestamps are in WIB (+07:00). A `401` at any of them refreshes the token and retries once, because the bank refused before processing.

**Rails:** `BIFAST` and `RTOL` (T§11.4, (A)), sent as `additionalInfo.channel`. Any other rail is refused before any call.

## Status mapping (T§11.1)

**(A)** marks an assumption: the contract shows only the success path (Q7). **(BMW)** marks a rule this adapter adds where T§11.1 is silent. Each rule is conservative: nothing is ever `FAILED` unless no money can have moved.

| Step and bank answer | Canonical | `reason`, `retry_elsewhere`, `reached_bank` |
| :--- | :--- | :--- |
| Inquiry `2001600` | continue | — |
| Inquiry `4041611` invalid account **(A)** | FAILED | `INVALID_ACCOUNT`, false, true |
| Inquiry: any other answer, 5xx or timeout **(BMW)** | FAILED | `RAIL_UNAVAILABLE`, true, false. No transfer was sent. |
| Transfer `2001800` | SUCCEEDED | `bank_reference = referenceNo` |
| Transfer `202 2021800`, `403 4031816`, HTTP 500, 504, timeout | UNKNOWN | — (S:A lists these as Pending) |
| Transfer, any unmapped answer **(BMW)** | UNKNOWN | — (only the status inquiry can tell) |
| Transfer `4031802` exceeds limit **(A)** | FAILED | `LIMIT_EXCEEDED`, true, true |
| Transfer `4031814` insufficient source funds **(A)** | FAILED | `INSUFFICIENT_SOURCE_FUNDS`, true, true |
| `401` (e.g. `4011800`) at any step | refresh the token, retry once | if it repeats: FAILED `AUTH_ERROR`, true, false |
| Token refused | FAILED | `AUTH_ERROR`, true, false |
| Token endpoint unreachable, or connection refused at the transfer **(BMW)** | FAILED | `RAIL_UNAVAILABLE`, true, false (provably never sent, so MOM may retry on the same node) |
| Status `latestTransactionStatus` `00` | SUCCEEDED | — |
| Status `01`, `02`, `03` (initiated, paying, pending) **(A, from SNAP)** | PROCESSING | — |
| Status `06` failed **(A)** | FAILED | `BANK_REJECTED`, true, true |
| Status `07` not found **(A)** | FAILED (doesn't exist) | `OTHER`, true, false |
| Status: any other answer, 5xx or timeout | UNKNOWN | — |

`bank_raw_code` carries the bank's `responseCode`, or `latestTransactionStatus` on status, unchanged and opaque to MOM.

## Configuration

| Variable | |
| :--- | :--- |
| `BANK_A_BASE_URL` | e.g. `http://bank-sim-a:8090` |
| `BANK_A_CLIENT_KEY`, `BANK_A_CLIENT_SECRET`, `BANK_A_PARTNER_ID` | Credentials. Required, no defaults in code. |
| `BANK_A_PRIVATE_KEY_FILE` | PKCS#8 PEM RSA key for the token signature |
| `BANK_A_SOURCE_ACCOUNT` | `sourceAccountNo` (T§12.1 #9) |
| `BANK_A_CHANNEL_ID`, `BANK_A_DEVICE_ID` | Default to `95221` and `bank-middleware-a` |

The shared ones (`MOM_PUBLIC_KEYS[_FILE]`, `MOM_SIGNATURE_SKEW`, `BMW_BANK_TIMEOUT`, …) are listed in `internal/config/bankmw.go`.

## Logs

There's one `bank.call` per API call (step `token`, `inquiry`, `transfer` or `status`), with the HTTP status, `bank_raw_code` and duration. The account number is masked to its last 4 digits. Tokens, secrets and signatures are never logged.
