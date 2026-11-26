# Bank simulators (test only)

`cmd/bank-simulator` serves one of the four bank contracts in
`spec/2026-11-25 bank-api-stubs.md`, picked by `SIM_BANK=a|b|c|d`. Each one
verifies its bank's own signature exactly as the contract says, keeps
transfers in memory, and is idempotent on its bank's reference. Bulk and OTP
approval routes are out of scope (P§1.3).

Compose (profile `money-out`): `bank-sim-a` … `bank-sim-d` on
`127.0.0.1:8191` … `8194`. Secrets come from the environment and have dev
defaults only in `docker-compose.yml`. S:A's RSA partner key pair is
generated at start-up by `money-out-keys-init` into the `moneyoutkeys` volume.

## Control API

| Route | What |
| :--- | :--- |
| `POST /_sim/faults` | Add a fault (below). `201` with its id. |
| `GET /_sim/faults` | The faults still pending |
| `DELETE /_sim/faults` | Remove every fault |
| `GET /_sim/calls` | Calls per step that passed the bank's auth, e.g. `{"transfer":1}` |
| `POST /_sim/reset` | Faults, call counts and the bank's in-memory transfers |

```json
{"match": {"step": "transfer", "account_no": "0201245681"},
 "action": "status", "http_status": 403, "code": "4031814", "times": 1}
```

- **`match.step`**: one of the bank's steps (below). `account_no` is optional; empty matches any account.
- **`times`**: how many requests the fault applies to. The default is 1.
- **`action`**:

| Action | Fields | Behaviour |
| :--- | :--- | :--- |
| `delay` | `delay_ms` | Wait, then answer normally |
| `timeout` | `delay_ms` (the longest hold, default 30 s) | **Process the request**, then hold until the caller gives up. The money moved, and only the status route can tell. |
| `status` | `http_status` (default 200), `code` | Answer with the bank's error for `code`, **without processing** |
| `pending_then_success` | `polls` (default 1; `-1` forever) | Accept as pending. `polls` status replies stay pending, then success. |
| `pending_then_fail` | `polls`, `code` | The same, then the bank's failure |
| `drop_webhook` | – | S:C only: never send this webhook |

## Steps and what `code` means

| Bank | Steps (pending step in bold) | `status` code | Pending and failure |
| :--- | :--- | :--- | :--- |
| A (BI-SNAP) | `token`, `inquiry`, **`transfer`**, `status` | The SNAP `responseCode`, e.g. `4041611`, `4031802`, `4031814`. Default: `<http>` + service code + `00`. | Transfer `202 2021800`. Status `03` while pending, then `00` or `06`. An unknown reference is `07`. |
| B (Corporate H2H) | `token`, `balance`, `inquiry`, **`transfer`**, `status` | `ErrorCode` (A). At `inquiry` with `http_status` 200, the `AccountStatus`, e.g. `CLOSED`, `DORMANT`. | Transfer `504 ESB-99-504`. Status `PENDING`, then `SETTLED`, or `REJECTED` with `FailureReason` = `code` (default `REJECTED_BY_BANK`). Unknown: `404 ESB-82-404`. |
| C (async multi-ACK) | `initiate`, `ack-1`, `aaa-verify`, `ack-2`, **`execute`**, `status` | At `ack-2`: `ACCT_INVALID`, `ACCT_CLOSED`, `REQUEST_INVALID` (an ACK-2 refusal, no token). Elsewhere, the `error` field. | Execute `202 PROCESSING`. Poll `PROCESSING`, then `SETTLED` or `FAILED`. |
| D (multi-rail) | `acct-validate`, `funding-check`, **`remit-single`**, `remit-status` | At `acct-validate` with 200, `meta.status-code`, e.g. `14` `ACCOUNT_NOT_FOUND`. At `funding-check`: `WINDOW_CLOSED` or `LOW_LIQUIDITY`. Non-200: `meta.status-code` (default `99`). | Remit is always `IN_CLEARING`. With no fault, one status poll shows `IN_CLEARING`, then `SETTLED`. With a fault: `REJECTED`, or `RETURNED` when `code` is `RETURNED`. |

S:C's `final_state` values beyond `SETTLED` are assumptions (T§11.3):
- `AWAITING_ACK_1`, `AWAITING_AAA_VERIFY`, `VALIDATING_ACCOUNT_FOR_ACK_2`;
- `AWAITING_EXECUTE`, `PROCESSING`;
- `FAILED`, `EXPIRED` (a deadline passed before execute).

Every code beyond the contracts' success examples is an (A) row in T§11 (Q7).
