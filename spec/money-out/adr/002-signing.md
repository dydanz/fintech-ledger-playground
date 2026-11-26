# ADR-002: Signing MOM's requests to the bank middlewares

**Status:** accepted (2026-11-25) · **Context:** P§2.3, TRD §3.5 · **Code:** `internal/moneyout/signing` (M1.2)

## Context

A bank middleware moves real money when asked. Two things must hold:
- only MOM can produce a request it accepts;
- a captured request can't be replayed.

A BMW has no database.

## Decision

- **Algorithm:** **Ed25519**, as a **detached JWS** (RFC 7515 Appendix F) in `X-MOM-Signature` with header `{"alg":"EdDSA","kid":…}`.
- **Signed bytes:** `METHOD \n PATH \n X-MOM-Timestamp \n X-MOM-Nonce \n base64url(SHA-256(body))`.
- **Keys:** MOM holds the private key (`MOM_SIGNING_KEY`, a 32-byte seed from a secret). BMWs hold only public keys (`MOM_PUBLIC_KEYS`, `kid=key` pairs), so two keys can be live during rotation.
- **Freshness:** a timestamp window of ±5 min (`MOM_SIGNATURE_SKEW`), inclusive. A per-process nonce cache refuses replays inside the window.
- **Two rules in the verifier:**
  - A nonce is recorded only after its signature verifies.
  - A nonce still inside the window is never evicted. The cache size is a soft cap, reported, not enforced.

## Consequences

- A BMW can verify but never forge, and nothing but MOM can sign.
- A request replayed to a **different** BMW replica would pass that replica's cache. Each replica's cache is its own, because a BMW has no database. Mitigations:
  - the window is short;
  - the body carries `request_id`, and MOM's attempt record makes a duplicate harmless at MOM's end;
  - the banks' own idempotency on the reference is a further guard.

  A shared nonce store would remove this. **Risk accepted (2026-11-26):** per-replica caches stay, and no shared store is built.
- Standard library only (`crypto/ed25519`). HMAC was rejected: a shared secret would let any BMW forge requests to the others.
