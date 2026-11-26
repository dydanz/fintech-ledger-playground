package signing

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	ErrBadSignature = errors.New("signing: missing or invalid signature")
	ErrStale        = errors.New("signing: timestamp outside the allowed window")
	ErrReplay       = errors.New("signing: nonce already used")
)

// Verifier checks signed requests against a set of public keys.
type Verifier struct {
	keys map[string]ed25519.PublicKey
	skew time.Duration
	// OverCap, when set, is called once each time the nonce cache first grows
	// past its size cap. The cache keeps every nonce inside the window
	// regardless: forgetting one would allow its replay.
	OverCap func(size int)

	mu     sync.Mutex
	nonces *nonceCache
}

func NewVerifier(keys map[string]ed25519.PublicKey, skew time.Duration, cacheSize int) *Verifier {
	return &Verifier{keys: keys, skew: skew, nonces: newNonceCache(cacheSize)}
}

// Verify checks r's signature over body at now: the headers, the timestamp
// window, the key and signature, and then the nonce. A nonce is recorded only
// once the signature is valid, so a forger can't use up or flood the cache.
func (v *Verifier) Verify(r *http.Request, body []byte, now time.Time) error {
	ts, nonce, sig := r.Header.Get(HeaderTimestamp), r.Header.Get(HeaderNonce), r.Header.Get(HeaderSignature)
	if ts == "" || nonce == "" || sig == "" {
		return fmt.Errorf("%w: missing header", ErrBadSignature)
	}
	at, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return fmt.Errorf("%w: timestamp: %v", ErrBadSignature, err)
	}
	if d := now.Sub(at); d > v.skew || d < -v.skew {
		return ErrStale
	}
	protected, signature, ok := splitDetached(sig)
	if !ok {
		return fmt.Errorf("%w: not a detached JWS", ErrBadSignature)
	}
	var h jwsHeader
	hb, err := b64.DecodeString(protected)
	if err != nil || json.Unmarshal(hb, &h) != nil || h.Alg != "EdDSA" {
		return fmt.Errorf("%w: header", ErrBadSignature)
	}
	key, known := v.keys[h.Kid]
	raw, err := b64.DecodeString(signature)
	if !known || err != nil {
		return fmt.Errorf("%w: unknown key or bad encoding", ErrBadSignature)
	}
	payload := b64.EncodeToString(signingInput(r.Method, r.URL.Path, ts, nonce, body))
	if !ed25519.Verify(key, []byte(protected+"."+payload), raw) {
		return ErrBadSignature
	}

	v.mu.Lock()
	defer v.mu.Unlock()
	fresh, crossed := v.nonces.add(nonce, at.Add(v.skew), now)
	if crossed && v.OverCap != nil {
		v.OverCap(v.nonces.len())
	}
	if !fresh {
		return ErrReplay
	}
	return nil
}

// CacheLen is the number of nonces held.
func (v *Verifier) CacheLen() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.nonces.len()
}

// splitDetached splits "header..signature": the payload part must be empty.
func splitDetached(s string) (protected, signature string, ok bool) {
	parts := strings.Split(s, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] != "" || parts[2] == "" {
		return "", "", false
	}
	return parts[0], parts[2], true
}
