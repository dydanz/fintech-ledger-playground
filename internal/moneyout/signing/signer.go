// Package signing signs money-out-middleware's requests to the bank
// middlewares and verifies them (spec/money-out TRD §3.5, ADR-002): Ed25519,
// a detached JWS (RFC 7515 Appendix F) over the method, path, timestamp,
// nonce and the body's SHA-256. A bank middleware holds only public keys, so
// it can verify but never forge; the nonce cache refuses replays within the
// timestamp window.
package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// The request headers that carry the signature.
const (
	HeaderTimestamp = "X-MOM-Timestamp"
	HeaderNonce     = "X-MOM-Nonce"
	HeaderSignature = "X-MOM-Signature"
)

var b64 = base64.RawURLEncoding

type jwsHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
}

// Signer signs requests with one key, named by kid.
type Signer struct {
	kid string
	key ed25519.PrivateKey
}

func NewSigner(kid string, key ed25519.PrivateKey) *Signer { return &Signer{kid: kid, key: key} }

// Sign returns the three headers for a request, signed at now.
func (s *Signer) Sign(method, path string, body []byte, now time.Time) (http.Header, error) {
	n := make([]byte, 16)
	if _, err := rand.Read(n); err != nil {
		return nil, fmt.Errorf("signing: nonce: %w", err)
	}
	ts, nonce := now.UTC().Format(time.RFC3339), b64.EncodeToString(n)
	hdr, err := json.Marshal(jwsHeader{Alg: "EdDSA", Kid: s.kid})
	if err != nil {
		return nil, err
	}
	protected := b64.EncodeToString(hdr)
	sig := ed25519.Sign(s.key, []byte(protected+"."+b64.EncodeToString(signingInput(method, path, ts, nonce, body))))
	h := http.Header{}
	h.Set(HeaderTimestamp, ts)
	h.Set(HeaderNonce, nonce)
	h.Set(HeaderSignature, protected+".."+b64.EncodeToString(sig))
	return h, nil
}

// signingInput is the detached payload: METHOD \n PATH \n timestamp \n nonce
// \n base64url(SHA-256(body)).
func signingInput(method, path, ts, nonce string, body []byte) []byte {
	sum := sha256.Sum256(body)
	return []byte(strings.Join([]string{method, path, ts, nonce, b64.EncodeToString(sum[:])}, "\n"))
}

// EncodeKey renders a public key for MOM_PUBLIC_KEYS.
func EncodeKey(k ed25519.PublicKey) string { return base64.StdEncoding.EncodeToString(k) }

// ParsePublicKeys reads MOM_PUBLIC_KEYS: comma-separated kid=base64 pairs.
func ParsePublicKeys(s string) (map[string]ed25519.PublicKey, error) {
	keys := map[string]ed25519.PublicKey{}
	for _, part := range strings.Split(s, ",") {
		kid, enc, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || kid == "" {
			return nil, fmt.Errorf("signing: public key entry must be kid=base64")
		}
		raw, err := base64.StdEncoding.DecodeString(enc)
		if err != nil || len(raw) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("signing: public key %q must be %d bytes of base64", kid, ed25519.PublicKeySize)
		}
		keys[kid] = ed25519.PublicKey(raw)
	}
	return keys, nil
}

// ParsePrivateKey reads MOM_SIGNING_KEY: the base64 32-byte seed.
func ParsePrivateKey(s string) (ed25519.PrivateKey, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(raw) != ed25519.SeedSize {
		return nil, fmt.Errorf("signing: the signing key must be a %d-byte base64 seed", ed25519.SeedSize)
	}
	return ed25519.NewKeyFromSeed(raw), nil
}
