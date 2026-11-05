// Package token generates opaque bearer tokens and their storage digests.
package token

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
)

// byteLen is 256 bits of entropy.
const byteLen = 32

// EncodedLen is the length of a token produced by Generate.
var EncodedLen = base64.RawURLEncoding.EncodedLen(byteLen)

// Generate returns a cryptographically random, URL-safe opaque token.
func Generate() (string, error) {
	b := make([]byte, byteLen)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// Hash returns the hex SHA-256 digest of t. Tokens carry 256 bits of
// entropy, so a fast unsalted hash is sufficient (no brute-force risk) and
// keeps lookups deterministic.
func Hash(t string) string {
	sum := sha256.Sum256([]byte(t))
	return hex.EncodeToString(sum[:])
}

// WellFormed reports whether t looks like a token produced by Generate.
// It lets callers reject garbage before touching Redis.
func WellFormed(t string) bool {
	if len(t) != EncodedLen {
		return false
	}
	for i := 0; i < len(t); i++ {
		c := t[i]
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}
