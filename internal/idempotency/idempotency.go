// Package idempotency derives the stored forms of an Idempotency-Key and of
// the request it guards (spec/TRD §6.4). The raw key is never stored or logged.
package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
)

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,255}$`)

// ValidKey reports whether k is an acceptable Idempotency-Key header value.
func ValidKey(k string) bool { return keyPattern.MatchString(k) }

// HashKey returns the hex SHA-256 of a key, as stored in key_hash.
func HashKey(k string) string {
	sum := sha256.Sum256([]byte(k))
	return hex.EncodeToString(sum[:])
}

// Fingerprint returns the hex SHA-256 of operation, target (e.g. the account
// id in the path) and the canonical JSON of fields. Canonical means object
// keys are sorted and insignificant whitespace is dropped, so two requests
// that differ only in key order or formatting get the same fingerprint.
func Fingerprint(operation, target string, fields any) (string, error) {
	canon, err := canonicalJSON(fields)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%s\x00", operation, target)
	h.Write(canon)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// canonicalJSON round-trips v through a generic value: encoding/json sorts
// map keys, and UseNumber keeps numbers exactly as written.
func canonicalJSON(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("idempotency: marshal fields: %w", err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("idempotency: canonicalize fields: %w", err)
	}
	return json.Marshal(generic)
}
