package signing_test

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/moneyout/signing"
)

var now = time.Date(2026, 11, 25, 2, 15, 0, 0, time.UTC)

func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

const path = "/internal/v1/transfers"

var body = []byte(`{"request_id":"r1","transfer_id":"t1","amount":{"value":"2500000.00","currency":"IDR"}}`)

// signed builds a request signed at signAt.
func signed(t *testing.T, s *signing.Signer, b []byte, signAt time.Time) *http.Request {
	t.Helper()
	h, err := s.Sign(http.MethodPost, path, b, signAt)
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(b))
	for k, v := range h {
		r.Header[k] = v
	}
	return r
}

func TestSignVerifyRoundTrip(t *testing.T) {
	pub, priv := newKey(t)
	s := signing.NewSigner("mom-1", priv)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000)
	r := signed(t, s, body, now)
	for _, h := range []string{signing.HeaderTimestamp, signing.HeaderNonce, signing.HeaderSignature} {
		if r.Header.Get(h) == "" {
			t.Fatalf("no %s header", h)
		}
	}
	if !strings.Contains(r.Header.Get(signing.HeaderSignature), "..") {
		t.Fatalf("signature %q isn't a detached JWS (header..signature)", r.Header.Get(signing.HeaderSignature))
	}
	if err := v.Verify(r, body, now); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// P§5 #8: a body changed after signing is refused.
func TestTamperedBodyRejected(t *testing.T) {
	pub, priv := newKey(t)
	s := signing.NewSigner("mom-1", priv)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000)
	r := signed(t, s, body, now)
	tampered := bytes.Replace(body, []byte("2500000.00"), []byte("9500000.00"), 1)
	if err := v.Verify(r, tampered, now); !errors.Is(err, signing.ErrBadSignature) {
		t.Fatalf("tampered body: %v, want ErrBadSignature", err)
	}
	// So is a changed path, method or timestamp header.
	r2 := signed(t, s, body, now)
	r2.URL.Path = "/internal/v1/transfers/status"
	if err := v.Verify(r2, body, now); !errors.Is(err, signing.ErrBadSignature) {
		t.Fatalf("changed path: %v", err)
	}
	r3 := signed(t, s, body, now)
	r3.Header.Set(signing.HeaderTimestamp, now.Add(time.Second).Format(time.RFC3339))
	if err := v.Verify(r3, body, now); !errors.Is(err, signing.ErrBadSignature) {
		t.Fatalf("changed timestamp: %v", err)
	}
}

// P§5 #8: the same signed request twice is refused the second time.
func TestReplayRejected(t *testing.T) {
	pub, priv := newKey(t)
	s := signing.NewSigner("mom-1", priv)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000)
	r := signed(t, s, body, now)
	if err := v.Verify(r, body, now); err != nil {
		t.Fatal(err)
	}
	again := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	again.Header = r.Header.Clone()
	if err := v.Verify(again, body, now.Add(time.Second)); !errors.Is(err, signing.ErrReplay) {
		t.Fatalf("replay: %v, want ErrReplay", err)
	}
}

// P§5 #8: signed with a key the verifier doesn't hold, under a known or an
// unknown kid, is refused.
func TestOtherKeyRejected(t *testing.T) {
	pub, _ := newKey(t)
	_, other := newKey(t)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000)
	if err := v.Verify(signed(t, signing.NewSigner("mom-1", other), body, now), body, now); !errors.Is(err, signing.ErrBadSignature) {
		t.Fatalf("known kid, other key: %v", err)
	}
	if err := v.Verify(signed(t, signing.NewSigner("mom-9", other), body, now), body, now); !errors.Is(err, signing.ErrBadSignature) {
		t.Fatalf("unknown kid: %v", err)
	}
}

func TestMissingOrMalformedHeadersRejected(t *testing.T) {
	pub, priv := newKey(t)
	s := signing.NewSigner("mom-1", priv)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000)
	for name, mut := range map[string]func(http.Header){
		"no signature":  func(h http.Header) { h.Del(signing.HeaderSignature) },
		"no nonce":      func(h http.Header) { h.Del(signing.HeaderNonce) },
		"no timestamp":  func(h http.Header) { h.Del(signing.HeaderTimestamp) },
		"bad timestamp": func(h http.Header) { h.Set(signing.HeaderTimestamp, "yesterday") },
		"garbage jws":   func(h http.Header) { h.Set(signing.HeaderSignature, "not-a-jws") },
		"attached jws": func(h http.Header) {
			h.Set(signing.HeaderSignature, strings.Replace(h.Get(signing.HeaderSignature), "..", ".eyJ4Ijox.", 1))
		},
		"alg none": func(h http.Header) { h.Set(signing.HeaderSignature, "eyJhbGciOiJub25lIiwia2lkIjoibW9tLTEifQ..") },
	} {
		r := signed(t, s, body, now)
		mut(r.Header)
		if err := v.Verify(r, body, now); !errors.Is(err, signing.ErrBadSignature) && !errors.Is(err, signing.ErrStale) {
			t.Errorf("%s: %v, want a rejection", name, err)
		}
	}
}

// Review Focus 5: ±skew is accepted, one second past it is stale, in either
// direction; and the cache never forgets a nonce while it's inside the window,
// even past its size cap.
func TestVerifierWindowEdges(t *testing.T) {
	pub, priv := newKey(t)
	s := signing.NewSigner("mom-1", priv)
	keys := map[string]ed25519.PublicKey{"mom-1": pub}
	v := signing.NewVerifier(keys, 5*time.Minute, 1000)
	for _, d := range []time.Duration{-5 * time.Minute, 5 * time.Minute} {
		if err := v.Verify(signed(t, s, body, now.Add(d)), body, now); err != nil {
			t.Errorf("signed %v from now: %v, want accepted", d, err)
		}
	}
	for _, d := range []time.Duration{-5*time.Minute - time.Second, 5*time.Minute + time.Second} {
		if err := v.Verify(signed(t, s, body, now.Add(d)), body, now); !errors.Is(err, signing.ErrStale) {
			t.Errorf("signed %v from now: %v, want ErrStale", d, err)
		}
	}

	over := 0
	small := signing.NewVerifier(keys, 5*time.Minute, 2)
	small.OverCap = func(int) { over++ }
	var reqs []*http.Request
	for i := 0; i < 3; i++ {
		r := signed(t, s, body, now)
		reqs = append(reqs, r)
		if err := small.Verify(r, body, now); err != nil {
			t.Fatal(err)
		}
	}
	for i, r := range reqs {
		again := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		again.Header = r.Header.Clone()
		if err := small.Verify(again, body, now.Add(time.Minute)); !errors.Is(err, signing.ErrReplay) {
			t.Errorf("nonce %d replayed inside the window: %v, want ErrReplay", i, err)
		}
	}
	if over != 1 {
		t.Errorf("OverCap called %d times, want once when the cap was first passed", over)
	}
	// Past the window the nonces are swept: the cache doesn't grow forever.
	if err := small.Verify(signed(t, s, body, now.Add(11*time.Minute)), body, now.Add(11*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if n := small.CacheLen(); n != 1 {
		t.Errorf("cache holds %d nonces after the window passed, want 1", n)
	}
}

// Rotation: requests under either of two live kids verify.
func TestKeyRotation(t *testing.T) {
	pub1, priv1 := newKey(t)
	pub2, priv2 := newKey(t)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub1, "mom-2": pub2}, 5*time.Minute, 1000)
	for kid, priv := range map[string]ed25519.PrivateKey{"mom-1": priv1, "mom-2": priv2} {
		if err := v.Verify(signed(t, signing.NewSigner(kid, priv), body, now), body, now); err != nil {
			t.Errorf("%s: %v", kid, err)
		}
	}
}

// A rejected signature doesn't use up the nonce: the genuine request carrying
// it still verifies, so a forger can't burn nonces or fill the cache.
func TestBadSignatureDoesNotConsumeNonce(t *testing.T) {
	pub, priv := newKey(t)
	s := signing.NewSigner("mom-1", priv)
	v := signing.NewVerifier(map[string]ed25519.PublicKey{"mom-1": pub}, 5*time.Minute, 1000)
	r := signed(t, s, body, now)
	forged := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	forged.Header = r.Header.Clone()
	if err := v.Verify(forged, []byte(`{"tampered":true}`), now); !errors.Is(err, signing.ErrBadSignature) {
		t.Fatal(err)
	}
	if err := v.Verify(r, body, now); err != nil {
		t.Fatalf("genuine request after a forged one with its nonce: %v", err)
	}
	if v.CacheLen() != 1 {
		t.Fatalf("cache = %d", v.CacheLen())
	}
}

func TestParsePublicKeys(t *testing.T) {
	pub, _ := newKey(t)
	keys, err := signing.ParsePublicKeys("mom-1=" + signing.EncodeKey(pub))
	if err != nil || !keys["mom-1"].Equal(pub) {
		t.Fatalf("ParsePublicKeys: %v %v", keys, err)
	}
	for _, bad := range []string{"", "mom-1", "mom-1=!!", "mom-1=AAAA"} {
		if _, err := signing.ParsePublicKeys(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
}

func TestParsePrivateKey(t *testing.T) {
	_, priv := newKey(t)
	got, err := signing.ParsePrivateKey(base64Std(priv.Seed()))
	if err != nil || !got.Equal(priv) {
		t.Fatalf("ParsePrivateKey: %v", err)
	}
	if _, err := signing.ParsePrivateKey("c2hvcnQ="); err == nil {
		t.Fatal("a short seed parsed")
	}
}

func base64Std(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
