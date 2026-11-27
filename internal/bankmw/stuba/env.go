package stuba

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
)

// ConfigFromEnv reads S:A's credentials. Secrets have no defaults; the RSA
// key comes from a PKCS#8 PEM file.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	var errs []error
	need := func(k string) string {
		v := getenv(k)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
		return v
	}
	or := func(k, def string) string {
		if v := getenv(k); v != "" {
			return v
		}
		return def
	}
	cfg := Config{BaseURL: need("BANK_A_BASE_URL"), ClientKey: need("BANK_A_CLIENT_KEY"), ClientSecret: need("BANK_A_CLIENT_SECRET"),
		PartnerID: need("BANK_A_PARTNER_ID"), SourceAccount: need("BANK_A_SOURCE_ACCOUNT"),
		ChannelID: or("BANK_A_CHANNEL_ID", "95221"), DeviceID: or("BANK_A_DEVICE_ID", "bank-middleware-a")}
	if f := need("BANK_A_PRIVATE_KEY_FILE"); f != "" {
		k, err := loadKey(f)
		if err != nil {
			errs = append(errs, fmt.Errorf("BANK_A_PRIVATE_KEY_FILE: %w", err))
		}
		cfg.PrivateKey = k
	}
	return cfg, errors.Join(errs...)
}

func loadKey(path string) (*rsa.PrivateKey, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b, _ := pem.Decode(raw)
	if b == nil {
		return nil, errors.New("not PEM")
	}
	k, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return nil, err
	}
	rk, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errors.New("not an RSA key")
	}
	return rk, nil
}
