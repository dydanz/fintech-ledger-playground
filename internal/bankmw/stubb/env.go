package stubb

import (
	"errors"
	"fmt"
)

// ConfigFromEnv reads S:B's credentials. Secrets have no defaults.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	var errs []error
	need := func(k string) string {
		v := getenv(k)
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", k))
		}
		return v
	}
	cfg := Config{BaseURL: need("BANK_B_BASE_URL"), ClientID: need("BANK_B_CLIENT_ID"), ClientSecret: need("BANK_B_CLIENT_SECRET"),
		APIKey: need("BANK_B_API_KEY"), APISecret: need("BANK_B_API_SECRET"), CorporateID: need("BANK_B_CORPORATE_ID"),
		SourceAccount: need("BANK_B_SOURCE_ACCOUNT")}
	return cfg, errors.Join(errs...)
}
