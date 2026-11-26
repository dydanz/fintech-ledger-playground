package bankmw_test

import (
	"testing"

	"fintech-ledger-playground/internal/bankmw"
)

// T§9: only the last 4 digits of an account number are ever logged.
func TestMask(t *testing.T) {
	for in, want := range map[string]string{"0201245681": "******5681", "4110029384": "******9384", "1234": "****", "12": "**", "": ""} {
		if got := bankmw.Mask(in); got != want {
			t.Errorf("Mask(%q) = %q, want %q", in, got, want)
		}
	}
}
