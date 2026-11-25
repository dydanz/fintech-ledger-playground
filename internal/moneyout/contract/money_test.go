package contract_test

import (
	"testing"

	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

func TestParseMoneyAccepts(t *testing.T) {
	for in, want := range map[string]int64{
		"2500000.00":       2500000,
		"2500000":          2500000,
		"2500000.0":        2500000,
		"1":                1,
		"1000000000000.00": 1_000_000_000_000, // LEDGER_MAX_AMOUNT
	} {
		got, err := contract.ParseMoney(in, "IDR")
		if err != nil || got != (ledger.Money{Amount: want, Currency: "IDR"}) {
			t.Errorf("ParseMoney(%q) = %+v, %v; want %d IDR", in, got, err, want)
		}
	}
}

// Review Focus 2: anything that isn't a plain positive decimal in range, in a
// configured currency, is refused before it's stored.
func TestParseMoneyRejects(t *testing.T) {
	for _, c := range []struct{ value, currency string }{
		{"1e6", "IDR"},
		{"-5", "IDR"},
		{"0", "IDR"},
		{"0.00", "IDR"},
		{"2500000.5", "IDR"}, // IDR has exponent 0: a fraction isn't representable
		{"2500000.01", "IDR"},
		{"2500000.001", "IDR"}, // more decimals than the wire format allows
		{"2500000.00000", "IDR"},
		{" 1", "IDR"},
		{"1 ", "IDR"},
		{"1,000", "IDR"},
		{"01", "IDR"},
		{"+1", "IDR"},
		{".5", "IDR"},
		{"1.", "IDR"},
		{"", "IDR"},
		{"1000000000001", "IDR"},        // above LEDGER_MAX_AMOUNT
		{"99999999999999999999", "IDR"}, // overflows int64
		{"100", "USD"},                  // not configured
		{"100", "idr"},
	} {
		if got, err := contract.ParseMoney(c.value, c.currency); err == nil {
			t.Errorf("ParseMoney(%q, %q) = %+v; want an error", c.value, c.currency, got)
		}
	}
}

func TestFormatMoney(t *testing.T) {
	if got := contract.FormatMoney(ledger.Money{Amount: 2500000, Currency: "IDR"}); got != "2500000.00" {
		t.Fatalf("FormatMoney = %q, want 2500000.00", got)
	}
}

// Rules with an exponent-2 currency: the fraction is kept, the wire format
// still has two decimals.
func TestMoneyRulesWithMinorUnits(t *testing.T) {
	r := contract.MoneyRules{MaxAmount: 1_000_000, Exponents: map[string]int{"USD": 2}}
	m, err := r.Parse("12.34", "USD")
	if err != nil || m.Amount != 1234 {
		t.Fatalf("Parse(12.34 USD) = %+v, %v; want 1234", m, err)
	}
	if got := r.Format(m); got != "12.34" {
		t.Fatalf("Format = %q", got)
	}
	if _, err := r.Parse("10000.01", "USD"); err == nil {
		t.Fatal("1,000,001 minor units passed a 1,000,000 max")
	}
}

func TestAmountRoundTrip(t *testing.T) {
	a := contract.Amount{Value: "2500000.00", Currency: "IDR"}
	m, err := a.Money()
	if err != nil || m.Amount != 2500000 {
		t.Fatalf("Money() = %+v, %v", m, err)
	}
	if back := contract.AmountOf(m); back != a {
		t.Fatalf("AmountOf = %+v, want %+v", back, a)
	}
}
