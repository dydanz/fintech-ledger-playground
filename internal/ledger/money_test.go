package ledger_test

import (
	"errors"
	"math"
	"testing"

	"auth-api/internal/ledger"
)

func TestMoneyAdd(t *testing.T) {
	idr := func(a int64) ledger.Money { return ledger.Money{Amount: a, Currency: "IDR"} }
	cases := []struct {
		name string
		a, b ledger.Money
		want ledger.Money
		err  error
	}{
		{"sum", idr(5), idr(-7), idr(-2), nil},
		{"max", idr(math.MaxInt64 - 1), idr(1), idr(math.MaxInt64), nil},
		{"overflow", idr(math.MaxInt64), idr(1), ledger.Money{}, ledger.ErrOverflow},
		{"underflow", idr(math.MinInt64), idr(-1), ledger.Money{}, ledger.ErrOverflow},
		{"currency mismatch", idr(1), ledger.Money{Amount: 1, Currency: "USD"}, ledger.Money{}, ledger.ErrCurrencyMismatch},
	}
	for _, tc := range cases {
		got, err := tc.a.Add(tc.b)
		if !errors.Is(err, tc.err) || got != tc.want {
			t.Errorf("%s: got %+v, %v; want %+v, %v", tc.name, got, err, tc.want, tc.err)
		}
	}
}

func TestMoneyNeg(t *testing.T) {
	if got, err := (ledger.Money{Amount: 5, Currency: "IDR"}).Neg(); err != nil || got.Amount != -5 {
		t.Errorf("Neg(5) = %+v, %v", got, err)
	}
	if _, err := (ledger.Money{Amount: math.MinInt64, Currency: "IDR"}).Neg(); !errors.Is(err, ledger.ErrOverflow) {
		t.Errorf("Neg(MinInt64): err = %v, want ErrOverflow", err)
	}
}

func TestValidateAmount(t *testing.T) {
	const max = 1_000_000_000_000
	for _, a := range []int64{1, max} {
		if err := ledger.ValidateAmount(a, max); err != nil {
			t.Errorf("%d: %v", a, err)
		}
	}
	for _, a := range []int64{0, -1, max + 1, math.MinInt64} {
		if err := ledger.ValidateAmount(a, max); !errors.Is(err, ledger.ErrInvalidAmount) {
			t.Errorf("%d: err = %v, want ErrInvalidAmount", a, err)
		}
	}
}

func TestValidCurrency(t *testing.T) {
	for code, want := range map[string]bool{"IDR": true, "USD": true, "idr": false, "ID": false, "IDRR": false, "": false, "1DR": false} {
		if got := ledger.ValidCurrency(code); got != want {
			t.Errorf("ValidCurrency(%q) = %v, want %v", code, got, want)
		}
	}
}
