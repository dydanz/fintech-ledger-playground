// Package ledger is the pure domain model of the balance system (spec/TRD §4):
// money, transaction and account types, events, the state machines that fold
// event streams into state, and the settle-or-fail decision. It does no I/O.
package ledger

import (
	"errors"
	"math"
	"regexp"
)

var (
	ErrOverflow         = errors.New("ledger: amount overflows int64")
	ErrCurrencyMismatch = errors.New("ledger: currency mismatch")
	ErrInvalidCurrency  = errors.New("ledger: currency must be a 3-letter ISO 4217 code")
	ErrInvalidAmount    = errors.New("ledger: amount out of range")
)

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// Money is an amount in the currency's minor unit (IDR has exponent 0).
// There is no floating point anywhere in the ledger.
type Money struct {
	Amount   int64
	Currency string
}

// ValidCurrency reports whether code looks like an ISO 4217 code. Whether the
// ledger accepts it is a separate, configured decision (LEDGER_CURRENCIES).
func ValidCurrency(code string) bool { return currencyPattern.MatchString(code) }

// ValidateAmount checks a requested amount: 1 ≤ amount ≤ max. With max well
// below MaxInt64/2, the sum of two valid amounts can never overflow.
func ValidateAmount(amount, max int64) error {
	if amount < 1 || amount > max {
		return ErrInvalidAmount
	}
	return nil
}

// Add returns m + o, or ErrCurrencyMismatch / ErrOverflow.
func (m Money) Add(o Money) (Money, error) {
	if m.Currency != o.Currency {
		return Money{}, ErrCurrencyMismatch
	}
	sum, err := addInt64(m.Amount, o.Amount)
	if err != nil {
		return Money{}, err
	}
	return Money{Amount: sum, Currency: m.Currency}, nil
}

// Neg returns -m. MinInt64 has no positive counterpart, so it overflows.
func (m Money) Neg() (Money, error) {
	if m.Amount == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{Amount: -m.Amount, Currency: m.Currency}, nil
}

func addInt64(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}
