// Package contract holds the canonical, bank-agnostic models shared by the
// payout adapter, money-out-middleware and the bank middlewares
// (spec/money-out TRD §3): money on the wire, canonical statuses and reasons,
// the event envelope and the event payloads. It knows no bank.
package contract

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"fintech-ledger-playground/internal/ledger"
)

var ErrInvalidMoney = errors.New("contract: invalid amount")

// Amount is money on the wire: a decimal string and an ISO 4217 code (P§2.1).
type Amount struct {
	Value    string `json:"value"`
	Currency string `json:"currency"`
}

// MoneyRules are the currencies accepted, with their exponents, and the
// per-transaction maximum in minor units (the core's LEDGER_MAX_AMOUNT).
type MoneyRules struct {
	MaxAmount int64
	Exponents map[string]int
}

// DefaultMoneyRules matches the core's defaults: IDR only, exponent 0 (TRD
// L§4.1 A2), at most 1,000,000,000,000.
var DefaultMoneyRules = MoneyRules{MaxAmount: 1_000_000_000_000, Exponents: map[string]int{"IDR": 0}}

// wireDecimal is a positive decimal with at most two places, no sign, no
// exponent, no separators, no leading zero.
var wireDecimal = regexp.MustCompile(`^[1-9][0-9]*(\.[0-9]{1,2})?$`)

// Parse turns a wire decimal into minor units with integer arithmetic only. A
// fraction finer than the currency's exponent must be zero.
func (r MoneyRules) Parse(value, currency string) (ledger.Money, error) {
	exp, ok := r.Exponents[currency]
	if !ok {
		return ledger.Money{}, fmt.Errorf("%w: currency %q is not accepted", ErrInvalidMoney, currency)
	}
	if !wireDecimal.MatchString(value) {
		return ledger.Money{}, fmt.Errorf("%w: %q is not a positive decimal with at most 2 places", ErrInvalidMoney, value)
	}
	whole, frac, _ := strings.Cut(value, ".")
	frac += strings.Repeat("0", 2-len(frac)) // always two places now
	if strings.TrimRight(frac[exp:], "0") != "" {
		return ledger.Money{}, fmt.Errorf("%w: %s has no minor unit finer than %d decimals", ErrInvalidMoney, currency, exp)
	}
	digits := whole + frac[:exp]
	if len(digits) > 18 { // keeps ParseInt far from overflow; max amounts are far smaller
		return ledger.Money{}, fmt.Errorf("%w: %q is out of range", ErrInvalidMoney, value)
	}
	n, err := strconv.ParseInt(digits, 10, 64)
	if err != nil {
		return ledger.Money{}, fmt.Errorf("%w: %q", ErrInvalidMoney, value)
	}
	if err := ledger.ValidateAmount(n, r.MaxAmount); err != nil {
		return ledger.Money{}, fmt.Errorf("%w: %q is out of range", ErrInvalidMoney, value)
	}
	return ledger.Money{Amount: n, Currency: currency}, nil
}

// Format renders minor units as a two-place wire decimal.
func (r MoneyRules) Format(m ledger.Money) string {
	exp := r.Exponents[m.Currency]
	s := strconv.FormatInt(m.Amount, 10)
	if exp == 0 {
		return s + ".00"
	}
	if len(s) <= exp {
		s = strings.Repeat("0", exp-len(s)+1) + s
	}
	whole, frac := s[:len(s)-exp], s[len(s)-exp:]
	return whole + "." + (frac + "00")[:2]
}

// ParseMoney and FormatMoney use DefaultMoneyRules.
func ParseMoney(value, currency string) (ledger.Money, error) {
	return DefaultMoneyRules.Parse(value, currency)
}

func FormatMoney(m ledger.Money) string { return DefaultMoneyRules.Format(m) }

// Money parses a under DefaultMoneyRules.
func (a Amount) Money() (ledger.Money, error) { return ParseMoney(a.Value, a.Currency) }

// AmountOf renders m for the wire under DefaultMoneyRules.
func AmountOf(m ledger.Money) Amount { return Amount{Value: FormatMoney(m), Currency: m.Currency} }
