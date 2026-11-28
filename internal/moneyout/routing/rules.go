package routing

import (
	"context"
	"slices"
	"time"
)

// The initial rules (T§6.4). breaker_open joins them in M4.3.
func init() {
	Register("bmw_enabled", func(c *Config) (Rule, error) { return enabledRule{c}, nil })
	Register("rail_capability", func(c *Config) (Rule, error) { return capabilityRule{c}, nil })
	Register("amount_rail_limits", func(c *Config) (Rule, error) { return limitsRule{c}, nil })
	Register(RuleWindow, func(c *Config) (Rule, error) { return windowRule{c}, nil })
	Register("intrabank_preference", func(c *Config) (Rule, error) { return homeBankRule{c}, nil })
}

// enabledRule is the kill switch: a disabled bank middleware gets nothing.
type enabledRule struct{ cfg *Config }

func (enabledRule) Name() string { return "bmw_enabled" }

func (r enabledRule) Apply(_ context.Context, _ Request, in []Candidate) ([]Candidate, []Decision) {
	return filter(r.Name(), in, func(c Candidate) (bool, string) {
		return r.cfg.bmw(c.BankMiddleware).enabled(), "disabled"
	})
}

// capabilityRule keeps the rails each bank middleware supports. INTRABANK
// reaches only a beneficiary at the middleware's own bank.
type capabilityRule struct{ cfg *Config }

func (capabilityRule) Name() string { return "rail_capability" }

func (r capabilityRule) Apply(_ context.Context, req Request, in []Candidate) ([]Candidate, []Decision) {
	return filter(r.Name(), in, func(c Candidate) (bool, string) {
		b := r.cfg.bmw(c.BankMiddleware)
		switch {
		case !slices.Contains(b.Rails, c.Rail):
			return false, "rail_not_supported"
		case c.Rail == RailIntrabank && b.HomeBank != req.BankCode:
			return false, "intrabank_other_bank"
		}
		return true, ""
	})
}

// limitsRule applies each rail's amount limits, in the configured currency's
// minor units. Both bounds are inclusive.
type limitsRule struct{ cfg *Config }

func (limitsRule) Name() string { return "amount_rail_limits" }

func (r limitsRule) Apply(_ context.Context, req Request, in []Candidate) ([]Candidate, []Decision) {
	return filter(r.Name(), in, func(c Candidate) (bool, string) {
		if req.Currency != r.cfg.Currency {
			return false, "currency_unsupported"
		}
		l := r.cfg.Limits[c.Rail]
		switch {
		case l.Min > 0 && req.Amount < l.Min:
			return false, "below_rail_min"
		case l.Max > 0 && req.Amount > l.Max:
			return false, "above_rail_max"
		}
		return true, ""
	})
}

var wib = time.FixedZone("WIB", 7*60*60)

// windowRule applies each rail's operating window in WIB: open inclusive,
// close exclusive, weekends closed when weekdays_only.
type windowRule struct{ cfg *Config }

func (windowRule) Name() string { return RuleWindow }

func (r windowRule) Apply(_ context.Context, req Request, in []Candidate) ([]Candidate, []Decision) {
	now := req.Now.In(wib)
	minute := now.Hour()*60 + now.Minute()
	weekend := now.Weekday() == time.Saturday || now.Weekday() == time.Sunday
	return filter(r.Name(), in, func(c Candidate) (bool, string) {
		w, ok := r.cfg.Windows[c.Rail]
		if !ok {
			return true, ""
		}
		if (w.WeekdaysOnly && weekend) || minute < w.open || minute >= w.close {
			return false, "outside_window"
		}
		return true, ""
	})
}

// homeBankRule moves the candidates of the bank middleware whose own bank is
// the beneficiary's to the front, keeping every candidate and their order.
type homeBankRule struct{ cfg *Config }

func (homeBankRule) Name() string { return "intrabank_preference" }

func (r homeBankRule) Apply(_ context.Context, req Request, in []Candidate) ([]Candidate, []Decision) {
	var home, rest []Candidate
	var ds []Decision
	for _, c := range in {
		if b := r.cfg.bmw(c.BankMiddleware); b.HomeBank != "" && b.HomeBank == req.BankCode {
			home = append(home, c)
			ds = append(ds, Decision{BankMiddleware: c.BankMiddleware, Rail: c.Rail, Rule: r.Name(), Kept: true, Reason: "home_bank_preferred"})
			continue
		}
		rest = append(rest, c)
	}
	return append(home, rest...), ds
}
