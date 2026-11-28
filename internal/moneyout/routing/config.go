package routing

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"
)

// The rails MOM knows (P§2.3). Which bank middleware offers which is config.
const (
	RailIntrabank = "INTRABANK"
)

var knownRails = []string{"BIFAST", "RTOL", "SKNBI", "RTGS", RailIntrabank}

// Config is config/money-out/routing.yaml.
type Config struct {
	Version         int               `yaml:"version"`
	Currency        string            `yaml:"currency"` // limits are in its minor units
	RailOrder       []string          `yaml:"rail_order"`
	BankMiddlewares []BankMiddleware  `yaml:"bank_middlewares"`
	Limits          map[string]Limit  `yaml:"limits"`
	Windows         map[string]Window `yaml:"windows"`
	Rules           []string          `yaml:"rules"`
}

type BankMiddleware struct {
	Name     string   `yaml:"name"`
	Enabled  *bool    `yaml:"enabled"` // default true
	Rails    []string `yaml:"rails"`
	HomeBank string   `yaml:"home_bank"` // the bank its source account is at; INTRABANK goes only there
}

func (b BankMiddleware) enabled() bool { return b.Enabled == nil || *b.Enabled }

// Limit bounds an amount, inclusive; 0 is no bound.
type Limit struct {
	Min int64 `yaml:"min"`
	Max int64 `yaml:"max"`
}

// Window is a rail's operating hours in WIB.
type Window struct {
	Open         string `yaml:"open"`  // "HH:MM", inclusive
	Close        string `yaml:"close"` // "HH:MM", exclusive
	WeekdaysOnly bool   `yaml:"weekdays_only"`
	open, close  int    // minutes since midnight
}

// Factory builds a rule from the configuration.
type Factory func(*Config) (Rule, error)

var (
	registryMu sync.RWMutex
	registry   = map[string]Factory{}
)

// Register adds a rule class under name, for routing.yaml's rules list
// (P§3.1: new rules without changing the chain). It panics on a duplicate.
func Register(name string, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, dup := registry[name]; dup {
		panic("routing: rule registered twice: " + name)
	}
	registry[name] = f
}

var (
	hhmm     = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)
	bankCode = regexp.MustCompile(`^[0-9]{3}$`)
	currency = regexp.MustCompile(`^[A-Z]{3}$`)
)

// Parse validates a routing.yaml and builds its chain. Unknown keys, rules
// and rails are errors: a typo must not silently change routing.
func Parse(b []byte) (*Chain, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("routing: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	c := &Chain{cfg: &cfg, version: hex.EncodeToString(sum[:])[:12]}
	registryMu.RLock()
	defer registryMu.RUnlock()
	for _, name := range cfg.Rules {
		f, ok := registry[name]
		if !ok {
			return nil, fmt.Errorf("routing: unknown rule %q", name)
		}
		r, err := f(&cfg)
		if err != nil {
			return nil, fmt.Errorf("routing: rule %s: %w", name, err)
		}
		c.rules = append(c.rules, r)
	}
	return c, nil
}

func (c *Config) validate() error {
	var errs []error
	bad := func(f string, a ...any) { errs = append(errs, fmt.Errorf("routing: "+f, a...)) }
	if c.Version != 1 {
		bad("version must be 1, got %d", c.Version)
	}
	if !currency.MatchString(c.Currency) {
		bad("currency must be an ISO 4217 code")
	}
	seen := map[string]bool{}
	for _, r := range c.RailOrder {
		if !slices.Contains(knownRails, r) || seen[r] {
			bad("rail_order: unknown or repeated rail %q", r)
		}
		seen[r] = true
	}
	if len(c.BankMiddlewares) == 0 {
		bad("no bank_middlewares")
	}
	names := map[string]bool{}
	for _, b := range c.BankMiddlewares {
		switch {
		case b.Name == "" || names[b.Name]:
			bad("bank middleware name %q empty or repeated", b.Name)
		case len(b.Rails) == 0:
			bad("%s: no rails", b.Name)
		case b.HomeBank != "" && !bankCode.MatchString(b.HomeBank):
			bad("%s: home_bank must be 3 digits", b.Name)
		case slices.Contains(b.Rails, RailIntrabank) && b.HomeBank == "":
			bad("%s: INTRABANK needs a home_bank", b.Name)
		}
		names[b.Name] = true
		for _, r := range b.Rails {
			if !seen[r] {
				bad("%s: rail %q isn't in rail_order", b.Name, r)
			}
		}
	}
	for rail, l := range c.Limits {
		switch {
		case !slices.Contains(knownRails, rail):
			bad("limits: unknown rail %q", rail)
		case l.Min < 0 || l.Max < 0:
			bad("limits %s: negative", rail)
		case l.Max > 0 && l.Min > l.Max:
			bad("limits %s: min above max", rail)
		}
	}
	for rail, w := range c.Windows {
		if !slices.Contains(knownRails, rail) {
			bad("windows: unknown rail %q", rail)
			continue
		}
		if !hhmm.MatchString(w.Open) || !hhmm.MatchString(w.Close) {
			bad("windows %s: open and close must be HH:MM", rail)
			continue
		}
		o, _ := time.Parse("15:04", w.Open)
		cl, _ := time.Parse("15:04", w.Close)
		w.open, w.close = o.Hour()*60+o.Minute(), cl.Hour()*60+cl.Minute()
		if w.open >= w.close {
			bad("windows %s: open must be before close", rail)
		}
		c.Windows[rail] = w
	}
	ruleSeen := map[string]bool{}
	for _, r := range c.Rules {
		if ruleSeen[r] {
			bad("rule %q listed twice", r)
		}
		ruleSeen[r] = true
	}
	if len(c.Rules) == 0 {
		bad("no rules")
	}
	return errors.Join(errs...)
}

func (c *Config) bmw(name string) BankMiddleware {
	for _, b := range c.BankMiddlewares {
		if b.Name == name {
			return b
		}
	}
	return BankMiddleware{}
}
