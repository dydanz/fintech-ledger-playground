// Package routing turns a transfer into an ordered list of candidates
// (bank_middleware, rail) (P§3.1, spec/money-out TRD §6.4). The starting
// candidates are every configured rail, in the configured order (Q5),
// crossed with every bank middleware, in its order; each rule of the chain
// then drops or reorders them and says why. Rules are classes registered by
// name and chosen in config/money-out/routing.yaml, which also holds every
// bank-specific value: MOM's code has none (P§8).
package routing

import (
	"context"
	"fmt"
	"time"
)

// Request is what routing knows about a transfer.
type Request struct {
	TransferID string
	Amount     int64 // minor units
	Currency   string
	BankCode   string // the beneficiary's bank
	Now        time.Time
}

// Candidate is one way to send the transfer.
type Candidate struct {
	BankMiddleware string `json:"bank_middleware"`
	Rail           string `json:"rail"`
}

// Decision explains one candidate: dropped by a rule, moved by one, or kept
// at a position (Rule "chain").
type Decision struct {
	BankMiddleware string `json:"bank_middleware"`
	Rail           string `json:"rail"`
	Rule           string `json:"rule"`
	Kept           bool   `json:"kept"`
	Reason         string `json:"reason"`
}

// Rule is one step of the chain. It returns the candidates it keeps, in
// order, and a Decision for each one it dropped or moved.
type Rule interface {
	Name() string
	Apply(ctx context.Context, req Request, in []Candidate) ([]Candidate, []Decision)
}

// Result is a routing decision, stored with the transfer (P§3.1).
type Result struct {
	ConfigVersion string      `json:"config_version"`
	Candidates    []Candidate `json:"candidates"`
	Decisions     []Decision  `json:"decisions"`
	RoutedAt      time.Time   `json:"routed_at"`
}

// RuleWindow is the rule whose drops make an empty result a cut-off.
const RuleWindow = "rail_window"

// NoCandidateReason is the canonical reason for a transfer with no
// candidate: CUTOFF when a window dropped an otherwise eligible rail (it
// could go later), else RAIL_UNAVAILABLE.
func (r Result) NoCandidateReason() string {
	for _, d := range r.Decisions {
		if d.Rule == RuleWindow && !d.Kept {
			return "CUTOFF"
		}
	}
	return "RAIL_UNAVAILABLE"
}

// Chain is a validated configuration and its rules. It is immutable: a
// reload builds a new one.
type Chain struct {
	cfg     *Config
	version string
	rules   []Rule
}

func (c *Chain) Version() string { return c.version }

// Config is the configuration the chain was built from. Don't modify it.
func (c *Chain) Config() *Config { return c.cfg }

// Route runs the chain.
func (c *Chain) Route(ctx context.Context, req Request) Result {
	res := Result{ConfigVersion: c.version, RoutedAt: req.Now}
	var cands []Candidate
	for _, rail := range c.cfg.RailOrder {
		for _, b := range c.cfg.BankMiddlewares {
			cands = append(cands, Candidate{BankMiddleware: b.Name, Rail: rail})
		}
	}
	for _, r := range c.rules {
		var ds []Decision
		cands, ds = r.Apply(ctx, req, cands)
		res.Decisions = append(res.Decisions, ds...)
	}
	for i, cand := range cands {
		res.Decisions = append(res.Decisions, Decision{BankMiddleware: cand.BankMiddleware, Rail: cand.Rail, Rule: "chain",
			Kept: true, Reason: fmt.Sprintf("candidate_%d", i+1)})
	}
	res.Candidates = cands
	if res.Candidates == nil {
		res.Candidates = []Candidate{}
	}
	return res
}

// filter keeps the candidates keep accepts; for each dropped one, keep's
// reason becomes a Decision of rule.
func filter(rule string, in []Candidate, keep func(Candidate) (bool, string)) ([]Candidate, []Decision) {
	var out []Candidate
	var ds []Decision
	for _, c := range in {
		ok, reason := keep(c)
		if ok {
			out = append(out, c)
			continue
		}
		ds = append(ds, Decision{BankMiddleware: c.BankMiddleware, Rail: c.Rail, Rule: rule, Reason: reason})
	}
	return out, ds
}
