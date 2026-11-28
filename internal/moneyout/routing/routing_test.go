package routing_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/moneyout/routing"
)

var wib = time.FixedZone("WIB", 7*60*60)

// A Wednesday at 10:00 WIB: every window is open.
var midweek = time.Date(2026, 11, 25, 10, 0, 0, 0, wib)

const sample = `
version: 1
currency: IDR
rail_order: [INTRABANK, BIFAST, RTOL, SKNBI, RTGS]
bank_middlewares:
  - {name: bmw-a, rails: [BIFAST, RTOL]}
  - {name: bmw-b, rails: [INTRABANK], home_bank: "014"}
  - {name: bmw-c, rails: [BIFAST]}
  - {name: bmw-d, rails: [BIFAST, SKNBI, RTGS]}
limits:
  BIFAST: {max: 250000000}
  RTOL: {max: 50000000}
  SKNBI: {max: 1000000000}
  RTGS: {min: 100000000}
windows:
  RTGS: {open: "06:30", close: "15:00", weekdays_only: true}
  SKNBI: {open: "08:00", close: "15:00", weekdays_only: true}
rules: [bmw_enabled, rail_capability, amount_rail_limits, rail_window, intrabank_preference]
`

func chain(t *testing.T, yaml string) *routing.Chain {
	t.Helper()
	c, err := routing.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func names(cs []routing.Candidate) string {
	var out []string
	for _, c := range cs {
		out = append(out, c.BankMiddleware+"/"+c.Rail)
	}
	return strings.Join(out, " ")
}

func route(c *routing.Chain, amount int64, bank string, at time.Time) routing.Result {
	return c.Route(context.Background(), routing.Request{TransferID: "t", Amount: amount, Currency: "IDR", BankCode: bank, Now: at})
}

// Q5: rails in the configured order, then the BMWs in theirs.
func TestCandidateOrder(t *testing.T) {
	got := route(chain(t, sample), 1_000_000, "022", midweek)
	if want := "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-a/RTOL bmw-d/SKNBI"; names(got.Candidates) != want {
		t.Fatalf("candidates = %s, want %s", names(got.Candidates), want)
	}
}

// P§5 #11: 300,000,000 is above BI-FAST's and RTOL's limits: SKNBI or RTGS.
func TestAboveBIFASTLimitGoesToSKNBIOrRTGS(t *testing.T) {
	got := route(chain(t, sample), 300_000_000, "022", midweek)
	if want := "bmw-d/SKNBI bmw-d/RTGS"; names(got.Candidates) != want {
		t.Fatalf("candidates = %s, want %s", names(got.Candidates), want)
	}
	for _, c := range got.Candidates {
		if c.Rail == "BIFAST" || c.Rail == "RTOL" {
			t.Fatalf("routed %d over %s", 300_000_000, c.Rail)
		}
	}
}

func TestAmountLimitEdges(t *testing.T) {
	c := chain(t, sample)
	for _, tc := range []struct {
		amount int64
		want   string
	}{
		{250_000_000, "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-d/SKNBI bmw-d/RTGS"}, // BI-FAST's limit is inclusive
		{250_000_001, "bmw-d/SKNBI bmw-d/RTGS"},
		{50_000_000, "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-a/RTOL bmw-d/SKNBI"},
		{99_999_999, "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-d/SKNBI"},
		{100_000_000, "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-d/SKNBI bmw-d/RTGS"}, // RTGS's minimum is inclusive
		{1_000_000_001, "bmw-d/RTGS"},
	} {
		if got := names(route(c, tc.amount, "022", midweek).Candidates); got != tc.want {
			t.Errorf("%d: %s, want %s", tc.amount, got, tc.want)
		}
	}
}

// Intrabank goes only to the BMW whose home bank is the beneficiary's, and
// that BMW comes first.
func TestIntrabank(t *testing.T) {
	c := chain(t, sample)
	got := route(c, 1_000_000, "014", midweek)
	if want := "bmw-b/INTRABANK bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-a/RTOL bmw-d/SKNBI"; names(got.Candidates) != want {
		t.Fatalf("to 014: %s, want %s", names(got.Candidates), want)
	}
	if strings.Contains(names(route(c, 1_000_000, "008", midweek).Candidates), "INTRABANK") {
		t.Fatal("intrabank rail offered for another bank's beneficiary")
	}
}

// The home bank BMW is preferred on every rail it has, not only intrabank.
func TestHomeBankPreferredOnAllItsRails(t *testing.T) {
	c := chain(t, strings.Replace(sample, `{name: bmw-d, rails: [BIFAST, SKNBI, RTGS]}`,
		`{name: bmw-d, rails: [BIFAST, SKNBI, RTGS], home_bank: "009"}`, 1))
	got := route(c, 1_000_000, "009", midweek)
	if !strings.HasPrefix(names(got.Candidates), "bmw-d/BIFAST bmw-d/SKNBI ") {
		t.Fatalf("candidates = %s, want bmw-d's first", names(got.Candidates))
	}
}

func TestRailWindows(t *testing.T) {
	c := chain(t, sample)
	for _, tc := range []struct {
		at   time.Time
		want string
	}{
		{time.Date(2026, 11, 25, 6, 29, 0, 0, wib), "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST"},                            // before RTGS opens
		{time.Date(2026, 11, 25, 6, 30, 0, 0, wib), "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-d/RTGS"},                 // RTGS open, SKNBI not yet
		{time.Date(2026, 11, 25, 14, 59, 0, 0, wib), "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-d/SKNBI bmw-d/RTGS"},    // both open
		{time.Date(2026, 11, 25, 15, 0, 0, 0, wib), "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST"},                            // close is exclusive
		{time.Date(2026, 11, 28, 10, 0, 0, 0, wib), "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST"},                            // Saturday
		{time.Date(2026, 11, 25, 3, 0, 0, 0, time.UTC), "bmw-a/BIFAST bmw-c/BIFAST bmw-d/BIFAST bmw-d/SKNBI bmw-d/RTGS"}, // 10:00 WIB, given in UTC
	} {
		if got := names(route(c, 150_000_000, "022", tc.at).Candidates); got != tc.want {
			t.Errorf("%s: %s, want %s", tc.at, got, tc.want)
		}
	}
}

// The kill switch drops every candidate of a disabled BMW.
func TestKillSwitch(t *testing.T) {
	c := chain(t, strings.Replace(sample, `{name: bmw-a, rails: [BIFAST, RTOL]}`, `{name: bmw-a, rails: [BIFAST, RTOL], enabled: false}`, 1))
	got := route(c, 1_000_000, "022", midweek)
	if strings.Contains(names(got.Candidates), "bmw-a") {
		t.Fatalf("disabled BMW routed: %s", names(got.Candidates))
	}
	if !hasDecision(got, "bmw-a", "BIFAST", "bmw_enabled", false, "disabled") {
		t.Fatalf("no bmw_enabled decision for bmw-a: %+v", got.Decisions)
	}
}

func TestOtherCurrencyHasNoCandidate(t *testing.T) {
	got := chain(t, sample).Route(context.Background(), routing.Request{TransferID: "t", Amount: 100, Currency: "USD", BankCode: "022", Now: midweek})
	if len(got.Candidates) != 0 {
		t.Fatalf("candidates = %s", names(got.Candidates))
	}
}

// Every candidate dropped says by which rule and why; every one kept says
// its position (P§3.1: the decision is persisted with the transfer).
func TestDecisionsExplainEveryCandidate(t *testing.T) {
	got := route(chain(t, sample), 300_000_000, "022", midweek)
	for _, w := range []struct {
		bmw, rail, rule string
		kept            bool
		reason          string
	}{
		{"bmw-a", "BIFAST", "amount_rail_limits", false, "above_rail_max"},
		{"bmw-a", "SKNBI", "rail_capability", false, "rail_not_supported"},
		{"bmw-b", "INTRABANK", "rail_capability", false, "intrabank_other_bank"},
		{"bmw-d", "SKNBI", "chain", true, "candidate_1"},
		{"bmw-d", "RTGS", "chain", true, "candidate_2"},
	} {
		if !hasDecision(got, w.bmw, w.rail, w.rule, w.kept, w.reason) {
			t.Errorf("missing %+v in %+v", w, got.Decisions)
		}
	}
	if got.ConfigVersion == "" {
		t.Fatal("no config version on the result")
	}
	// 5 rails × 4 BMWs = 20 pairs, each explained exactly once as dropped
	// or kept.
	seen := map[string]int{}
	for _, d := range got.Decisions {
		if !d.Kept || d.Rule == "chain" {
			seen[d.BankMiddleware+"/"+d.Rail]++
		}
	}
	if len(seen) != 20 {
		t.Fatalf("%d pairs explained, want 20", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("%s explained %d times", k, n)
		}
	}
}

// A window that drops the last eligible rail makes the outcome a cut-off,
// not "no rail": the transfer could go later.
func TestNoCandidateReason(t *testing.T) {
	c := chain(t, sample)
	night := time.Date(2026, 11, 25, 22, 0, 0, 0, wib)
	if got := route(c, 300_000_000, "022", night); len(got.Candidates) != 0 || got.NoCandidateReason() != "CUTOFF" {
		t.Fatalf("night: %s %q", names(got.Candidates), got.NoCandidateReason())
	}
	off := chain(t, strings.ReplaceAll(sample, "rails: [BIFAST, SKNBI, RTGS]", "rails: [BIFAST], enabled: false"))
	if got := route(off, 300_000_000, "022", midweek); len(got.Candidates) != 0 || got.NoCandidateReason() != "RAIL_UNAVAILABLE" {
		t.Fatalf("no rail: %s %q", names(got.Candidates), got.NoCandidateReason())
	}
}

func hasDecision(r routing.Result, bmw, rail, rule string, kept bool, reason string) bool {
	for _, d := range r.Decisions {
		if d.BankMiddleware == bmw && d.Rail == rail && d.Rule == rule && d.Kept == kept && d.Reason == reason {
			return true
		}
	}
	return false
}

// New rules are classes: registered by name, then usable from the YAML
// without touching the chain (P§3.1).
func TestRegisteredRule(t *testing.T) {
	routing.Register("drop_rtol_for_test", func(*routing.Config) (routing.Rule, error) { return dropRail("RTOL"), nil })
	c := chain(t, strings.Replace(sample, "intrabank_preference]", "intrabank_preference, drop_rtol_for_test]", 1))
	if got := names(route(c, 1_000_000, "022", midweek).Candidates); strings.Contains(got, "RTOL") {
		t.Fatalf("registered rule not applied: %s", got)
	}
}

type dropRail string

func (d dropRail) Name() string { return "drop_rtol_for_test" }
func (d dropRail) Apply(_ context.Context, _ routing.Request, in []routing.Candidate) ([]routing.Candidate, []routing.Decision) {
	var out []routing.Candidate
	var ds []routing.Decision
	for _, c := range in {
		if c.Rail == string(d) {
			ds = append(ds, routing.Decision{BankMiddleware: c.BankMiddleware, Rail: c.Rail, Rule: d.Name(), Reason: "test"})
			continue
		}
		out = append(out, c)
	}
	return out, ds
}
