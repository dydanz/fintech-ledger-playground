package routing_test

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/routing"
)

func TestParseRejects(t *testing.T) {
	for name, mod := range map[string]func(string) string{
		"bad YAML":    func(s string) string { return s + "\n  : : :" },
		"unknown key": func(s string) string { return s + "\nsurprise: 1\n" },
		"unknown rule": func(s string) string {
			return strings.Replace(s, "intrabank_preference]", "intrabank_preference, cheapest_first]", 1)
		},
		"no rules": func(s string) string {
			return strings.Replace(s, "rules: [bmw_enabled, rail_capability, amount_rail_limits, rail_window, intrabank_preference]", "rules: []", 1)
		},
		"duplicate rule": func(s string) string {
			return strings.Replace(s, "intrabank_preference]", "intrabank_preference, bmw_enabled]", 1)
		},
		"negative limit": func(s string) string { return strings.Replace(s, "RTOL: {max: 50000000}", "RTOL: {max: -1}", 1) },
		"min above max": func(s string) string {
			return strings.Replace(s, "RTOL: {max: 50000000}", "RTOL: {min: 60000000, max: 50000000}", 1)
		},
		"unknown rail": func(s string) string { return strings.Replace(s, "rails: [BIFAST, RTOL]", "rails: [BIFAST, SWIFT]", 1) },
		"rail not in order": func(s string) string {
			return strings.Replace(s, "rail_order: [INTRABANK, BIFAST, RTOL, SKNBI, RTGS]", "rail_order: [BIFAST, RTOL, SKNBI, RTGS]", 1)
		},
		"duplicate BMW": func(s string) string { return strings.Replace(s, "name: bmw-c", "name: bmw-a", 1) },
		"BMW without rails": func(s string) string {
			return strings.Replace(s, "{name: bmw-c, rails: [BIFAST]}", "{name: bmw-c, rails: []}", 1)
		},
		"bad home bank":       func(s string) string { return strings.Replace(s, `home_bank: "014"`, `home_bank: "14"`, 1) },
		"intrabank, no home":  func(s string) string { return strings.Replace(s, `, home_bank: "014"`, "", 1) },
		"bad window time":     func(s string) string { return strings.Replace(s, `open: "06:30"`, `open: "6.30"`, 1) },
		"window closes first": func(s string) string { return strings.Replace(s, `open: "06:30"`, `open: "16:00"`, 1) },
		"window, unknown rail": func(s string) string {
			return strings.Replace(s, "windows:\n", "windows:\n  SWIFT: {open: \"01:00\", close: \"02:00\"}\n", 1)
		},
		"limit, unknown rail": func(s string) string { return strings.Replace(s, "limits:\n", "limits:\n  SWIFT: {max: 1}\n", 1) },
		"no currency":         func(s string) string { return strings.Replace(s, "currency: IDR\n", "", 1) },
		"wrong version":       func(s string) string { return strings.Replace(s, "version: 1", "version: 2", 1) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := routing.Parse([]byte(mod(sample))); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

// The repo's own file must parse, and route P§5 #11 the way the tests above
// do.
func TestShippedConfigParses(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "config", "money-out", "routing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	c, err := routing.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	for _, cand := range route(c, 300_000_000, "022", midweek).Candidates {
		if cand.Rail != "SKNBI" && cand.Rail != "RTGS" {
			t.Fatalf("300,000,000 routed over %s", cand.Rail)
		}
	}
	if got := route(c, 1_000_000, "014", midweek).Candidates; len(got) == 0 || got[0].Rail != "INTRABANK" {
		t.Fatalf("014 not intrabank first: %s", names(got))
	}
}

type watchEnv struct {
	t    *testing.T
	path string
	w    *routing.Watcher
	logs *bytes.Buffer
	ctx  context.Context
}

func newWatch(t *testing.T, content string) *watchEnv {
	t.Helper()
	path := filepath.Join(t.TempDir(), "routing.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	logs := &bytes.Buffer{}
	ctx := logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "money-out-middleware"))
	w, err := routing.NewWatcher(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	return &watchEnv{t: t, path: path, w: w, logs: logs, ctx: ctx}
}

func (e *watchEnv) write(content string) {
	e.t.Helper()
	if err := os.WriteFile(e.path, []byte(content), 0o600); err != nil {
		e.t.Fatal(err)
	}
}

func TestWatcherReloadsAChangedFile(t *testing.T) {
	e := newWatch(t, sample)
	before := e.w.Chain().Version()
	e.w.Check(e.ctx) // unchanged: nothing happens
	if e.w.Chain().Version() != before || strings.Count(e.logs.String(), `"event":"transfer.routing_config"`) != 1 {
		t.Fatalf("unchanged file reloaded: %s", e.logs)
	}
	e.write(strings.Replace(sample, `{name: bmw-a, rails: [BIFAST, RTOL]}`, `{name: bmw-a, rails: [BIFAST, RTOL], enabled: false}`, 1))
	e.w.Check(e.ctx)
	if e.w.Chain().Version() == before {
		t.Fatal("changed file not reloaded")
	}
	if strings.Contains(names(route(e.w.Chain(), 1_000_000, "022", midweek).Candidates), "bmw-a") {
		t.Fatal("the reloaded chain still routes to the disabled BMW")
	}
	if !strings.Contains(e.logs.String(), `"reason":"reloaded"`) {
		t.Fatalf("no reload event: %s", e.logs)
	}
}

// Review Focus 4: a reload that fails validation keeps the previous chain
// live, and is logged once, not on every poll.
func TestInvalidReloadKeepsPreviousChain(t *testing.T) {
	e := newWatch(t, sample)
	before := e.w.Chain()
	for _, bad := range []string{
		"not: [yaml",
		strings.Replace(sample, "intrabank_preference]", "intrabank_preference, cheapest_first]", 1),
		strings.Replace(sample, "RTOL: {max: 50000000}", "RTOL: {max: -5}", 1),
	} {
		e.write(bad)
		for i := 0; i < 3; i++ {
			e.w.Check(e.ctx)
		}
		if e.w.Chain() != before {
			t.Fatalf("a bad file replaced the chain: %q", bad)
		}
	}
	if n := strings.Count(e.logs.String(), `"reason":"invalid_config"`); n != 3 {
		t.Fatalf("invalid_config logged %d times for 3 bad files polled 3 times each, want 3: %s", n, e.logs)
	}
	if !strings.Contains(e.logs.String(), `"level":"error"`) {
		t.Fatal("a rejected reload is not an error")
	}
	// Fixing the file reloads it.
	e.write(strings.Replace(sample, "RTOL: {max: 50000000}", "RTOL: {max: 40000000}", 1))
	e.w.Check(e.ctx)
	if e.w.Chain() == before {
		t.Fatal("the fixed file was not loaded")
	}
}

// An unreadable file (deleted, mid-rename) keeps the chain too.
func TestMissingFileKeepsChain(t *testing.T) {
	e := newWatch(t, sample)
	before := e.w.Chain()
	if err := os.Remove(e.path); err != nil {
		t.Fatal(err)
	}
	e.w.Check(e.ctx)
	e.w.Check(e.ctx)
	if e.w.Chain() != before {
		t.Fatal("a missing file replaced the chain")
	}
	if n := strings.Count(e.logs.String(), `"reason":"unreadable"`); n != 1 {
		t.Fatalf("unreadable logged %d times, want 1", n)
	}
}

// At startup there is no previous chain: a bad file stops MOM.
func TestWatcherNeedsAValidFileAtStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "routing.yaml")
	if err := os.WriteFile(path, []byte("rules: [nope]"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := routing.NewWatcher(context.Background(), path); err == nil {
		t.Fatal("started on an invalid file")
	}
	if _, err := routing.NewWatcher(context.Background(), filepath.Join(t.TempDir(), "absent.yaml")); err == nil {
		t.Fatal("started without a file")
	}
}

// Same-node retries are per bank middleware (P§3.3): 2 unless set.
func TestSameNodeRetries(t *testing.T) {
	c := chain(t, strings.Replace(sample, `{name: bmw-c, rails: [BIFAST]}`, `{name: bmw-c, rails: [BIFAST], same_node_retries: 0}`, 1))
	if got := c.Config().SameNodeRetries("bmw-a"); got != 2 {
		t.Fatalf("default = %d, want 2", got)
	}
	if got := c.Config().SameNodeRetries("bmw-c"); got != 0 {
		t.Fatalf("bmw-c = %d, want 0", got)
	}
	if got := c.Config().SameNodeRetries("not-configured"); got != 0 {
		t.Fatalf("unknown BMW = %d, want 0", got)
	}
	for _, bad := range []string{"-1", "11"} {
		if _, err := routing.Parse([]byte(strings.Replace(sample, `{name: bmw-c, rails: [BIFAST]}`,
			`{name: bmw-c, rails: [BIFAST], same_node_retries: `+bad+`}`, 1))); err == nil {
			t.Fatalf("same_node_retries %s accepted", bad)
		}
	}
}
