package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/logger"
)

// The read-only ops routes (D§7.4): ledger-api's ops reads, made with the
// console's read token, merged with what only the logs know (Loki). Nothing
// here writes. Without Loki the screens still work from ledger-api's data,
// with signals.available false and the log-backed parts empty.

const (
	// MonitorWindow is the tiles' window: the last 10 minutes (D§5.1; longer
	// windows wait for a cheaper source than ledger-api's stats, plan §5).
	MonitorWindow = 10 * time.Minute
	// SignalsLookback is how far back incidents and self-healing reach.
	SignalsLookback = 30 * time.Minute
	opsCacheTTL     = 2 * time.Second
	feedSize        = 50
	healingSize     = 20
)

// OpsConfig is what the ops routes need besides ledger-api.
type OpsConfig struct {
	ReadToken string           // CONSOLE_READ_TOKEN: ledger-api's read operation; never logged
	Loki      *Loki            // nil: no activity signals
	Now       func() time.Time // tests; nil means time.Now
}

type ops struct {
	cfg    OpsConfig
	ledger *proxy
	now    func() time.Time
	cache  *respCache
}

func newOps(cfg OpsConfig, ledger *proxy) *ops {
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &ops{cfg: cfg, ledger: ledger, now: now, cache: newRespCache(opsCacheTTL)}
}

// --- ledger-api reads ---

type upstreamError struct {
	status int
	body   []byte
}

func (e *upstreamError) Error() string { return fmt.Sprintf("ledger-api answered %d", e.status) }

// read GETs a ledger-api internal route with the read token into out. It
// doesn't touch the gin response: the caller decides.
func (o *ops) read(ctx context.Context, target string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.ledger.up.BaseURL+target, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+o.cfg.ReadToken)
	if id := requestIDFrom(ctx); id != "" {
		req.Header.Set("X-Request-ID", id)
	}
	start := time.Now()
	resp, err := o.ledger.client.Do(req)
	if err != nil {
		logger.FromContext(ctx).WarnContext(ctx, "upstream call failed", "event", "dependency.call", "outcome", "failure",
			"reason", callReason(err), "dependency", o.ledger.up.Name, "operation", "ops_read",
			"duration_ms", float64(time.Since(start).Microseconds())/1000, "error", err)
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBody))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return &upstreamError{status: resp.StatusCode, body: body}
	}
	return json.Unmarshal(body, out)
}

type ctxKey struct{}

func requestIDFrom(ctx context.Context) string { v, _ := ctx.Value(ctxKey{}).(string); return v }

// fail answers an error from read: ledger-api's own error passes through;
// an unreachable ledger-api is a 503.
func fail(c *gin.Context, err error) {
	if ue, ok := err.(*upstreamError); ok {
		c.Data(ue.status, "application/json; charset=utf-8", ue.body)
		return
	}
	api.AbortError(c, http.StatusServiceUnavailable, "upstream_unavailable", "ledger-api is temporarily unavailable; try again")
}

// --- shapes ---

// OpsTx is ledger-api's ops transaction view, as the console relays it.
type OpsTx struct {
	Tx
	AccountID   string    `json:"account_id"`
	Amount      int64     `json:"amount"`
	Currency    string    `json:"currency"`
	OriginalID  string    `json:"original_transaction_id,omitempty"`
	RequestedBy string    `json:"requested_by"`
	TraceID     string    `json:"trace_id"`
	UpdatedAt   time.Time `json:"updated_at"`
	Balance     *int64    `json:"balance_after,omitempty"`
}

type FeedItem struct {
	Transaction OpsTx `json:"transaction"`
	Labelled
}

type count struct {
	Type          string `json:"type"`
	Status        string `json:"status"`
	FailureReason string `json:"failure_reason"`
	Count         int64  `json:"count"`
}

type stats struct {
	Since        time.Time `json:"since"`
	Transactions []count   `json:"transactions"`
	Pending      struct {
		Count       int64 `json:"count"`
		OldestAgeMs int64 `json:"oldest_age_ms"`
	} `json:"pending"`
	Outbox struct {
		Pending     int64 `json:"pending"`
		OldestAgeMs int64 `json:"oldest_age_ms"`
	} `json:"outbox"`
}

type Healing struct {
	At   time.Time `json:"at"`
	Text string    `json:"text"`
}

type Reconcile struct {
	Status     string     `json:"status"` // clean, mismatches, failed, unknown
	Mismatches int        `json:"mismatches"`
	Mode       string     `json:"mode,omitempty"`
	At         *time.Time `json:"at,omitempty"`
}

type Signals struct {
	Available bool `json:"available"`
}

type Monitor struct {
	GeneratedAt time.Time `json:"generated_at"`
	Window      struct {
		Since time.Time `json:"since"`
		Until time.Time `json:"until"`
	} `json:"window"`
	Signals Signals `json:"signals"`
	Tiles   struct {
		Transactions struct {
			Count     int64   `json:"count"`
			PerSecond float64 `json:"per_second"`
		} `json:"transactions"`
		HandledCorrectly struct {
			Ratio          float64 `json:"ratio"`
			SystemFailures int64   `json:"system_failures"`
			Valid          int64   `json:"valid"`
		} `json:"handled_correctly"`
		NeededRecovery struct {
			Recovered    int   `json:"recovered"`
			Refused      int   `json:"refused"`
			StillPending int64 `json:"still_pending"`
		} `json:"needed_recovery"`
		Reconciler Reconcile `json:"reconciler"`
		Outbox     struct {
			Pending     int64 `json:"pending"`
			OldestAgeMs int64 `json:"oldest_age_ms"`
		} `json:"outbox"`
	} `json:"tiles"`
	Incidents   []Incident   `json:"incidents"`
	Feed        []FeedItem   `json:"feed"`
	Handled     []LabelCount `json:"handled"`
	SelfHealing []Healing    `json:"self_healing"`
}

type LabelCount struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

// --- signals from Loki ---

// The LogQL behind the signals. Only level and service are stream labels;
// the rest is read with | json on named fields (logging rule §5).
const (
	qEvents    = `sum by (event, reason, dependency, outcome) (count_over_time({service=~"ledger-api|ledger-worker"} | json event, reason, dependency, outcome | event=~"outbox.publish|dependency.call|ledger.recover" [15s]))`
	qLag       = `max(max_over_time({service="ledger-worker"} | json event, lag_ms | event="outbox.lag" | unwrap lag_ms [15s]))`
	qWrites    = `sum by (status) (count_over_time({service="ledger-api"} | json message, method, route, status | message="http_request" | method="POST" | route=~"/internal/v1/.+" [15s]))`
	lRecover   = `{service="ledger-worker"} | json event, outcome | event="ledger.recover" | outcome="success"`
	lLeader    = `{service="ledger-worker"} | json event, state | event="app.leader" | state="acquired"`
	lReconcile = `{service="ledger-worker"} | json event, check | event="ledger.reconcile" | check=""`
	lFraud     = `{service="fraud-engine"} | json event | event="fraud.decision"`
)

type gathered struct {
	buckets   []Bucket
	recovered map[string]time.Time // transaction id → when the recovery job completed it
	healing   []Healing
	reconcile Reconcile
}

// gatherSignals reads the log-backed signals for [now-lookback, now].
func (o *ops) gatherSignals(ctx context.Context, now time.Time) (gathered, error) {
	l := o.cfg.Loki
	if l == nil {
		return gathered{}, fmt.Errorf("no loki configured")
	}
	from := now.Add(-SignalsLookback).Truncate(BucketWidth)
	var events, lag, writes []Series
	var recov, leaders, recon []LogLine
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) { events, err = l.Matrix(gctx, qEvents, from, now, BucketWidth); return })
	g.Go(func() (err error) { lag, err = l.Matrix(gctx, qLag, from, now, BucketWidth); return })
	g.Go(func() (err error) { writes, err = l.Matrix(gctx, qWrites, from, now, BucketWidth); return })
	g.Go(func() (err error) { recov, err = l.Lines(gctx, lRecover, from, now, 500, true); return })
	g.Go(func() (err error) { leaders, err = l.Lines(gctx, lLeader, from, now, 200, true); return })
	g.Go(func() (err error) {
		recon, err = l.Lines(gctx, lReconcile, now.Add(-2*time.Hour), now, 1, true)
		return
	})
	if err := g.Wait(); err != nil {
		return gathered{}, err
	}

	// Buckets: a count_over_time point stamped t covers (t-15s, t].
	idx := map[int64]int{}
	var b []Bucket
	for t := from; !t.After(now); t = t.Add(BucketWidth) {
		idx[t.Unix()] = len(b)
		b = append(b, Bucket{At: t})
	}
	put := func(p Point, f func(*Bucket, float64)) {
		if i, ok := idx[p.At.Add(-BucketWidth).Unix()]; ok {
			f(&b[i], p.V)
		}
	}
	for _, s := range events {
		e, r, dep, out := s.Labels["event"], s.Labels["reason"], s.Labels["dependency"], s.Labels["outcome"]
		for _, p := range s.Points {
			switch {
			case e == "outbox.publish" && r == "kafka_unavailable":
				put(p, func(k *Bucket, v float64) { k.KafkaFailures += int(v) })
			case e == "dependency.call" && dep == "redis":
				put(p, func(k *Bucket, v float64) { k.RedisFailures += int(v) })
			case e == "ledger.recover" && out == "success":
				put(p, func(k *Bucket, v float64) { k.Recovered += int(v) })
			}
		}
	}
	for _, s := range lag {
		for _, p := range s.Points {
			put(p, func(k *Bucket, v float64) { k.LagMs = &v })
		}
	}
	for _, s := range writes {
		st := s.Labels["status"]
		for _, p := range s.Points {
			switch {
			case st == "503":
				put(p, func(k *Bucket, v float64) { k.Refused += int(v) })
			case strings.HasPrefix(st, "2"):
				put(p, func(k *Bucket, v float64) { k.Writes2xx += int(v) })
			}
		}
	}

	out := gathered{buckets: b, recovered: map[string]time.Time{}, reconcile: Reconcile{Status: "unknown"}}
	for _, ln := range recov {
		id, _ := ln.Fields["transaction_id"].(string)
		if id == "" {
			continue
		}
		out.recovered[id] = ln.At
		age, _ := ln.Fields["age_ms"].(float64)
		out.healing = append(out.healing, Healing{At: ln.At,
			Text: fmt.Sprintf("Recovery job completed transaction …%s, stuck for %s", short(id), durationWords(time.Duration(age)*time.Millisecond))})
	}
	out.healing = append(out.healing, leaderHealing(leaders)...)
	if len(recon) > 0 {
		out.reconcile = reconcileOf(recon[0])
	}
	return out, nil
}

// leaderHealing turns bursts of app.leader acquired (one replica taking
// several jobs within 10 s) into one line each.
func leaderHealing(lines []LogLine) []Healing {
	sort.Slice(lines, func(i, j int) bool { return lines[i].At.Before(lines[j].At) })
	var out []Healing
	for i := 0; i < len(lines); {
		c := lines[i].Labels["container"]
		j := i + 1
		for j < len(lines) && lines[j].Labels["container"] == c && lines[j].At.Sub(lines[i].At) < 10*time.Second {
			j++
		}
		who := "a ledger-worker replica"
		if c != "" {
			who = "ledger-worker replica " + c
		}
		out = append(out, Healing{At: lines[i].At, Text: fmt.Sprintf("%s took leadership of %d background %s",
			strings.ToUpper(who[:1])+who[1:], j-i, plural(j-i, "job", "jobs"))})
		i = j
	}
	return out
}

func reconcileOf(ln LogLine) Reconcile {
	at := ln.At
	r := Reconcile{At: &at}
	r.Mode, _ = ln.Fields["mode"].(string)
	mm, _ := ln.Fields["mismatches"].(float64)
	r.Mismatches = int(mm)
	switch out, _ := ln.Fields["outcome"].(string); {
	case out == "success":
		r.Status = "clean"
	case ln.Fields["reason"] == "mismatches_found":
		r.Status = "mismatches"
	default:
		r.Status = "failed"
	}
	return r
}

func short(id string) string {
	if len(id) > 4 {
		return id[len(id)-4:]
	}
	return id
}

// --- handlers ---

// monitor is GET /console/api/ops/monitor.
func (o *ops) monitor(c *gin.Context) {
	o.cached(c, func(ctx context.Context) (any, error) {
		now := o.now().UTC()
		var st stats
		var page struct {
			Transactions []OpsTx `json:"transactions"`
		}
		var sig gathered
		var sigErr error
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error {
			return o.read(gctx, "/internal/v1/stats?since="+url.QueryEscape(now.Add(-MonitorWindow).Format(time.RFC3339)), &st)
		})
		g.Go(func() error { return o.read(gctx, fmt.Sprintf("/internal/v1/transactions?limit=%d", feedSize), &page) })
		g.Go(func() error { sig, sigErr = o.gatherSignals(gctx, now); return nil }) // Loki is optional
		if err := g.Wait(); err != nil {
			return nil, err
		}
		return buildMonitor(now, st, page.Transactions, sig, sigErr == nil), nil
	})
}

func buildMonitor(now time.Time, st stats, txs []OpsTx, sig gathered, available bool) Monitor {
	var m Monitor
	m.GeneratedAt, m.Window.Since, m.Window.Until = now, now.Add(-MonitorWindow), now
	m.Signals.Available = available
	m.Incidents, m.SelfHealing, m.Feed = []Incident{}, []Healing{}, []FeedItem{}

	var total, settled, rejected, timedOut, reversed int64
	for _, c := range st.Transactions {
		total += c.Count
		switch {
		case c.Status == "FAILED" && c.FailureReason == "processing_timeout":
			timedOut += c.Count
		case c.Status == "FAILED":
			rejected += c.Count
		case c.Status == "VOIDED", c.Type == "VOID" && c.Status == "SETTLED":
			reversed += c.Count
		case c.Status == "SETTLED":
			settled += c.Count
		}
	}
	recovered, refused := 0, 0
	for _, b := range sig.buckets {
		if !b.At.Before(m.Window.Since) {
			recovered += b.Recovered
			refused += b.Refused
		}
	}
	t := &m.Tiles
	t.Transactions.Count = total
	t.Transactions.PerSecond = float64(total) / MonitorWindow.Seconds()
	t.HandledCorrectly.Valid = total + int64(refused)
	t.HandledCorrectly.SystemFailures = int64(refused) + timedOut
	t.HandledCorrectly.Ratio = 1
	if t.HandledCorrectly.Valid > 0 {
		t.HandledCorrectly.Ratio = 1 - float64(t.HandledCorrectly.SystemFailures)/float64(t.HandledCorrectly.Valid)
	}
	t.NeededRecovery.Recovered, t.NeededRecovery.Refused, t.NeededRecovery.StillPending = recovered, refused, st.Pending.Count
	t.Reconciler = sig.reconcile
	if !available {
		t.Reconciler = Reconcile{Status: "unknown"}
	}
	t.Outbox.Pending, t.Outbox.OldestAgeMs = st.Outbox.Pending, st.Outbox.OldestAgeMs

	normal := max(settled-int64(recovered), 0)
	m.Handled = []LabelCount{
		{string(LabelNormal), normal}, {string(LabelRecovered), int64(recovered)}, {"refused_try_again", int64(refused)},
		{string(LabelRejected), rejected}, {string(LabelReversed), reversed}, {string(LabelTimedOut), timedOut},
		{string(LabelInProgress), st.Pending.Count},
	}
	if available {
		m.Incidents = Incidents(sig.buckets, now)
		m.SelfHealing = append(m.SelfHealing, sig.healing...)
		for _, inc := range m.Incidents {
			if inc.End != nil {
				m.SelfHealing = append(m.SelfHealing, Healing{At: *inc.End, Text: backAgain[inc.Kind]})
			}
		}
		sort.Slice(m.SelfHealing, func(i, j int) bool { return m.SelfHealing[i].At.After(m.SelfHealing[j].At) })
		if len(m.SelfHealing) > healingSize {
			m.SelfHealing = m.SelfHealing[:healingSize]
		}
	}
	for _, tx := range txs {
		_, rec := sig.recovered[tx.ID]
		m.Feed = append(m.Feed, FeedItem{Transaction: tx, Labelled: LabelOf(tx.Tx, rec, now)})
	}
	return m
}

// backAgain is the self-healing line at the end of an incident.
var backAgain = map[string]string{
	"kafka":    "Kafka is back: the events queued in the outbox were delivered",
	"database": "The database is back: writes succeed again, nothing half-done was kept",
	"redis":    "Redis is back: the idempotency cache is in use again",
}

// transactions is GET /console/api/ops/transactions: ledger-api's feed with
// labels.
func (o *ops) transactions(c *gin.Context) {
	o.cached(c, func(ctx context.Context) (any, error) {
		now := o.now().UTC()
		var page struct {
			Transactions []OpsTx `json:"transactions"`
			NextCursor   string  `json:"next_cursor,omitempty"`
		}
		q := url.Values{}
		for _, k := range []string{"limit", "cursor", "status", "type", "since"} {
			if v := c.Query(k); v != "" {
				q.Set(k, v)
			}
		}
		if err := o.read(ctx, "/internal/v1/transactions?"+q.Encode(), &page); err != nil {
			return nil, err
		}
		recovered := o.recoveredAmong(ctx, page.Transactions, now)
		items := []FeedItem{}
		for _, tx := range page.Transactions {
			_, rec := recovered[tx.ID]
			items = append(items, FeedItem{Transaction: tx, Labelled: LabelOf(tx.Tx, rec, now)})
		}
		return gin.H{"transactions": items, "next_cursor": page.NextCursor, "signals": Signals{Available: recovered != nil}}, nil
	})
}

// recoveredAmong returns which of txs the recovery job completed, or nil
// when Loki can't say.
func (o *ops) recoveredAmong(ctx context.Context, txs []OpsTx, now time.Time) map[string]bool {
	if o.cfg.Loki == nil {
		return nil
	}
	if len(txs) == 0 {
		return map[string]bool{}
	}
	from, to := txs[0].CreatedAt, txs[0].UpdatedAt
	for _, t := range txs {
		if t.CreatedAt.Before(from) {
			from = t.CreatedAt
		}
		if t.UpdatedAt.After(to) {
			to = t.UpdatedAt
		}
	}
	lines, err := o.cfg.Loki.Lines(ctx, lRecover, from.Add(-time.Minute), minTime(to.Add(time.Minute), now), 1000, true)
	if err != nil {
		return nil
	}
	inPage := map[string]bool{}
	for _, t := range txs {
		inPage[t.ID] = true
	}
	out := map[string]bool{}
	for _, ln := range lines {
		if id, _ := ln.Fields["transaction_id"].(string); inPage[id] {
			out[id] = true
		}
	}
	return out
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

// TimelineEntry is one step of a transaction's audit trail.
type TimelineEntry struct {
	At     time.Time `json:"at"`
	Kind   string    `json:"kind"` // ok, recovered, refused, info
	Title  string    `json:"title"`
	Detail string    `json:"detail,omitempty"`
	Source string    `json:"source"` // ledger (the permanent record) or logs
}

// transaction is GET /console/api/ops/transactions/:id: ledger-api's detail
// plus the log-backed steps, merged into one timeline.
func (o *ops) transaction(c *gin.Context) {
	o.cached(c, func(ctx context.Context) (any, error) {
		now := o.now().UTC()
		var d struct {
			Transaction OpsTx `json:"transaction"`
			Events      []struct {
				Type       string          `json:"type"`
				Version    int64           `json:"version"`
				Payload    json.RawMessage `json:"payload"`
				OccurredAt time.Time       `json:"occurred_at"`
			} `json:"events"`
			Journal []json.RawMessage `json:"journal"`
			Outbox  []struct {
				EventID     string     `json:"event_id"`
				PublishedAt *time.Time `json:"published_at"`
			} `json:"outbox"`
		}
		id := c.Param("id")
		if err := o.read(ctx, "/internal/v1/transactions/"+url.PathEscape(id)+"/detail", &d); err != nil {
			return nil, err
		}
		tx := d.Transaction
		var tl []TimelineEntry
		for _, e := range d.Events {
			tl = append(tl, eventEntry(e.Type, e.Payload, e.OccurredAt))
		}
		published, waiting := time.Time{}, 0
		for _, ob := range d.Outbox {
			if ob.PublishedAt == nil {
				waiting++
			} else if ob.PublishedAt.After(published) {
				published = *ob.PublishedAt
			}
		}
		switch {
		case waiting > 0:
			tl = append(tl, TimelineEntry{At: now, Kind: "info", Title: fmt.Sprintf("%d %s waiting to be published to Kafka", waiting, plural(waiting, "event", "events")), Source: "ledger"})
		case !published.IsZero():
			tl = append(tl, TimelineEntry{At: published, Kind: "ok", Title: "Events published to Kafka", Detail: "Other systems (the fraud engine) receive them from here", Source: "ledger"})
		}

		recovered, available := false, o.cfg.Loki != nil
		if o.cfg.Loki != nil {
			from, to := tx.CreatedAt.Add(-time.Minute), minTime(tx.UpdatedAt.Add(30*time.Minute), now)
			var recov, fraud, recon []LogLine
			g, gctx := errgroup.WithContext(ctx)
			g.Go(func() (err error) {
				recov, err = o.cfg.Loki.Lines(gctx, lRecover+` | json transaction_id | transaction_id="`+escapeLogQL(tx.ID)+`"`, from, to, 5, false)
				return
			})
			g.Go(func() (err error) {
				fraud, err = o.cfg.Loki.Lines(gctx, lFraud+` | json transaction_id | transaction_id="`+escapeLogQL(tx.ID)+`"`, from, to, 5, false)
				return
			})
			if tx.SettledAt != nil {
				g.Go(func() (err error) {
					recon, err = o.cfg.Loki.Lines(gctx, lReconcile, *tx.SettledAt, to, 20, false)
					return
				})
			}
			if err := g.Wait(); err != nil {
				available = false
			} else {
				for _, ln := range recov {
					recovered = true
					tl = append(tl, TimelineEntry{At: ln.At, Kind: "recovered", Title: "Recovery job picked it up",
						Detail: "The request couldn't finish on its own (for example, the database went away mid-way); the sweeper completed it", Source: "logs"})
				}
				for _, ln := range fraud {
					rule, _ := ln.Fields["rule"].(string)
					tl = append(tl, TimelineEntry{At: ln.At, Kind: "info", Title: "Fraud engine asked to freeze the account",
						Detail: "This transaction triggered rule " + rule + " (the account may already have been frozen)", Source: "logs"})
				}
				for _, ln := range recon {
					if ln.Fields["outcome"] == "success" {
						tl = append(tl, TimelineEntry{At: ln.At, Kind: "ok", Title: "Verified by the reconciler",
							Detail: "A clean check of balances, sequences and double entries ran after it settled", Source: "logs"})
						break
					}
				}
			}
		}
		sort.SliceStable(tl, func(i, j int) bool { return tl[i].At.Before(tl[j].At) })
		return gin.H{"transaction": tx, "label": LabelOf(tx.Tx, recovered, now), "timeline": tl, "journal": d.Journal,
			"signals": Signals{Available: available}}, nil
	})
}

// eventEntry describes one ledger event, from its type and payload.
func eventEntry(typ string, payload json.RawMessage, at time.Time) TimelineEntry {
	var p map[string]any
	_ = json.Unmarshal(payload, &p)
	e := TimelineEntry{At: at, Kind: "ok", Source: "ledger"}
	switch typ {
	case "TransactionRequested":
		by, _ := p["requested_by"].(string)
		e.Title, e.Detail = "Request received and saved", "By "+by+"; recorded before any money moves, so it can't be lost"
	case "TransactionProcessing":
		e.Title, e.Kind = "Processing started", "info"
	case "TransactionSettled":
		bal, _ := p["balance_after"].(float64)
		e.Title, e.Detail = "Settled: money moved", fmt.Sprintf("Balance after: %s", thousands(int(bal)))
	case "TransactionFailed":
		r, _ := p["reason"].(string)
		e.Kind, e.Title, e.Detail = "refused", "Refused", refusal(r)
		if r == "processing_timeout" {
			e.Title, e.Detail = "Failed safely", "Couldn't complete in time; nothing moved"
		}
	case "TransactionVoided":
		v, _ := p["void_transaction_id"].(string)
		e.Kind, e.Title, e.Detail = "info", "Reversed", "By void …"+short(v)+"; the original stays on record"
	default:
		e.Kind, e.Title = "info", typ
	}
	return e
}

// escapeLogQL keeps a value inside a LogQL string (ids are UUIDs; this is a
// guard, not a parser).
func escapeLogQL(s string) string {
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s)
}

// accounts is GET /console/api/ops/accounts: ledger-api's list, as is.
func (o *ops) accounts(c *gin.Context) {
	o.cached(c, func(ctx context.Context) (any, error) {
		q := url.Values{}
		for _, k := range []string{"limit", "cursor"} {
			if v := c.Query(k); v != "" {
				q.Set(k, v)
			}
		}
		var page json.RawMessage
		err := o.read(ctx, "/internal/v1/accounts?"+q.Encode(), &page)
		return page, err
	})
}

// account is GET /console/api/ops/accounts/:id: the account and its events.
func (o *ops) account(c *gin.Context) {
	o.cached(c, func(ctx context.Context) (any, error) {
		id := url.PathEscape(c.Param("id"))
		var acct struct {
			Account json.RawMessage `json:"account"`
		}
		var ev struct {
			Events json.RawMessage `json:"events"`
		}
		g, gctx := errgroup.WithContext(ctx)
		g.Go(func() error { return o.read(gctx, "/internal/v1/accounts/"+id, &acct) })
		g.Go(func() error { return o.read(gctx, "/internal/v1/accounts/"+id+"/events", &ev) })
		if err := g.Wait(); err != nil {
			return nil, err
		}
		return gin.H{"account": acct.Account, "events": ev.Events}, nil
	})
}

// --- the 2 s cache ---

type respCache struct {
	ttl   time.Duration
	group singleflight.Group
	mu    sync.Mutex
	m     map[string]cachedResp
}

type cachedResp struct {
	at   time.Time
	body []byte
}

func newRespCache(ttl time.Duration) *respCache {
	return &respCache{ttl: ttl, m: map[string]cachedResp{}}
}

// cached answers c from the cache when a fresh entry exists. Otherwise one
// caller computes it (concurrent identical requests share the result) and
// it's kept for the TTL. Errors aren't cached.
func (o *ops) cached(c *gin.Context, compute func(ctx context.Context) (any, error)) {
	key := c.Request.URL.Path + "?" + c.Request.URL.RawQuery
	o.cache.mu.Lock()
	if e, ok := o.cache.m[key]; ok && o.now().Sub(e.at) < o.cache.ttl {
		o.cache.mu.Unlock()
		c.Data(http.StatusOK, "application/json; charset=utf-8", e.body)
		return
	}
	o.cache.mu.Unlock()
	ctx := context.WithValue(c.Request.Context(), ctxKey{}, c.GetString("request_id"))
	v, err, _ := o.cache.group.Do(key, func() (any, error) {
		res, err := compute(context.WithoutCancel(ctx))
		if err != nil {
			return nil, err
		}
		body, err := json.Marshal(res)
		if err != nil {
			return nil, err
		}
		o.cache.mu.Lock()
		o.cache.m[key] = cachedResp{at: o.now(), body: body}
		for k, e := range o.cache.m { // drop stale entries as we go
			if o.now().Sub(e.at) > time.Minute {
				delete(o.cache.m, k)
			}
		}
		o.cache.mu.Unlock()
		return body, nil
	})
	if err != nil {
		fail(c, err)
		return
	}
	c.Data(http.StatusOK, "application/json; charset=utf-8", v.([]byte))
}
