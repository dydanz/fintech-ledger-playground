package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"fintech-ledger-playground/internal/logger"
)

// Loki reads the signals the ledger only keeps in its logs (D§6.1): the
// recovery job's completions, outages, leader changes, the reconciler.
type Loki struct {
	base    string
	client  *http.Client
	failing atomic.Bool // set during an outage, so it's logged once
}

// Point is one sample of a series.
type Point struct {
	At time.Time
	V  float64
}

// Series is one result of a metric query.
type Series struct {
	Labels map[string]string
	Points []Point
}

// LogLine is one log line, its JSON fields parsed.
type LogLine struct {
	At     time.Time
	Labels map[string]string // stream labels: service, level, container
	Fields map[string]any
}

func NewLoki(baseURL string, timeout time.Duration) *Loki {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &Loki{base: strings.TrimRight(baseURL, "/"), client: &http.Client{Timeout: timeout}}
}

// Matrix runs a metric query over [start, end] at step.
func (l *Loki) Matrix(ctx context.Context, query string, start, end time.Time, step time.Duration) ([]Series, error) {
	q := url.Values{"query": {query}, "start": {unix(start)}, "end": {unix(end)}, "step": {strconv.Itoa(int(step.Seconds()))}}
	var res struct {
		Data struct {
			Result []struct {
				Metric map[string]string `json:"metric"`
				Values [][2]any          `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := l.get(ctx, "loki_matrix", q, &res); err != nil {
		return nil, err
	}
	out := make([]Series, 0, len(res.Data.Result))
	for _, r := range res.Data.Result {
		s := Series{Labels: r.Metric}
		for _, v := range r.Values {
			ts, _ := v[0].(float64)
			str, _ := v[1].(string)
			f, err := strconv.ParseFloat(str, 64)
			if err != nil {
				continue
			}
			s.Points = append(s.Points, Point{At: time.Unix(int64(ts), 0), V: f})
		}
		out = append(out, s)
	}
	return out, nil
}

// Lines runs a log query over [start, end], newest first when backward. Lines
// that aren't JSON are skipped.
func (l *Loki) Lines(ctx context.Context, query string, start, end time.Time, limit int, backward bool) ([]LogLine, error) {
	dir := "forward"
	if backward {
		dir = "backward"
	}
	q := url.Values{"query": {query}, "start": {unix(start)}, "end": {unix(end)}, "limit": {strconv.Itoa(limit)}, "direction": {dir}}
	var res struct {
		Data struct {
			Result []struct {
				Stream map[string]string `json:"stream"`
				Values [][2]string       `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := l.get(ctx, "loki_lines", q, &res); err != nil {
		return nil, err
	}
	var out []LogLine
	for _, r := range res.Data.Result {
		for _, v := range r.Values {
			ns, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				continue
			}
			var f map[string]any
			if json.Unmarshal([]byte(v[1]), &f) != nil {
				continue
			}
			out = append(out, LogLine{At: time.Unix(0, ns), Labels: r.Stream, Fields: f})
		}
	}
	return out, nil
}

// get calls query_range. A failure is logged as one dependency.call per
// outage (the console polls every few seconds; logging each would flood).
func (l *Loki) get(ctx context.Context, operation string, q url.Values, out any) error {
	start := time.Now()
	err := l.fetch(ctx, q, out)
	if err == nil {
		l.failing.Store(false)
		return nil
	}
	if !l.failing.Swap(true) {
		logger.FromContext(ctx).WarnContext(ctx, "loki call failed; activity signals unavailable", "event", "dependency.call",
			"outcome", "failure", "reason", callReason(err), "dependency", "loki", "operation", operation,
			"duration_ms", float64(time.Since(start).Microseconds())/1000, "error", err)
	}
	return err
}

func (l *Loki) fetch(ctx context.Context, q url.Values, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, l.base+"/loki/api/v1/query_range?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("loki: %d %s", resp.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return errors.Join(errors.New("loki: bad response"), err)
	}
	return nil
}

func unix(t time.Time) string { return strconv.FormatInt(t.UnixNano(), 10) }
