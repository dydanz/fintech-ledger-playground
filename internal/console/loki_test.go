package console_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fintech-ledger-playground/internal/console"
	"fintech-ledger-playground/internal/logger"
)

func lokiCtx(logs *bytes.Buffer) context.Context {
	return logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "ledger-console"))
}

func TestLokiMatrix(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/loki/api/v1/query_range" {
			t.Errorf("path %s", r.URL.Path)
		}
		got = r.URL.Query().Get("query") + " step=" + r.URL.Query().Get("step")
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[
			{"metric":{"event":"outbox.publish","reason":"kafka_unavailable"},"values":[[1794220935,"2"],[1794220950,"1"]]}]}}`))
	}))
	defer srv.Close()
	c := console.NewLoki(srv.URL, time.Second)
	series, err := c.Matrix(context.Background(), `sum(x)`, time.Unix(1794220900, 0), time.Unix(1794221000, 0), 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if got != "sum(x) step=15" {
		t.Fatalf("query sent = %q", got)
	}
	if len(series) != 1 || series[0].Labels["reason"] != "kafka_unavailable" || len(series[0].Points) != 2 ||
		series[0].Points[0].V != 2 || !series[0].Points[1].At.Equal(time.Unix(1794220950, 0)) {
		t.Fatalf("series = %+v", series)
	}
}

func TestLokiLines(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("limit") != "50" || r.URL.Query().Get("direction") != "backward" {
			t.Errorf("query = %v", r.URL.Query())
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"streams","result":[
			{"stream":{"service":"ledger-worker","container":"c-ledger-worker-2"},"values":[
				["1794294725000000000","{\"event\":\"ledger.recover\",\"transaction_id\":\"tx-1\",\"outcome\":\"success\"}"],
				["1794294724000000000","not json"]]}]}}`))
	}))
	defer srv.Close()
	lines, err := console.NewLoki(srv.URL, time.Second).Lines(context.Background(), `{service="ledger-worker"}`, time.Unix(0, 0), time.Now(), 50, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(lines) != 1 || lines[0].Fields["transaction_id"] != "tx-1" || lines[0].Labels["container"] != "c-ledger-worker-2" ||
		!lines[0].At.Equal(time.Unix(0, 1794294725000000000)) {
		t.Fatalf("lines = %+v", lines)
	}
}

// Loki down: an error for each call, but one dependency.call per outage,
// not one per poll; logged again after it recovers and fails again.
func TestLokiFailureIsLoggedOncePerOutage(t *testing.T) {
	var up atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !up.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`))
	}))
	defer srv.Close()
	logs := &bytes.Buffer{}
	ctx := lokiCtx(logs)
	c := console.NewLoki(srv.URL, time.Second)
	for range 5 {
		if _, err := c.Matrix(ctx, "q", time.Now(), time.Now(), time.Minute); err == nil {
			t.Fatal("no error while Loki answers 503")
		}
	}
	if n := strings.Count(logs.String(), `"event":"dependency.call"`); n != 1 {
		t.Fatalf("%d dependency.call events for one outage, want 1:\n%s", n, logs)
	}
	if !strings.Contains(logs.String(), `"dependency":"loki"`) || !strings.Contains(logs.String(), `"reason":"unknown"`) {
		t.Fatalf("event fields:\n%s", logs)
	}
	up.Store(true)
	if _, err := c.Matrix(ctx, "q", time.Now(), time.Now(), time.Minute); err != nil {
		t.Fatal(err)
	}
	up.Store(false)
	_, _ = c.Matrix(ctx, "q", time.Now(), time.Now(), time.Minute)
	if n := strings.Count(logs.String(), `"event":"dependency.call"`); n != 2 {
		t.Fatalf("%d events after a second outage, want 2", n)
	}
}
