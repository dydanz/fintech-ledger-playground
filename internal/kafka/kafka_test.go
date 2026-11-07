package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"auth-api/internal/kafka"
	"auth-api/internal/logger"
)

func TestSplitBrokers(t *testing.T) {
	if got := kafka.SplitBrokers(" a:9092, ,b:9092 "); strings.Join(got, "|") != "a:9092|b:9092" {
		t.Fatalf("got %v", got)
	}
}

// With no broker listening, Connect logs dependency.connect for kafka at
// warn with reason refused, and gives up when ctx ends.
func TestConnectLogsRetries(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens here now

	p, err := kafka.NewProducer([]string{addr}, "t")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	buf := &bytes.Buffer{}
	log := logger.New(buf, slog.LevelDebug, "ledger-worker")
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if err := p.Connect(ctx, log); err == nil {
		t.Fatal("connected to nothing")
	}
	var warned bool
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]any
		if json.Unmarshal([]byte(line), &m) != nil {
			t.Fatalf("not JSON: %q", line)
		}
		if m["event"] == "dependency.connect" && m["dependency"] == "kafka" && m["level"] == "warn" && m["outcome"] == "failure" {
			warned = true
			if m["reason"] != "refused" {
				t.Errorf("reason = %v, want refused", m["reason"])
			}
		}
	}
	if !warned {
		t.Fatalf("no dependency.connect warning:\n%s", buf)
	}
}
