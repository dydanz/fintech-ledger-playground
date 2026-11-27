package bankmw_test

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
)

// A call this process canceled (shutdown) isn't a bank timeout: it's logged
// as canceled, so the bank.call panel doesn't show a fake bank fault.
func TestDoCanceledIsNotTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer srv.Close()
	for name, c := range map[string]struct {
		client *http.Client
		cancel bool
		reason string
	}{
		"canceled by us":    {&http.Client{Timeout: 5 * time.Second}, true, "canceled"},
		"the bank too slow": {&http.Client{Timeout: 50 * time.Millisecond}, false, "timeout"},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			ctx, cancel := context.WithCancel(logger.WithContext(context.Background(), logger.New(&logs, slog.LevelDebug, "b")))
			defer cancel()
			if c.cancel {
				time.AfterFunc(50*time.Millisecond, cancel)
			}
			hr, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, nil)
			r := bankmw.Do(ctx, c.client, "remit-status", hr, contract.TransferRequest{}, func([]byte) string { return "" })
			if r.Err == nil || r.NotReached {
				t.Fatalf("response = %+v", r)
			}
			if !strings.Contains(logs.String(), `"reason":"`+c.reason+`"`) {
				t.Fatalf("want reason %s: %s", c.reason, logs.String())
			}
		})
	}
}
