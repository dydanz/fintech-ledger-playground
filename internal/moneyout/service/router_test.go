//go:build integration

package service_test

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/routing"
	"fintech-ledger-playground/internal/moneyout/service"
	"fintech-ledger-playground/internal/moneyout/transfer"
)

const routes = `
version: 1
currency: IDR
rail_order: [INTRABANK, BIFAST, RTOL, SKNBI, RTGS]
bank_middlewares:
  - {name: bmw-a, rails: [BIFAST, RTOL]}
  - {name: bmw-b, rails: [INTRABANK], home_bank: "014"}
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

type staticRoutes struct{ c *routing.Chain }

func (s staticRoutes) Chain() *routing.Chain { return s.c }

var wib = time.FixedZone("WIB", 7*60*60)

func (e *env) router(at time.Time) *service.Router {
	e.t.Helper()
	c, err := routing.Parse([]byte(routes))
	if err != nil {
		e.t.Fatal(err)
	}
	return service.NewRouter(e.store, staticRoutes{c}, func() time.Time { return at })
}

// receive stores a command whose beneficiary is at bank 014 (command's).
func (e *env) receive(amount string) string { return e.receiveAt("014", amount) }

// receiveAt stores a command for a beneficiary at bank.
func (e *env) receiveAt(bank, amount string) string {
	e.t.Helper()
	id := uuid.Must(uuid.NewV7()).String()
	cmd, err := contract.NewEvent(contract.EventTransferRequested, contract.StreamTransfer, id, 1, time.Now(), "4bf92f3577b34da6a3ce929d0e0e4736",
		contract.TransferRequested{Command: contract.CommandDoTransfer, TransferID: id, Amount: contract.Amount{Value: amount, Currency: "IDR"},
			Beneficiary: contract.Beneficiary{BankCode: bank, AccountNo: "0201245681"}, RequestedAt: time.Now().UTC()})
	if err != nil {
		e.t.Fatal(err)
	}
	if _, _, err := e.intake.Receive(e.ctx, cmd); err != nil {
		e.t.Fatal(err)
	}
	return id
}

type outboxRow struct {
	topic string
	env   contract.Envelope
}

func (e *env) outboxRows(transferID string) []outboxRow {
	e.t.Helper()
	rows, err := e.store.DB().Query(e.ctx, `SELECT topic, payload::text FROM outbox WHERE msg_key = $1 ORDER BY id`, transferID)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []outboxRow
	for rows.Next() {
		var topic, s string
		if err := rows.Scan(&topic, &s); err != nil {
			e.t.Fatal(err)
		}
		env, err := contract.DecodeEnvelope([]byte(s))
		if err != nil {
			e.t.Fatal(err)
		}
		out = append(out, outboxRow{topic, env})
	}
	return out
}

var midweek = time.Date(2026, 11, 25, 10, 0, 0, 0, wib)

// RECEIVED → ROUTED, with the decision stored on the transfer and journaled.
func TestRouterRoutesReceived(t *testing.T) {
	e := newEnv(t)
	id := e.receive("750965.00") // the command's beneficiary is at 014
	if err := e.router(midweek).Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	tr, _, err := e.store.GetTransfer(e.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if tr.State != transfer.Routed || tr.Version != 2 {
		t.Fatalf("state %s v%d, want ROUTED v2", tr.State, tr.Version)
	}
	var res routing.Result
	if err := json.Unmarshal(tr.Routing, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Candidates) == 0 || res.Candidates[0] != (routing.Candidate{BankMiddleware: "bmw-b", Rail: "INTRABANK"}) || res.ConfigVersion == "" {
		t.Fatalf("routing = %+v", res)
	}
	if len(res.Decisions) == 0 {
		t.Fatal("no decisions stored")
	}
	rows := e.outboxRows(id)
	last := rows[len(rows)-1]
	if last.topic != contract.TopicJournal || last.env.EventType != contract.EventTransferRouted || last.env.StreamVersion != 2 {
		t.Fatalf("last outbox row = %s %s v%d", last.topic, last.env.EventType, last.env.StreamVersion)
	}
	var p contract.JournalPayload
	_ = last.env.DecodePayload(&p)
	if p.FromState != "RECEIVED" || p.ToState != "ROUTED" || !strings.Contains(string(p.Detail), `"bmw-b"`) {
		t.Fatalf("payload = %+v detail %s", p, p.Detail)
	}
	if !strings.Contains(e.logs.String(), `"event":"transfer.route"`) || !strings.Contains(e.logs.String(), `"candidates":5`) {
		t.Fatalf("no transfer.route with candidates: %s", e.logs)
	}
}

// No candidate: ROUTED → FAILED, the journal event and the core's result in
// the same transaction, core_ack pending. At night a large amount is a
// cut-off.
func TestRouterNoCandidateFailsWithOneResult(t *testing.T) {
	e := newEnv(t)
	id := e.receiveAt("022", "300000000.00") // not intrabank: only SKNBI and RTGS fit, both closed
	night := time.Date(2026, 11, 25, 22, 0, 0, 0, wib)
	if err := e.router(night).Run(e.ctx); err != nil {
		t.Fatal(err)
	}
	tr, _, _ := e.store.GetTransfer(e.ctx, id)
	if tr.State != transfer.Failed || tr.Version != 3 || tr.CoreAck != "pending" {
		t.Fatalf("state %s v%d core_ack %s, want FAILED v3 pending", tr.State, tr.Version, tr.CoreAck)
	}
	var journal, results []outboxRow
	for _, r := range e.outboxRows(id) {
		switch r.topic {
		case contract.TopicJournal:
			journal = append(journal, r)
		case contract.TopicResults:
			results = append(results, r)
		}
	}
	if len(results) != 1 {
		t.Fatalf("%d results to the core, want 1", len(results))
	}
	var res contract.TransferResult
	if err := results[0].env.DecodePayload(&res); err != nil {
		t.Fatal(err)
	}
	if results[0].env.EventType != contract.EventTransferFailed || res.Status != contract.StatusFailed || res.Reason != contract.ReasonCutoff ||
		res.Attempts != 0 || res.TransferID != id {
		t.Fatalf("result = %s %+v", results[0].env.EventType, res)
	}
	if got := journal[len(journal)-1].env; got.EventType != contract.EventTransferFailed || got.StreamVersion != 3 {
		t.Fatalf("last journal event = %s v%d", got.EventType, got.StreamVersion)
	}
	if !strings.Contains(e.logs.String(), `"reason":"no_candidate"`) {
		t.Fatalf("no transfer.route denied: %s", e.logs)
	}
}

// Routing runs once per transfer: a second pass finds nothing to do.
func TestRouterIsIdempotent(t *testing.T) {
	e := newEnv(t)
	id := e.receive("750965.00")
	r := e.router(midweek)
	for i := 0; i < 3; i++ {
		if err := r.Run(e.ctx); err != nil {
			t.Fatal(err)
		}
	}
	tr, _, _ := e.store.GetTransfer(e.ctx, id)
	if tr.State != transfer.Routed || tr.Version != 2 {
		t.Fatalf("state %s v%d after three passes", tr.State, tr.Version)
	}
}

// Two MOM instances routing at once (P§5 #12's precursor): each transfer is
// routed exactly once.
func TestConcurrentRoutersRouteOnce(t *testing.T) {
	e := newEnv(t)
	var ids []string
	for i := 0; i < 20; i++ {
		ids = append(ids, e.receive("750965.00"))
	}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.router(midweek).Run(e.ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	for _, id := range ids {
		n := 0
		for _, r := range e.outboxRows(id) {
			if r.env.EventType == contract.EventTransferRouted {
				n++
			}
		}
		tr, _, _ := e.store.GetTransfer(e.ctx, id)
		if n != 1 || tr.State != transfer.Routed {
			t.Fatalf("%s: %d TransferRouted, state %s", id, n, tr.State)
		}
	}
}
