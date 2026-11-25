//go:build integration

package service_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"fintech-ledger-playground/internal/logger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
	"fintech-ledger-playground/internal/moneyout/service"
	"fintech-ledger-playground/internal/moneyout/transfer"
	"fintech-ledger-playground/internal/testutil/pgtest"
)

type env struct {
	t      *testing.T
	ctx    context.Context
	logs   *bytes.Buffer
	store  *repository.Store
	intake *service.Intake
}

func newEnv(t *testing.T) *env {
	t.Helper()
	logs := &bytes.Buffer{}
	store := repository.NewStore(pgtest.NewFrom(t, "migrations/money_out"))
	return &env{t: t, ctx: logger.WithContext(context.Background(), logger.New(logs, slog.LevelDebug, "money-out-middleware")),
		logs: logs, store: store, intake: service.NewIntake(store)}
}

// command builds a TransferRequested envelope with a fresh event id.
func command(t *testing.T, transferID, amount string) contract.Envelope {
	t.Helper()
	e, err := contract.NewEvent(contract.EventTransferRequested, contract.StreamTransfer, transferID, 1, time.Now(), "4bf92f3577b34da6a3ce929d0e0e4736",
		contract.TransferRequested{Command: contract.CommandDoTransfer, TransferID: transferID,
			Amount: contract.Amount{Value: amount, Currency: "IDR"}, Beneficiary: contract.Beneficiary{BankCode: "014", AccountNo: "0201245681"},
			RequestedAt: time.Now().UTC(), Metadata: json.RawMessage(`{"core_transaction_id":"01a1d000-0000-7000-8000-00000000c0de","remark":"INV-1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func (e *env) outbox() []contract.Envelope {
	e.t.Helper()
	rows, err := e.store.DB().Query(e.ctx, `SELECT payload::text FROM outbox ORDER BY id`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	var out []contract.Envelope
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			e.t.Fatal(err)
		}
		env, err := contract.DecodeEnvelope([]byte(s))
		if err != nil {
			e.t.Fatal(err)
		}
		out = append(out, env)
	}
	return out
}

func (e *env) events(name string) []map[string]any {
	e.t.Helper()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(e.logs.Bytes()))
	for sc.Scan() {
		var r map[string]any
		if json.Unmarshal(sc.Bytes(), &r) == nil && r["event"] == name {
			out = append(out, r)
		}
	}
	return out
}

func TestReceiveJournalsReceived(t *testing.T) {
	e := newEnv(t)
	id := uuid.NewString()
	tr, out, err := e.intake.Receive(e.ctx, command(t, id, "2500000.00"))
	if err != nil || out != service.Created {
		t.Fatalf("receive: %v %v", out, err)
	}
	if tr.State != transfer.Received || tr.Amount != 2500000 || tr.CoreTransactionID != "01a1d000-0000-7000-8000-00000000c0de" {
		t.Fatalf("transfer = %+v", tr)
	}
	ob := e.outbox()
	if len(ob) != 1 || ob[0].EventType != contract.EventTransferReceived || ob[0].StreamID != id || ob[0].StreamVersion != 1 {
		t.Fatalf("outbox = %+v", ob)
	}
	var p contract.JournalPayload
	if err := json.Unmarshal(ob[0].Payload, &p); err != nil || p.ToState != "RECEIVED" || p.CoreTransactionID != tr.CoreTransactionID {
		t.Fatalf("journal payload = %s", ob[0].Payload)
	}
	got := e.events("transfer.receive")
	if len(got) != 1 || got[0]["outcome"] != "success" || got[0]["transfer_id"] != id || got[0]["amount"] != 2500000.0 {
		t.Fatalf("transfer.receive = %v", got)
	}
	if sc := e.events("transfer.state_change"); len(sc) != 1 || sc[0]["to_state"] != "RECEIVED" {
		t.Fatalf("transfer.state_change = %v", sc)
	}
	if strings.Contains(e.logs.String(), "0201245681") {
		t.Fatal("full account number logged")
	}
}

// P§5 #2 at the gate: the same transfer_id and payload, as a new event (the
// core re-sent it), changes nothing.
func TestDuplicateCommandSamePayload(t *testing.T) {
	e := newEnv(t)
	id := uuid.NewString()
	e.intake.Receive(e.ctx, command(t, id, "2500000.00"))
	tr, out, err := e.intake.Receive(e.ctx, command(t, id, "2500000"))
	if err != nil || out != service.Duplicate || tr.Version != 1 {
		t.Fatalf("duplicate: %v v%d %v", out, tr.Version, err)
	}
	if n := len(e.outbox()); n != 1 {
		t.Fatalf("outbox = %d rows, want 1", n)
	}
	got := e.events("transfer.receive")
	if len(got) != 2 || got[1]["outcome"] != "success" || got[1]["duplicate"] != true {
		t.Fatalf("transfer.receive = %v", got)
	}
}

// The same event delivered twice (Kafka redelivery) is a duplicate too.
func TestRedeliveredEventIsDuplicate(t *testing.T) {
	e := newEnv(t)
	cmd := command(t, uuid.NewString(), "100.00")
	e.intake.Receive(e.ctx, cmd)
	if _, out, err := e.intake.Receive(e.ctx, cmd); err != nil || out != service.Duplicate {
		t.Fatalf("redelivery: %v %v", out, err)
	}
	if n := len(e.outbox()); n != 1 {
		t.Fatalf("outbox = %d rows", n)
	}
}

// P§5 #3: the same transfer_id with a different amount is a conflict: it's
// journaled, and the transfer doesn't change.
func TestSameIDDifferentAmountIsConflict(t *testing.T) {
	e := newEnv(t)
	id := uuid.NewString()
	e.intake.Receive(e.ctx, command(t, id, "2500000.00"))
	tr, out, err := e.intake.Receive(e.ctx, command(t, id, "2600000.00"))
	if err != nil || out != service.Conflict {
		t.Fatalf("conflict: %v %v", out, err)
	}
	if tr.State != transfer.Received || tr.Amount != 2500000 || tr.Version != 2 {
		t.Fatalf("transfer after conflict = %s %d v%d", tr.State, tr.Amount, tr.Version)
	}
	ob := e.outbox()
	if len(ob) != 2 || ob[1].EventType != contract.EventTransferRejected || ob[1].StreamVersion != 2 {
		t.Fatalf("outbox = %+v", ob)
	}
	got := e.events("transfer.receive")
	if len(got) != 2 || got[1]["outcome"] != "denied" || got[1]["reason"] != "conflict" || got[1]["level"] != "warn" {
		t.Fatalf("transfer.receive = %v", got)
	}
}

// An invalid command (a rail the core must not send, a fractional IDR amount)
// is refused and stores nothing.
func TestInvalidCommandRefused(t *testing.T) {
	e := newEnv(t)
	bad := command(t, uuid.NewString(), "100.50")
	withRail := command(t, uuid.NewString(), "100.00")
	withRail.Payload = json.RawMessage(strings.Replace(string(withRail.Payload), `"command"`, `"rail":"RTGS","command"`, 1))
	wrongType := command(t, uuid.NewString(), "100.00")
	wrongType.EventType = "TransferSucceeded"
	for name, cmd := range map[string]contract.Envelope{"fractional IDR": bad, "rail field": withRail, "wrong event type": wrongType} {
		if _, out, err := e.intake.Receive(e.ctx, cmd); err != nil || out != service.Invalid {
			t.Errorf("%s: %v %v, want Invalid", name, out, err)
		}
	}
	if n := len(e.outbox()); n != 0 {
		t.Fatalf("outbox = %d rows after invalid commands", n)
	}
	for _, r := range e.events("transfer.receive") {
		if r["outcome"] != "denied" || r["reason"] != "invalid_input" {
			t.Fatalf("transfer.receive = %v", r)
		}
	}
}
