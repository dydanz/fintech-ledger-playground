package ledger_test

import (
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"fintech-ledger-playground/internal/ledger"
)

// op is one randomly generated request against a single ledger.
type op struct {
	Kind   uint8 // 0 credit, 1 debit, 2 void, 3 freeze toggle
	Amount int64 // 1..1000
	Target uint8 // which earlier transaction a void aims at
}

type ops []op

func (ops) Generate(r *rand.Rand, size int) reflect.Value {
	out := make(ops, r.Intn(size*4+1))
	for i := range out {
		out[i] = op{Kind: uint8(r.Intn(4)), Amount: 1 + r.Int63n(1000), Target: uint8(r.Intn(256))}
	}
	return reflect.ValueOf(out)
}

// TestLedgerInvariants runs random request sequences through Decide and the
// transaction state machine, the way Tx2 does, and checks (TRD §13.1):
//   - the balance never drops below the overdraft floor;
//   - the balance equals the sum of settled deltas (the journal fold), and
//     every settled balance_after equals the running sum at that point;
//   - folding each transaction's event stream gives exactly the state that
//     processing produced (projection == fold);
//   - a transaction is voided at most once, and only from SETTLED.
func TestLedgerInvariants(t *testing.T) {
	const overdraft = 200

	property := func(seq ops) bool {
		var (
			balance, seq0 int64
			frozen        bool
			txs           []ledger.Transaction // live projection, by index
			streams       = map[string][]ledger.Event{}
			sumDeltas     int64
		)
		emit := func(tx *ledger.Transaction, p ledger.Payload) bool {
			e := ledger.Event{ID: ledger.NewID(), StreamID: tx.ID, StreamVersion: tx.Version + 1, AccountID: accountID, Payload: p, TraceID: traceID}
			next, err := ledger.ApplyTransaction(*tx, e)
			if err != nil {
				t.Logf("apply %s: %v", e.Type(), err)
				return false
			}
			*tx = next
			streams[tx.ID] = append(streams[tx.ID], e)
			return true
		}

		for _, o := range seq {
			if o.Kind == 3 {
				frozen = !frozen
				continue
			}
			typ := []ledger.TransactionType{ledger.Credit, ledger.Debit, ledger.Void}[o.Kind]
			req := ledger.TransactionRequested{Type: typ, Amount: o.Amount, Currency: "IDR", RequestedBy: "prop"}
			var orig *ledger.Transaction
			if typ == ledger.Void {
				if len(txs) == 0 {
					continue
				}
				orig = &txs[int(o.Target)%len(txs)]
				req.Amount, req.OriginalTransactionID = orig.Amount, orig.ID
			}

			tx := ledger.Transaction{ID: ledger.NewID()}
			if !emit(&tx, req) {
				return false
			}
			status := ledger.AccountStatusActive
			if frozen {
				status = ledger.AccountStatusFrozen
			}
			in := ledger.DecideInput{Tx: tx, AccountStatus: status, LedgerFound: true, Balance: balance, OverdraftLimit: overdraft}
			if orig != nil {
				snapshot := *orig
				in.Original = &snapshot
			}
			d, err := ledger.Decide(in)
			if err != nil {
				t.Logf("decide: %v", err)
				return false
			}
			if !emit(&tx, ledger.TransactionProcessing{LedgerID: ledgerID}) {
				return false
			}
			if d.Settle {
				seq0++
				balance = d.BalanceAfter
				sumDeltas += d.Delta
				if balance != sumDeltas || balance < -overdraft {
					t.Logf("balance %d, sum of deltas %d", balance, sumDeltas)
					return false
				}
				if !emit(&tx, ledger.TransactionSettled{LedgerSequence: seq0, BalanceAfter: balance}) {
					return false
				}
				if orig != nil && !emit(orig, ledger.TransactionVoided{VoidTransactionID: tx.ID}) {
					return false
				}
			} else if !emit(&tx, ledger.TransactionFailed{Reason: d.Reason}) {
				return false
			}
			txs = append(txs, tx)
		}

		// Every stream folds back to the projection that processing built.
		for _, tx := range txs {
			folded, err := ledger.FoldTransaction(streams[tx.ID])
			if err != nil || folded != tx {
				t.Logf("fold %s: %v\n got  %+v\n want %+v", tx.ID, err, folded, tx)
				return false
			}
		}
		return true
	}
	if err := quick.Check(property, &quick.Config{MaxCount: 300}); err != nil {
		t.Fatal(err)
	}
}
