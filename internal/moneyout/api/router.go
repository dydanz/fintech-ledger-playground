// Package api is money-out-middleware's read-only operational HTTP API
// (P§3.6), on the repo's shared base engine.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/gin-gonic/gin"

	coreapi "fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/config"
	"fintech-ledger-playground/internal/ledger"
	"fintech-ledger-playground/internal/moneyout/contract"
	"fintech-ledger-playground/internal/moneyout/repository"
)

// TransferReader reads a transfer and its attempts.
type TransferReader interface {
	GetTransfer(ctx context.Context, id string) (repository.Transfer, []repository.Attempt, error)
}

type Deps struct {
	Logger         *slog.Logger
	Transfers      TransferReader
	Ready          func(ctx context.Context) error // the database, for /readyz
	TrustedProxies []string
	Clients        []config.MoneyOutClient // MOM_CLIENTS: required, every ops route needs one
}

func NewRouter(d Deps) (*gin.Engine, error) {
	if len(d.Clients) == 0 {
		return nil, errors.New("moneyout api: no clients configured")
	}
	r, err := coreapi.NewBaseEngine(d.Logger, d.TrustedProxies, d.Ready)
	if err != nil {
		return nil, err
	}
	h := &handlers{transfers: d.Transfers}
	ops := r.Group("/", requireClient(d.Clients))
	ops.GET("/transfers/:transfer_id", h.getTransfer)
	return r, nil
}

type handlers struct{ transfers TransferReader }

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type transferView struct {
	TransferID        string               `json:"transfer_id"`
	State             string               `json:"state"`
	Version           int64                `json:"version"`
	Amount            contract.Amount      `json:"amount"`
	Beneficiary       contract.Beneficiary `json:"beneficiary"`
	CoreTransactionID string               `json:"core_transaction_id,omitempty"`
	CoreAck           string               `json:"core_ack"`
	CurrentAttempt    int                  `json:"current_attempt"`
	Routing           json.RawMessage      `json:"routing,omitempty"`
	RequestedAt       time.Time            `json:"requested_at"`
	CreatedAt         time.Time            `json:"created_at"`
	UpdatedAt         time.Time            `json:"updated_at"`
}

type attemptView struct {
	AttemptNo      int       `json:"attempt_no"`
	RequestID      string    `json:"request_id"`
	BankMiddleware string    `json:"bank_middleware"`
	Rail           string    `json:"rail"`
	Status         string    `json:"status"`
	Reason         string    `json:"reason,omitempty"`
	RetryElsewhere bool      `json:"retry_elsewhere"`
	ReachedBank    bool      `json:"reached_bank"`
	BankReference  string    `json:"bank_reference,omitempty"`
	BankRawCode    string    `json:"bank_raw_code,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

// GET /transfers/:transfer_id: the current state, the attempts and the
// routing decision.
func (h *handlers) getTransfer(c *gin.Context) {
	id := c.Param("transfer_id")
	if !uuidPattern.MatchString(id) {
		coreapi.AbortError(c, http.StatusNotFound, "transfer_not_found", "transfer not found")
		return
	}
	t, attempts, err := h.transfers.GetTransfer(c.Request.Context(), id)
	if errors.Is(err, repository.ErrNotFound) {
		coreapi.AbortError(c, http.StatusNotFound, "transfer_not_found", "transfer not found")
		return
	}
	if err != nil {
		_ = c.Error(err)
		coreapi.AbortError(c, http.StatusServiceUnavailable, "unavailable", "money-out-middleware temporarily unavailable")
		return
	}
	view := transferView{TransferID: t.ID, State: string(t.State), Version: t.Version,
		Amount:      contract.AmountOf(ledger.Money{Amount: t.Amount, Currency: t.Currency}),
		Beneficiary: contract.Beneficiary{BankCode: t.BankCode, AccountNo: t.AccountNo}, CoreTransactionID: t.CoreTransactionID,
		CoreAck: t.CoreAck, CurrentAttempt: t.CurrentAttempt, Routing: t.Routing, RequestedAt: t.RequestedAt,
		CreatedAt: t.CreatedAt, UpdatedAt: t.UpdatedAt}
	views := make([]attemptView, 0, len(attempts))
	for _, a := range attempts {
		views = append(views, attemptView{AttemptNo: a.AttemptNo, RequestID: a.RequestID, BankMiddleware: a.BankMiddleware,
			Rail: a.Rail, Status: a.Status, Reason: a.Reason, RetryElsewhere: a.RetryElsewhere, ReachedBank: a.ReachedBank,
			BankReference: a.BankReference, BankRawCode: a.BankRawCode, CreatedAt: a.CreatedAt, UpdatedAt: a.UpdatedAt})
	}
	c.JSON(http.StatusOK, gin.H{"transfer": view, "attempts": views})
}
