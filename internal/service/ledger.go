package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"auth-api/internal/idempotency"
	"auth-api/internal/ledger"
	"auth-api/internal/logger"
	"auth-api/internal/repository"
)

const (
	maxReference = 64
	maxMetadata  = 4096
)

var (
	ErrAccountNotFound      = errors.New("account not found")
	ErrUserNotFound         = errors.New("user not found")
	ErrLedgerExists         = errors.New("account already has a ledger for this currency")
	ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")
	ErrRequestInProgress    = errors.New("a request with this idempotency key is still in progress")
	ErrTransactionNotFound  = errors.New("transaction not found")

	// ErrIdempotencyKeyInvalid is a ValidationError the API reports as
	// idempotency_key_required (TRD §6.4).
	ErrIdempotencyKeyInvalid = &ValidationError{Msg: "Idempotency-Key header is required: 1-255 characters of [A-Za-z0-9._:-]"}
)

var (
	uuidPattern  = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	tracePattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// LedgerStore is what LedgerService needs from the ledger repository.
type LedgerStore interface {
	InTx(ctx context.Context, timeout time.Duration, fn func(repository.LedgerTx) error) error
	GetAccount(ctx context.Context, id string) (repository.AccountRecord, error)
	GetAccountByUserID(ctx context.Context, userID string) (repository.AccountRecord, error)
	ListUserLedgers(ctx context.Context, accountID string) ([]repository.LedgerRecord, error)
	GetTransaction(ctx context.Context, id string) (repository.TransactionRecord, error)
	ListTransactions(ctx context.Context, q repository.TransactionQuery) ([]repository.TransactionRecord, error)
}

type LedgerConfig struct {
	Currencies       []string      // accepted currencies; the first is opened with every account
	MaxAmount        int64         // LEDGER_MAX_AMOUNT
	ProcessTimeout   time.Duration // LEDGER_PROCESS_TIMEOUT: bound on Tx1 and on Tx2
	IdempotencyTTL   time.Duration // IDEMPOTENCY_TTL
	OverdraftClients []string      // LEDGER_OVERDRAFT_CLIENTS: may set overdraft_limit > 0
}

type LedgerService struct {
	store LedgerStore
	cfg   LedgerConfig
	now   func() time.Time
}

func NewLedgerService(store LedgerStore, cfg LedgerConfig) (*LedgerService, error) {
	if len(cfg.Currencies) == 0 {
		return nil, errors.New("ledger: at least one currency is required")
	}
	for _, c := range cfg.Currencies {
		if !ledger.ValidCurrency(c) {
			return nil, fmt.Errorf("ledger: invalid currency %q", c)
		}
	}
	if cfg.MaxAmount < 1 || cfg.MaxAmount > 1<<61 {
		return nil, errors.New("ledger: max amount must be between 1 and 2^61")
	}
	if cfg.ProcessTimeout <= 0 || cfg.IdempotencyTTL <= 0 {
		return nil, errors.New("ledger: process timeout and idempotency TTL must be positive")
	}
	return &LedgerService{store: store, cfg: cfg, now: time.Now}, nil
}

// EnsureSystemLedgers creates any missing double-entry counterparties
// (topup_clearing, settlement) for every configured currency. Migration 002
// seeds them for IDR only; ledger-api calls this at startup. It is safe to
// run concurrently from several replicas.
func (s *LedgerService) EnsureSystemLedgers(ctx context.Context) error {
	for _, currency := range s.cfg.Currencies {
		for _, code := range []string{ledger.CounterpartyTopupClearing, ledger.CounterpartySettlement} {
			err := s.store.InTx(ctx, s.cfg.ProcessTimeout, func(tx repository.LedgerTx) error {
				return tx.InsertLedger(ctx, repository.LedgerRecord{ID: ledger.NewID(), Kind: repository.LedgerSystem, Code: code, Currency: currency})
			})
			if errors.Is(err, repository.ErrLedgerExists) {
				continue
			}
			if err != nil {
				return fmt.Errorf("ensure system ledger %s/%s: %w", code, currency, err)
			}
			logger.FromContext(ctx).InfoContext(ctx, "system ledger created", "event", "ledger.account_open",
				"outcome", "success", "code", code, "currency", currency)
		}
	}
	return nil
}

// Request carries what every mutating call needs besides its own fields.
type Request struct {
	ClientID       string // authenticated internal caller
	IdempotencyKey string // raw header value; only its hash is stored
	RequestID      string // echoed in stored error bodies
	TraceID        string // 32 hex; written to every row
}

func (r Request) validate() error {
	switch {
	case r.ClientID == "":
		return &ValidationError{Msg: "client id is required"}
	case !idempotency.ValidKey(r.IdempotencyKey):
		return ErrIdempotencyKeyInvalid
	case !tracePattern.MatchString(r.TraceID):
		return &ValidationError{Msg: "trace id must be 32 lowercase hex characters"}
	}
	return nil
}

// --- reads (no events: the access log covers them) ---

// Account returns an account with its ledgers' committed balances.
func (s *LedgerService) Account(ctx context.Context, id string) (AccountView, error) {
	if !uuidPattern.MatchString(id) {
		return AccountView{}, ErrAccountNotFound
	}
	a, err := s.store.GetAccount(ctx, id)
	return s.accountView(ctx, a, err)
}

// AccountByUser returns the user's account (1:1).
func (s *LedgerService) AccountByUser(ctx context.Context, userID string) (AccountView, error) {
	if !uuidPattern.MatchString(userID) {
		return AccountView{}, ErrAccountNotFound
	}
	a, err := s.store.GetAccountByUserID(ctx, userID)
	return s.accountView(ctx, a, err)
}

func (s *LedgerService) accountView(ctx context.Context, a repository.AccountRecord, err error) (AccountView, error) {
	if errors.Is(err, repository.ErrNotFound) {
		return AccountView{}, ErrAccountNotFound
	}
	if err != nil {
		return AccountView{}, err
	}
	ledgers, err := s.store.ListUserLedgers(ctx, a.ID)
	if err != nil {
		return AccountView{}, err
	}
	return ToAccountView(a, ledgers), nil
}

// Transaction returns one transaction in its current state.
func (s *LedgerService) Transaction(ctx context.Context, id string) (TransactionView, error) {
	if !uuidPattern.MatchString(id) {
		return TransactionView{}, ErrTransactionNotFound
	}
	t, err := s.store.GetTransaction(ctx, id)
	if errors.Is(err, repository.ErrNotFound) {
		return TransactionView{}, ErrTransactionNotFound
	}
	if err != nil {
		return TransactionView{}, err
	}
	return ToTransactionView(t), nil
}

// --- account and ledger open ---

type OpenAccountCommand struct {
	Request
	UserID string
}

// OpenAccount opens the user's account with a ledger in the default currency
// (201), or returns the existing one (200): one user, one account.
func (s *LedgerService) OpenAccount(ctx context.Context, cmd OpenAccountCommand) (Response, error) {
	log := logger.FromContext(ctx)
	deny := func(reason string, err error) (Response, error) {
		log.WarnContext(ctx, "account open denied", "event", "ledger.account_open", "outcome", "denied",
			"reason", reason, "client_id", cmd.ClientID)
		return Response{}, err
	}
	if err := cmd.Request.validate(); err != nil {
		return deny("invalid_input", err)
	}
	if !uuidPattern.MatchString(cmd.UserID) {
		return deny("invalid_input", &ValidationError{Msg: "user_id must be a UUID"})
	}
	fp, err := idempotency.Fingerprint("account_open", "", map[string]string{"user_id": cmd.UserID})
	if err != nil {
		return Response{}, err
	}
	keyHash := idempotency.HashKey(cmd.IdempotencyKey)
	currency := s.cfg.Currencies[0]

	var resp Response
	var opened repository.AccountRecord
	attempt := func() error {
		return s.store.InTx(ctx, s.cfg.ProcessTimeout, func(tx repository.LedgerTx) error {
			existing, err := tx.GetAccountByUserID(ctx, cmd.UserID)
			if err != nil && !errors.Is(err, repository.ErrNotFound) {
				return err
			}
			exists := err == nil
			accountID := ledger.NewID()
			if exists {
				accountID = existing.ID
			}
			if replay, err := s.claim(ctx, tx, cmd.Request, keyHash, fp, "account_open", accountID); err != nil || replay != nil {
				if replay != nil {
					resp = *replay
				}
				return err
			}

			status := http.StatusOK
			if !exists {
				status = http.StatusCreated
				ledgerID := ledger.NewID()
				if err := tx.InsertAccount(ctx, repository.AccountRecord{
					ID: accountID, UserID: cmd.UserID, Status: ledger.AccountStatusActive, Version: 2,
				}); err != nil {
					return err
				}
				if err := tx.InsertLedger(ctx, repository.LedgerRecord{
					ID: ledgerID, Kind: repository.LedgerUser, AccountID: accountID, Currency: currency,
				}); err != nil {
					return err
				}
				for i, p := range []ledger.Payload{
					ledger.AccountOpened{UserID: cmd.UserID},
					ledger.LedgerOpened{LedgerID: ledgerID, Currency: currency},
				} {
					if _, err := tx.AppendEvent(ctx, ledger.Event{StreamID: accountID, StreamVersion: int64(i + 1),
						AccountID: accountID, Payload: p, TraceID: cmd.TraceID}, ledgerID); err != nil {
						return err
					}
				}
			}
			acct, err := tx.LockAccount(ctx, accountID, repository.LockUpdate)
			if err != nil {
				return err
			}
			ledgers, err := tx.ListUserLedgers(ctx, accountID)
			if err != nil {
				return err
			}
			opened = acct
			if resp, err = marshalResponse(status, map[string]AccountView{"account": ToAccountView(acct, ledgers)}); err != nil {
				return err
			}
			return tx.CompleteIdempotencyKey(ctx, cmd.ClientID, keyHash, resp.Status, resp.Body)
		})
	}

	err = attempt()
	if errors.Is(err, repository.ErrAccountExists) {
		// A concurrent open for the same user won; the retry finds and returns it.
		err = attempt()
	}
	switch {
	case errors.Is(err, repository.ErrUserNotFound):
		return deny("user_not_found", ErrUserNotFound)
	case s.idempotencyOutcome(ctx, err, resp, "account_open"):
		return resp, err
	case err != nil:
		log.ErrorContext(ctx, "account open failed", "event", "ledger.account_open", "outcome", "failure",
			"reason", "pg_error", "error", err, "client_id", cmd.ClientID)
		return Response{}, err
	}
	log.InfoContext(ctx, "account opened", "event", "ledger.account_open", "outcome", "success",
		"account_id", opened.ID, "user_id", opened.UserID, "client_id", cmd.ClientID, "created", resp.Status == http.StatusCreated)
	return resp, nil
}

type OpenLedgerCommand struct {
	Request
	AccountID      string
	Currency       string
	OverdraftLimit int64
}

// OpenLedger adds a sub-ledger in another currency (201). Only clients in
// LEDGER_OVERDRAFT_CLIENTS may set an overdraft; for others it is forced to 0.
func (s *LedgerService) OpenLedger(ctx context.Context, cmd OpenLedgerCommand) (Response, error) {
	log := logger.FromContext(ctx)
	deny := func(reason string, err error) (Response, error) {
		log.WarnContext(ctx, "ledger open denied", "event", "ledger.account_open", "outcome", "denied",
			"reason", reason, "account_id", cmd.AccountID, "currency", cmd.Currency, "client_id", cmd.ClientID)
		return Response{}, err
	}
	if err := cmd.Request.validate(); err != nil {
		return deny("invalid_input", err)
	}
	if !uuidPattern.MatchString(cmd.AccountID) {
		return deny("account_not_found", ErrAccountNotFound)
	}
	if !slices.Contains(s.cfg.Currencies, cmd.Currency) {
		return deny("currency_unsupported", &ValidationError{Msg: "currency is not supported"})
	}
	if cmd.OverdraftLimit < 0 || cmd.OverdraftLimit > s.cfg.MaxAmount {
		return deny("invalid_input", &ValidationError{Msg: "overdraft_limit is out of range"})
	}
	if !slices.Contains(s.cfg.OverdraftClients, cmd.ClientID) {
		cmd.OverdraftLimit = 0
	}
	fp, err := idempotency.Fingerprint("ledger_open", cmd.AccountID,
		map[string]any{"currency": cmd.Currency, "overdraft_limit": cmd.OverdraftLimit})
	if err != nil {
		return Response{}, err
	}
	keyHash := idempotency.HashKey(cmd.IdempotencyKey)
	ledgerID := ledger.NewID()

	var resp Response
	err = s.store.InTx(ctx, s.cfg.ProcessTimeout, func(tx repository.LedgerTx) error {
		acct, err := tx.LockAccount(ctx, cmd.AccountID, repository.LockUpdate)
		if err != nil {
			return err
		}
		if replay, err := s.claim(ctx, tx, cmd.Request, keyHash, fp, "ledger_open", ledgerID); err != nil || replay != nil {
			if replay != nil {
				resp = *replay
			}
			return err
		}
		if err := tx.InsertLedger(ctx, repository.LedgerRecord{ID: ledgerID, Kind: repository.LedgerUser,
			AccountID: acct.ID, Currency: cmd.Currency, OverdraftLimit: cmd.OverdraftLimit}); err != nil {
			return err
		}
		version := acct.Version + 1
		if _, err := tx.AppendEvent(ctx, ledger.Event{StreamID: acct.ID, StreamVersion: version, AccountID: acct.ID,
			Payload: ledger.LedgerOpened{LedgerID: ledgerID, Currency: cmd.Currency, OverdraftLimit: cmd.OverdraftLimit},
			TraceID: cmd.TraceID}, ledgerID); err != nil {
			return err
		}
		if err := tx.UpdateAccount(ctx, acct.ID, acct.Status, version); err != nil {
			return err
		}
		ledgers, err := tx.ListUserLedgers(ctx, acct.ID)
		if err != nil {
			return err
		}
		for _, l := range ledgers {
			if l.ID == ledgerID {
				if resp, err = marshalResponse(http.StatusCreated, map[string]LedgerView{"ledger": ToLedgerView(l)}); err != nil {
					return err
				}
			}
		}
		return tx.CompleteIdempotencyKey(ctx, cmd.ClientID, keyHash, resp.Status, resp.Body)
	})
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return deny("account_not_found", ErrAccountNotFound)
	case errors.Is(err, repository.ErrLedgerExists):
		return deny("ledger_exists", ErrLedgerExists)
	case s.idempotencyOutcome(ctx, err, resp, "ledger_open"):
		return resp, err
	case err != nil:
		log.ErrorContext(ctx, "ledger open failed", "event", "ledger.account_open", "outcome", "failure",
			"reason", "pg_error", "error", err, "account_id", cmd.AccountID, "client_id", cmd.ClientID)
		return Response{}, err
	}
	log.InfoContext(ctx, "ledger opened", "event", "ledger.account_open", "outcome", "success",
		"account_id", cmd.AccountID, "ledger_id", ledgerID, "currency", cmd.Currency, "client_id", cmd.ClientID)
	return resp, nil
}

// --- credit and debit ---

type MoneyCommand struct {
	Request
	AccountID    string
	Amount       int64
	Currency     string
	Counterparty string          // system ledger code; "" for the type's default
	Reference    string          // caller's reference; stored, never logged
	Metadata     json.RawMessage // JSON object ≤ 4 KiB; stored, never logged
}

// Credit adds funds (FR-01). It settles even on a frozen account (FR-09).
func (s *LedgerService) Credit(ctx context.Context, cmd MoneyCommand) (Response, error) {
	return s.submit(ctx, ledger.Credit, cmd)
}

// Debit deducts funds, failing with insufficient_funds below the floor
// (FR-02) or account_frozen on a frozen account (FR-09).
func (s *LedgerService) Debit(ctx context.Context, cmd MoneyCommand) (Response, error) {
	return s.submit(ctx, ledger.Debit, cmd)
}

func eventName(t ledger.TransactionType) string { return "ledger." + strings.ToLower(string(t)) }

// verb is the lower-case type: the idempotency operation, and the log
// message prefix ("credit settled", "debit denied").
func verb(t ledger.TransactionType) string { return strings.ToLower(string(t)) }

func (s *LedgerService) validateMoney(typ ledger.TransactionType, cmd *MoneyCommand) (reason string, err error) {
	if err := cmd.Request.validate(); err != nil {
		return "invalid_input", err
	}
	if !uuidPattern.MatchString(cmd.AccountID) {
		return "account_not_found", ErrAccountNotFound
	}
	if err := ledger.ValidateAmount(cmd.Amount, s.cfg.MaxAmount); err != nil {
		return "invalid_input", &ValidationError{Msg: fmt.Sprintf("amount must be an integer from 1 to %d", s.cfg.MaxAmount)}
	}
	if !slices.Contains(s.cfg.Currencies, cmd.Currency) {
		return "currency_unsupported", &ValidationError{Msg: "currency is not supported"}
	}
	if cmd.Counterparty == "" {
		cmd.Counterparty = ledger.DefaultCounterparty(typ)
	}
	if utf8.RuneCountInString(cmd.Reference) > maxReference {
		return "invalid_input", &ValidationError{Msg: fmt.Sprintf("reference must be at most %d characters", maxReference)}
	}
	if len(cmd.Metadata) > 0 {
		var obj map[string]any
		if len(cmd.Metadata) > maxMetadata || json.Unmarshal(cmd.Metadata, &obj) != nil || obj == nil {
			return "invalid_input", &ValidationError{Msg: fmt.Sprintf("metadata must be a JSON object of at most %d bytes", maxMetadata)}
		}
	}
	return "", nil
}

func (s *LedgerService) submit(ctx context.Context, typ ledger.TransactionType, cmd MoneyCommand) (Response, error) {
	log := logger.FromContext(ctx)
	event := eventName(typ)
	attrs := func(extra ...any) []any {
		return append([]any{"event", event, "account_id", cmd.AccountID, "amount", cmd.Amount,
			"currency", cmd.Currency, "client_id", cmd.ClientID}, extra...)
	}
	deny := func(reason string, err error) (Response, error) {
		log.WarnContext(ctx, verb(typ)+" denied", attrs("outcome", "denied", "reason", reason)...)
		return Response{}, err
	}

	if reason, err := s.validateMoney(typ, &cmd); err != nil {
		return deny(reason, err)
	}
	if _, err := s.store.GetAccount(ctx, cmd.AccountID); errors.Is(err, repository.ErrNotFound) {
		return deny("account_not_found", ErrAccountNotFound)
	} else if err != nil {
		log.ErrorContext(ctx, verb(typ)+" failed", attrs("outcome", "failure", "reason", "pg_error", "error", err)...)
		return Response{}, err
	}

	fp, err := idempotency.Fingerprint(verb(typ), cmd.AccountID, struct {
		Amount       int64           `json:"amount"`
		Currency     string          `json:"currency"`
		Counterparty string          `json:"counterparty"`
		Reference    string          `json:"reference,omitempty"`
		Metadata     json.RawMessage `json:"metadata,omitempty"`
	}{cmd.Amount, cmd.Currency, cmd.Counterparty, cmd.Reference, cmd.Metadata})
	if err != nil {
		return Response{}, err
	}
	keyHash := idempotency.HashKey(cmd.IdempotencyKey)
	txID := ledger.NewID()

	// Tx1: claim the key and record the request. Once this commits the request
	// is durable, and a client retry maps to the same transaction.
	var replay *Response
	errUnknownCounterparty := errors.New("unknown counterparty")
	err = s.store.InTx(ctx, s.cfg.ProcessTimeout, func(tx repository.LedgerTx) error {
		cp, err := tx.GetSystemLedger(ctx, cmd.Counterparty, cmd.Currency)
		if errors.Is(err, repository.ErrNotFound) {
			return errUnknownCounterparty
		} else if err != nil {
			return err
		}
		if replay, err = s.claim(ctx, tx, cmd.Request, keyHash, fp, verb(typ), txID); err != nil || replay != nil {
			return err
		}
		if err := tx.InsertTransaction(ctx, repository.TransactionRecord{
			ID: txID, AccountID: cmd.AccountID, CounterpartyLedgerID: cp.ID, Type: typ, Status: ledger.StatusPending,
			Amount: cmd.Amount, Currency: cmd.Currency, Reference: cmd.Reference, Metadata: cmd.Metadata,
			RequestedBy: cmd.ClientID, Version: 1, TraceID: cmd.TraceID,
		}); err != nil {
			return err
		}
		_, err = tx.AppendEvent(ctx, ledger.Event{StreamID: txID, StreamVersion: 1, AccountID: cmd.AccountID,
			Payload: ledger.TransactionRequested{Type: typ, Amount: cmd.Amount, Currency: cmd.Currency,
				Counterparty: cmd.Counterparty, Reference: cmd.Reference, RequestedBy: cmd.ClientID},
			TraceID: cmd.TraceID}, "")
		return err
	})
	var resp Response
	if replay != nil {
		resp = *replay
	}
	switch {
	case errors.Is(err, errUnknownCounterparty):
		return deny("invalid_input", &ValidationError{Msg: "counterparty is not a system ledger for this currency"})
	case s.idempotencyOutcome(ctx, err, resp, verb(typ)):
		return resp, err
	case err != nil:
		log.ErrorContext(ctx, verb(typ)+" failed", attrs("outcome", "failure", "reason", "pg_error", "error", err)...)
		return Response{}, err
	}

	// Tx2 on the request path. If it doesn't finish, the request stays PENDING
	// (it's durable) and the sweeper completes it; the client gets 202.
	resp, err = s.Process(ctx, ProcessInput{TransactionID: txID, ClientID: cmd.ClientID, KeyHash: keyHash, RequestID: cmd.RequestID})
	if err != nil {
		reason := "tx2_error"
		if errors.Is(err, repository.ErrTimeout) {
			reason = "tx2_timeout"
		}
		log.ErrorContext(ctx, verb(typ)+" deferred to recovery", attrs("outcome", "partial", "reason", reason,
			"error", err, "transaction_id", txID)...)
		return s.pendingResponse(ctx, txID, typ, cmd)
	}
	return resp, nil
}

// pendingResponse is the 202 for a request whose Tx1 committed but whose
// Tx2 did not. It is not stored: the stored response is written when the
// transaction reaches a terminal state.
func (s *LedgerService) pendingResponse(ctx context.Context, txID string, typ ledger.TransactionType, cmd MoneyCommand) (Response, error) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()
	t, err := s.store.GetTransaction(rctx, txID)
	if err != nil {
		t = repository.TransactionRecord{ID: txID, AccountID: cmd.AccountID, Type: typ, Status: ledger.StatusPending,
			Amount: cmd.Amount, Currency: cmd.Currency, Reference: cmd.Reference, TraceID: cmd.TraceID, CreatedAt: s.now()}
	}
	return renderTransaction(t, cmd.RequestID)
}

// ProcessInput identifies a PENDING transaction and the idempotency key whose
// stored response Tx2 completes.
type ProcessInput struct {
	TransactionID string
	ClientID      string
	KeyHash       string
	RequestID     string
}

// Process is Tx2 (TRD §7.1), the single implementation shared by the request
// path and the P2 sweeper. Lock order: transaction → account → ledger. It
// logs the terminal outcome after commit, exactly once.
func (s *LedgerService) Process(ctx context.Context, in ProcessInput) (Response, error) {
	var (
		resp    Response
		final   repository.TransactionRecord
		already bool
	)
	err := s.store.InTx(ctx, s.cfg.ProcessTimeout, func(tx repository.LedgerTx) error {
		rec, err := tx.LockTransaction(ctx, in.TransactionID)
		if err != nil {
			return err
		}
		if rec.Status != ledger.StatusPending {
			// Another worker finished it; report its outcome without re-logging.
			already, final = true, rec
			resp, err = renderTransaction(rec, in.RequestID)
			return err
		}
		acct, err := tx.LockAccount(ctx, rec.AccountID, repository.LockShare)
		if err != nil {
			return err
		}
		l, err := tx.LockUserLedger(ctx, rec.AccountID, rec.Currency)
		found := err == nil
		if err != nil && !errors.Is(err, repository.ErrNotFound) {
			return err
		}

		version := rec.Version
		appendEvent := func(p ledger.Payload) error {
			version++
			_, err := tx.AppendEvent(ctx, ledger.Event{StreamID: rec.ID, StreamVersion: version, AccountID: rec.AccountID,
				Payload: p, TraceID: rec.TraceID}, l.ID)
			return err
		}
		if err := appendEvent(ledger.TransactionProcessing{LedgerID: l.ID}); err != nil {
			return err
		}
		d, err := ledger.Decide(ledger.DecideInput{
			Tx: ledger.Transaction{ID: rec.ID, AccountID: rec.AccountID, Status: rec.Status, Version: rec.Version,
				Type: rec.Type, Amount: rec.Amount, Currency: rec.Currency},
			AccountStatus: acct.Status, LedgerFound: found, Balance: l.Balance, OverdraftLimit: l.OverdraftLimit,
		})
		if err != nil {
			return err
		}

		update := repository.TransactionUpdate{ID: rec.ID, LedgerID: l.ID}
		if d.Settle {
			seq := l.LastSequence + 1
			user := repository.JournalLine{ID: ledger.NewID(), TransactionID: rec.ID, LedgerID: l.ID, Amount: d.Delta,
				Currency: rec.Currency, LedgerSequence: &seq, BalanceAfter: &d.BalanceAfter, TraceID: rec.TraceID}
			counter := repository.JournalLine{ID: ledger.NewID(), TransactionID: rec.ID, LedgerID: rec.CounterpartyLedgerID,
				Amount: -d.Delta, Currency: rec.Currency, TraceID: rec.TraceID}
			if err := tx.PostJournal(ctx, user, counter); err != nil {
				return err
			}
			if err := tx.UpdateLedgerBalance(ctx, l.ID, d.BalanceAfter, seq); err != nil {
				return err
			}
			if err := appendEvent(ledger.TransactionSettled{JournalEntryIDs: []string{user.ID, counter.ID},
				LedgerSequence: seq, BalanceAfter: d.BalanceAfter}); err != nil {
				return err
			}
			update.Status, update.Settled = ledger.StatusSettled, true
		} else {
			if err := appendEvent(ledger.TransactionFailed{Reason: d.Reason}); err != nil {
				return err
			}
			update.Status, update.FailureReason = ledger.StatusFailed, d.Reason
		}
		update.Version = version
		if final, err = tx.UpdateTransaction(ctx, update); err != nil {
			return err
		}
		if d.Settle {
			final.LedgerSequence, final.BalanceAfter = l.LastSequence+1, d.BalanceAfter
		}
		if resp, err = renderTransaction(final, in.RequestID); err != nil {
			return err
		}
		return tx.CompleteIdempotencyKey(ctx, in.ClientID, in.KeyHash, resp.Status, resp.Body)
	})
	if err != nil {
		return Response{}, err
	}
	if !already {
		logTerminal(ctx, final)
	}
	return resp, nil
}

func logTerminal(ctx context.Context, t repository.TransactionRecord) {
	event := eventName(t.Type)
	attrs := []any{"event", event, "transaction_id", t.ID, "account_id", t.AccountID, "ledger_id", t.LedgerID,
		"amount", t.Amount, "currency", t.Currency, "client_id", t.RequestedBy}
	log := logger.FromContext(ctx)
	if t.Status == ledger.StatusSettled {
		log.InfoContext(ctx, verb(t.Type)+" settled", append(attrs, "outcome", "success",
			"ledger_sequence", t.LedgerSequence, "balance_after", t.BalanceAfter)...)
		return
	}
	log.WarnContext(ctx, verb(t.Type)+" failed", append(attrs, "outcome", "denied", "reason", string(t.FailureReason))...)
}

// --- idempotency ---

// claim takes the idempotency key for this request inside tx. It returns a
// stored response to replay, ErrIdempotencyKeyReused, ErrRequestInProgress,
// or (nil, nil) when the key is now ours.
func (s *LedgerService) claim(ctx context.Context, tx repository.LedgerTx, r Request, keyHash, fp, op, resourceID string) (*Response, error) {
	ok, err := tx.InsertIdempotencyKey(ctx, repository.IdempotencyKey{ClientID: r.ClientID, KeyHash: keyHash,
		Fingerprint: fp, Operation: op, ResourceID: resourceID, ExpiresAt: s.now().Add(s.cfg.IdempotencyTTL)})
	if err != nil || ok {
		return nil, err
	}
	k, err := tx.GetIdempotencyKey(ctx, r.ClientID, keyHash)
	switch {
	case err != nil:
		return nil, err
	case k.Fingerprint != fp || k.Operation != op:
		return nil, ErrIdempotencyKeyReused
	case k.ResponseBody == nil:
		return nil, ErrRequestInProgress
	}
	return &Response{Status: k.ResponseStatus, Body: k.ResponseBody, Replayed: true}, nil
}

// idempotencyOutcome logs ledger.idempotency for a replay or a key conflict
// and reports whether err/resp was one.
func (s *LedgerService) idempotencyOutcome(ctx context.Context, err error, resp Response, op string) bool {
	log := logger.FromContext(ctx)
	switch {
	case err == nil && resp.Replayed:
		log.InfoContext(ctx, "idempotent replay", "event", "ledger.idempotency", "outcome", "success",
			"operation", op, "status", resp.Status)
	case errors.Is(err, ErrIdempotencyKeyReused):
		log.WarnContext(ctx, "idempotency key reused", "event", "ledger.idempotency", "outcome", "denied",
			"reason", "key_reused", "operation", op)
	case errors.Is(err, ErrRequestInProgress):
		log.WarnContext(ctx, "idempotent request in progress", "event", "ledger.idempotency", "outcome", "denied",
			"reason", "in_progress", "operation", op)
	default:
		return false
	}
	return true
}
