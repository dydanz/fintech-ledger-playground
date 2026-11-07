package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"unicode/utf8"

	"auth-api/internal/idempotency"
	"auth-api/internal/ledger"
	"auth-api/internal/logger"
	"auth-api/internal/repository"
)

// FraudRules are the rule names a freeze may cite (logging rule: `rule` comes
// from a fixed set).
var FraudRules = []string{"topup_velocity", "debit_volume"}

type FreezeCommand struct {
	Request
	AccountID string
	Reason    string // free text; stored in the event, never logged
	Rule      string // optional; one of FraudRules
}

type UnfreezeCommand struct {
	Request
	AccountID string
	Reason    string
}

// Freeze blocks debits on the account from the moment it commits (FR-09):
// it takes the account row FOR UPDATE, which waits for every in-flight Tx2
// holding it FOR SHARE (TRD §7.2). Credits and voids keep settling. Freezing
// a frozen account is an idempotent 200 that appends nothing.
func (s *LedgerService) Freeze(ctx context.Context, cmd FreezeCommand) (Response, error) {
	return s.setAccountStatus(ctx, statusChange{op: "freeze", target: ledger.AccountStatusFrozen,
		req: cmd.Request, accountID: cmd.AccountID, reason: cmd.Reason, rule: cmd.Rule})
}

// Unfreeze reactivates a frozen account. Which clients may call it is
// limited by configuration (LEDGER_UNFREEZE_CLIENTS, TRD §16 Q6).
func (s *LedgerService) Unfreeze(ctx context.Context, cmd UnfreezeCommand) (Response, error) {
	return s.setAccountStatus(ctx, statusChange{op: "unfreeze", target: ledger.AccountStatusActive,
		req: cmd.Request, accountID: cmd.AccountID, reason: cmd.Reason})
}

type statusChange struct {
	op        string // freeze | unfreeze: the idempotency operation and event suffix
	target    ledger.AccountStatus
	req       Request
	accountID string
	reason    string
	rule      string
}

func (c statusChange) attrs(extra ...any) []any {
	a := []any{"event", "ledger." + c.op, "account_id", c.accountID, "actor", c.req.ClientID, "client_id", c.req.ClientID}
	if c.rule != "" {
		a = append(a, "rule", c.rule)
	}
	return append(a, extra...)
}

func (s *LedgerService) setAccountStatus(ctx context.Context, c statusChange) (Response, error) {
	log := logger.FromContext(ctx)
	deny := func(reason string, err error) (Response, error) {
		log.WarnContext(ctx, c.op+" denied", c.attrs("outcome", "denied", "reason", reason)...)
		return Response{}, err
	}
	if err := c.req.validate(); err != nil {
		return deny("invalid_input", err)
	}
	if n := utf8.RuneCountInString(c.reason); n < 1 || n > maxReference {
		return deny("invalid_input", &ValidationError{Msg: fmt.Sprintf("reason is required, at most %d characters", maxReference)})
	}
	if c.rule != "" && !slices.Contains(FraudRules, c.rule) {
		rule := c.rule
		c.rule = "" // never log an unknown, caller-supplied value
		return deny("invalid_input", &ValidationError{Msg: fmt.Sprintf("rule %q is not one of %v", rule, FraudRules)})
	}
	if !uuidPattern.MatchString(c.accountID) {
		return deny("account_not_found", ErrAccountNotFound)
	}
	fp, err := idempotency.Fingerprint(c.op, c.accountID, map[string]string{"reason": c.reason, "rule": c.rule})
	if err != nil {
		return Response{}, err
	}
	keyHash := idempotency.HashKey(c.req.IdempotencyKey)
	if resp, ok := s.cachedReplay(ctx, c.req, keyHash, fp, c.op); ok {
		s.idempotencyOutcome(ctx, nil, resp, c.op)
		return resp, nil
	}

	var (
		resp    Response
		done    repository.IdempotencyKey
		changed bool
	)
	err = s.store.InTx(ctx, s.cfg.ProcessTimeout, func(tx repository.LedgerTx) error {
		acct, err := tx.LockAccount(ctx, c.accountID, repository.LockUpdate)
		if err != nil {
			return err
		}
		if stored, err := s.claim(ctx, tx, c.req, keyHash, fp, c.op, acct.ID); err != nil || stored != nil {
			if stored != nil {
				done, resp = *stored, replayOf(*stored)
			}
			return err
		}
		if acct.Status != c.target {
			var p ledger.Payload = ledger.AccountUnfrozen{Reason: c.reason, Actor: c.req.ClientID}
			if c.target == ledger.AccountStatusFrozen {
				p = ledger.AccountFrozen{Reason: c.reason, Actor: c.req.ClientID, Rule: c.rule}
			}
			version := acct.Version + 1
			if _, err := tx.AppendEvent(ctx, ledger.Event{StreamID: acct.ID, StreamVersion: version, AccountID: acct.ID,
				Payload: p, TraceID: c.req.TraceID}, ""); err != nil {
				return err
			}
			if err := tx.UpdateAccount(ctx, acct.ID, c.target, version); err != nil {
				return err
			}
			acct.Status, acct.Version, changed = c.target, version, true
		}
		ledgers, err := tx.ListUserLedgers(ctx, acct.ID)
		if err != nil {
			return err
		}
		if resp, err = marshalResponse(http.StatusOK, map[string]AccountView{"account": ToAccountView(acct, ledgers)}); err != nil {
			return err
		}
		done, err = tx.CompleteIdempotencyKey(ctx, c.req.ClientID, keyHash, resp.Status, resp.Body)
		return err
	})
	switch {
	case errors.Is(err, repository.ErrNotFound):
		return deny("account_not_found", ErrAccountNotFound)
	case s.idempotencyOutcome(ctx, err, resp, c.op):
		if err == nil {
			s.remember(ctx, done)
		}
		return resp, err
	case err != nil:
		reason := "pg_error"
		if errors.Is(err, repository.ErrTimeout) {
			reason = "timeout"
		}
		log.ErrorContext(ctx, c.op+" failed", c.attrs("outcome", "failure", "reason", reason, "error", err)...)
		return Response{}, err
	}
	s.remember(ctx, done)
	log.InfoContext(ctx, "account "+map[string]string{"freeze": "frozen", "unfreeze": "unfrozen"}[c.op],
		c.attrs("outcome", "success", "changed", changed)...)
	return resp, nil
}
