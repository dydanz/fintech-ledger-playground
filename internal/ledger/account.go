package ledger

import "fmt"

// Account is the fold of one account stream: the state the ledger.accounts
// projection must equal. Ledgers maps currency to the USER ledger id.
type Account struct {
	ID      string
	UserID  string
	Status  AccountStatus
	Version int64
	Ledgers map[string]string
}

// ApplyAccount returns s with e applied:
//
//	(none) ─AccountOpened→ ACTIVE ⇄ FROZEN   (AccountFrozen / AccountUnfrozen)
//	LedgerOpened: once per currency, in either state.
//
// Freezing a frozen account is illegal here; the service treats that request
// as an idempotent no-op and appends nothing (TRD §7.2).
func ApplyAccount(s Account, e Event) (Account, error) {
	if s.Version > 0 && e.StreamID != s.ID {
		return s, ErrWrongStream
	}
	if e.StreamVersion != s.Version+1 {
		return s, fmt.Errorf("%w: have %d, got %d", ErrVersionMismatch, s.Version, e.StreamVersion)
	}
	illegal := func() (Account, error) {
		return s, fmt.Errorf("%w: %s in state %q", ErrIllegalTransition, e.Type(), s.Status)
	}

	next := s
	switch p := e.Payload.(type) {
	case AccountOpened:
		if s.Status != AccountStatusNone {
			return illegal()
		}
		next = Account{ID: e.StreamID, UserID: p.UserID, Status: AccountStatusActive, Ledgers: map[string]string{}}
	case LedgerOpened:
		if s.Status == AccountStatusNone {
			return illegal()
		}
		if _, exists := s.Ledgers[p.Currency]; exists {
			return illegal()
		}
		next.Ledgers = make(map[string]string, len(s.Ledgers)+1)
		for c, id := range s.Ledgers {
			next.Ledgers[c] = id
		}
		next.Ledgers[p.Currency] = p.LedgerID
	case AccountFrozen:
		if s.Status != AccountStatusActive {
			return illegal()
		}
		next.Status = AccountStatusFrozen
	case AccountUnfrozen:
		if s.Status != AccountStatusFrozen {
			return illegal()
		}
		next.Status = AccountStatusActive
	default:
		return illegal()
	}
	next.Version = e.StreamVersion
	return next, nil
}

// FoldAccount is the left-fold of an account stream, in version order.
func FoldAccount(events []Event) (Account, error) {
	var s Account
	for _, e := range events {
		var err error
		if s, err = ApplyAccount(s, e); err != nil {
			return s, err
		}
	}
	return s, nil
}
