package console

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"

	"fintech-ledger-playground/internal/api"
	"fintech-ledger-playground/internal/logger"
)

// DemoConfig turns on the demo money actions on My account (D§7.3): opening
// the user's account, and credits and debits on it. They use the console's
// own service tokens, which never reach the browser.
type DemoConfig struct {
	Enabled         bool   // CONSOLE_DEMO_ACTIONS
	OnboardingToken string // opens the account; never logged
	PaymentsToken   string // credits and debits; never logged
}

const (
	// MaxDemoAmount caps one demo credit or debit (IDR, minor units).
	MaxDemoAmount = 1_000_000
	demoCurrency  = "IDR"
)

// The app sends a fresh key per click; the console namespaces it by account
// so two users' keys can never collide.
var clientKeyPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,100}$`)

type demo struct {
	cfg          DemoConfig
	auth, ledger *proxy
}

type upstreamReply struct {
	status int
	body   []byte
	ctype  string
}

// fetch makes one upstream call and reads the reply. ok is false once the
// caller has already answered (upstream unreachable: 503).
func (d *demo) fetch(c *gin.Context, p *proxy, method, target string, hdr http.Header, body []byte, operation string) (upstreamReply, bool) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
		hdr.Set("Content-Type", "application/json")
	}
	resp, ok := p.call(c, method, target, hdr, rd, operation)
	if !ok {
		return upstreamReply{}, false
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, maxUpstreamBody))
	return upstreamReply{status: resp.StatusCode, body: b, ctype: resp.Header.Get("Content-Type")}, true
}

func relay(c *gin.Context, r upstreamReply) {
	ct := r.ctype
	if ct == "" {
		ct = "application/json; charset=utf-8"
	}
	c.Data(r.status, ct, r.body)
}

func userHeader(c *gin.Context) http.Header {
	h := http.Header{}
	if v := c.GetHeader("Authorization"); v != "" {
		h.Set("Authorization", v)
	}
	return h
}

func serviceHeader(token, key string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	h.Set("Idempotency-Key", key)
	return h
}

func (d *demo) enabled(c *gin.Context) bool {
	if !d.cfg.Enabled {
		api.AbortError(c, http.StatusForbidden, "demo_actions_disabled", "demo actions are turned off on this console")
		return false
	}
	return true
}

// open creates the caller's account. The user id comes from auth-api with
// the caller's own token, so nobody can open an account for someone else.
// The key is fixed per user: a second open returns the same account.
func (d *demo) open(c *gin.Context) {
	if !d.enabled(c) {
		return
	}
	prof, ok := d.fetch(c, d.auth, "GET", "/api/v1/users/profile", userHeader(c), nil, "demo_profile")
	if !ok {
		return
	}
	var u struct {
		ID string `json:"id"`
	}
	if prof.status != http.StatusOK || json.Unmarshal(prof.body, &u) != nil || u.ID == "" {
		relay(c, prof) // 401 when the session ended
		return
	}
	body, _ := json.Marshal(map[string]string{"user_id": u.ID})
	res, ok := d.fetch(c, d.ledger, "POST", "/internal/v1/accounts", serviceHeader(d.cfg.OnboardingToken, "console-open-"+u.ID), body, "demo_open")
	if !ok {
		return
	}
	var out struct {
		Account struct {
			ID string `json:"id"`
		} `json:"account"`
	}
	_ = json.Unmarshal(res.body, &out)
	d.logAction(c, "open", res, u.ID, out.Account.ID, 0)
	relay(c, res)
}

// money returns the credit or debit action. The account is the caller's own,
// resolved by ledger-api from the caller's session; the request carries only
// the amount.
func (d *demo) money(kind string) gin.HandlerFunc {
	operation := "demo_" + kind
	return func(c *gin.Context) {
		if !d.enabled(c) {
			return
		}
		key := c.GetHeader("Idempotency-Key")
		if !clientKeyPattern.MatchString(key) {
			api.AbortError(c, http.StatusBadRequest, "invalid_input", "an Idempotency-Key of 1 to 100 letters, digits or ._:- is required")
			return
		}
		var req struct {
			Amount *int64 `json:"amount"`
		}
		dec := json.NewDecoder(c.Request.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil || req.Amount == nil || *req.Amount < 1 || *req.Amount > MaxDemoAmount {
			api.AbortError(c, http.StatusBadRequest, "invalid_input",
				fmt.Sprintf(`the body must be {"amount": n} with a whole number n from 1 to %d`, MaxDemoAmount))
			return
		}

		acct, ok := d.fetch(c, d.ledger, "GET", "/api/v1/ledger/account", userHeader(c), nil, "demo_account")
		if !ok {
			return
		}
		var a struct {
			Account struct {
				ID     string `json:"id"`
				UserID string `json:"user_id"`
			} `json:"account"`
		}
		if acct.status != http.StatusOK || json.Unmarshal(acct.body, &a) != nil || a.Account.ID == "" {
			relay(c, acct) // 401 session ended, 404 no account yet
			return
		}
		body, _ := json.Marshal(map[string]any{"amount": *req.Amount, "currency": demoCurrency})
		res, ok := d.fetch(c, d.ledger, "POST", "/internal/v1/accounts/"+a.Account.ID+"/"+kind+"s",
			serviceHeader(d.cfg.PaymentsToken, "console:"+a.Account.ID+":"+key), body, operation)
		if !ok {
			return
		}
		d.logAction(c, kind, res, a.Account.UserID, a.Account.ID, *req.Amount)
		relay(c, res)
	}
}

// logAction emits console.demo_action (C2): the record that this change
// came from the console on behalf of this user. ledger-api's own events
// carry client_id=payments or onboarding, not the end user. The outcome
// follows ledger-api's answer: 2xx success, a 4xx business refusal denied
// (reason = its code), anything else failure.
func (d *demo) logAction(c *gin.Context, action string, res upstreamReply, userID, accountID string, amount int64) {
	ctx := c.Request.Context()
	var r struct {
		Transaction struct {
			ID string `json:"id"`
		} `json:"transaction"`
		Error struct {
			Code          string `json:"code"`
			TransactionID string `json:"transaction_id"`
		} `json:"error"`
	}
	_ = json.Unmarshal(res.body, &r)
	attrs := []any{"event", "console.demo_action", "action", action, "user_id", userID, "account_id", accountID, "status", res.status}
	if amount > 0 {
		attrs = append(attrs, "amount", amount, "currency", demoCurrency)
	}
	if id := r.Transaction.ID + r.Error.TransactionID; id != "" {
		attrs = append(attrs, "transaction_id", id)
	}
	log := logger.FromContext(ctx)
	switch {
	case res.status >= 200 && res.status < 300:
		log.InfoContext(ctx, "demo action done", append(attrs, "outcome", "success")...)
	case res.status >= 400 && res.status < 500:
		reason := r.Error.Code
		if reason == "" {
			reason = "rejected"
		}
		log.WarnContext(ctx, "demo action refused", append(attrs, "outcome", "denied", "reason", reason)...)
	default:
		log.ErrorContext(ctx, "demo action failed", append(attrs, "outcome", "failure", "reason", "upstream_error")...)
	}
}
