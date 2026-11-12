#!/usr/bin/env python3
"""A walk-through of one user's money on the local stack (`make up`).

    python3 scripts/demo/account_story.py --email you@example.com

It plays three acts against ledger-api, as the real callers would (the
onboarding, payments and ops clients), and prints what happened in plain words:

  1. Top-up: a credit settles; the same request retried is applied once.
  2. Processing and accounting: purchases settle, one is refused for lack of
     funds, and each settled one is a balanced double entry.
  3. Disputes: a disputed purchase is refunded by a void (once, never twice);
     a charged-back top-up that was already spent is refused, the account is
     frozen while it's investigated, and the chargeback settles once money
     arrives.

It ends by checking the books: the account's journal lines sum to its balance,
and every transaction's two lines sum to zero. Every step shows up in the web
console (http://localhost:8090), linked at the end. Those screens need the ops
role: make grant-role EMAIL=you@example.com.

The user must exist (register in the console first). The account is opened if
it has none. Tokens default to the dev tokens in .env.example and are never
printed. Writes real transactions: run it on a local stack only.
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

LEDGER = os.environ.get("LEDGER_URL", "http://localhost:8081")
CONSOLE = os.environ.get("CONSOLE_URL", "http://localhost:8090")
TOKENS = {
    "onboarding": os.environ.get("ONBOARDING_TOKEN", "dev-onboarding-token"),
    "payments": os.environ.get("PAYMENTS_TOKEN", "dev-payments-token"),
    "ops": os.environ.get("OPS_TOKEN", "dev-ops-token"),
    "read": os.environ.get("READ_TOKEN", "dev-console-token"),
}
CUR = "IDR"


# --- plumbing -------------------------------------------------------------

def call(method, path, client, body=None, key=None):
    """One ledger-api call. Returns (status, json, headers); never raises on 4xx."""
    req = urllib.request.Request(LEDGER + path, method=method,
                                 data=None if body is None else json.dumps(body).encode())
    req.add_header("Authorization", "Bearer " + TOKENS[client])
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"{}"), r.headers
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}"), e.headers
    except urllib.error.URLError as e:
        sys.exit(f"ledger-api isn't reachable at {LEDGER} ({e.reason}). Start the stack with `make up`.")


def money(n):
    return f"{CUR} {n:,}"


def say(text=""):
    print(text)


def step(text):
    print(f"\n  ▸ {text}")


def ok(text):
    print(f"    ✓ {text}")


failures = 0


def no(text):
    global failures
    failures += 1
    print(f"    ✗ {text}")


def act(n, title):
    print(f"\n{'=' * 72}\nAct {n}: {title}\n{'=' * 72}")


def new_key(what):
    return f"sim-{what}-{uuid.uuid4()}"


def psql(sql, **params):
    """Runs one read-only query in the stack's Postgres and returns its rows.
    Values are passed as psql variables (:'name'), never spliced in."""
    args = ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", os.environ.get("POSTGRES_USER", "auth"),
            "-d", os.environ.get("POSTGRES_DB", "auth"), "-X", "-q", "-At", "-F", "|"]
    for k, v in params.items():
        args += ["-v", f"{k}={v}"]
    try:
        out = subprocess.run(args, input=sql + "\n", capture_output=True, text=True, timeout=30, check=True).stdout
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired) as e:
        sys.exit(f"Couldn't query the stack's Postgres: {e}. Run this from the repo folder with the stack up.")
    return [l.split("|") for l in out.splitlines() if l]


def user_id_for(email):
    """The user's id (auth-api has no lookup by email)."""
    rows = psql("SELECT id FROM users WHERE email = :'email'", email=email)
    if not rows:
        sys.exit("No user with that email in the local auth-api. Register it in the console first "
                 f"({CONSOLE}/register), then run this again.")
    return rows[0][0]


def wait_for_fraud_window(account_id):
    """The story makes 3 top-ups. The fraud engine freezes an account after
    more than FRAUD_TOPUP_MAX (3) in FRAUD_TOPUP_WINDOW (5 minutes), so a
    rerun within 5 minutes would be frozen halfway through. Stop instead."""
    window = int(os.environ.get("FRAUD_TOPUP_WINDOW_S", "300"))
    rows = psql("SELECT count(*), coalesce(ceil(extract(epoch FROM max(created_at) + make_interval(secs => :'w') - now())), 0) "
                "FROM ledger.transactions WHERE account_id = :'a' AND type = 'CREDIT' "
                "AND created_at > now() - make_interval(secs => :'w')", a=account_id, w=window)
    n, wait = int(rows[0][0]), int(float(rows[0][1]))
    if n:
        sys.exit(f"This account had {n} top-up(s) in the last {window // 60} minutes. The story's 3 more would trip the "
                 f"fraud engine's top-up limit and freeze it halfway. Run it again in {wait} s, or use another user.")


def account_of(user_id):
    s, body, _ = call("GET", f"/internal/v1/accounts?user_id={user_id}", "read")
    if s == 200:
        return body["account"], False
    s, body, _ = call("POST", "/internal/v1/accounts", "onboarding", {"user_id": user_id}, new_key("open"))
    if s not in (200, 201):
        sys.exit(f"Couldn't open an account: {s} {body}")
    return body["account"], True


def balance(account_id):
    _, body, _ = call("GET", f"/internal/v1/accounts/{account_id}", "read")
    return next((l["balance"] for l in body["account"]["ledgers"] if l["currency"] == CUR), 0)


def settle(status, body):
    """A 202 means the transaction is PENDING: the recovery job completes it.
    Wait for its final state so the story stays readable."""
    tx = body.get("transaction", {})
    for _ in range(30):
        if status != 202:
            return status, tx
        time.sleep(1)
        s, b, _ = call("GET", f"/internal/v1/transactions/{tx['id']}", "read")
        tx = b.get("transaction", tx)
        if tx.get("status") not in ("PENDING", "PROCESSING"):
            return 201, tx
    return status, tx


def credit(account_id, amount, what, key=None):
    s, body, h = call("POST", f"/internal/v1/accounts/{account_id}/credits", "payments",
                      {"amount": amount, "currency": CUR}, key or new_key(what))
    s, tx = settle(s, body)
    return s, tx, body, h


def debit(account_id, amount, what, client="payments"):
    s, body, _ = call("POST", f"/internal/v1/accounts/{account_id}/debits", client,
                      {"amount": amount, "currency": CUR}, new_key(what))
    s, tx = settle(s, body)
    return s, tx, body


def void(tx_id, reason):
    s, body, _ = call("POST", f"/internal/v1/transactions/{tx_id}/void", "ops", {"reason": reason}, new_key("void"))
    s, tx = settle(s, body)
    return s, tx, body


def refusal(body):
    err = body.get("error", {})
    return err.get("code", "?")


def journal(tx_id):
    _, body, _ = call("GET", f"/internal/v1/transactions/{tx_id}/detail", "read")
    return body.get("journal", [])


def show_entry(tx_id, indent="      "):
    lines = journal(tx_id)
    for l in lines:
        side = f"account …{l['account_id'][-4:]}" if l["ledger_kind"] == "USER" else f"system: {l.get('ledger_code') or 'ledger'}"
        direction = "in " if l["amount"] > 0 else "out"
        print(f"{indent}{side:<26} money {direction} {money(abs(l['amount'])):>16}")
    total = sum(l["amount"] for l in lines)
    print(f"{indent}{'lines sum to':<26}           {total:>16}  {'(balanced)' if total == 0 else '(NOT BALANCED)'}")


# --- the story ------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--email", required=True, help="a user registered in the local auth-api")
    args = ap.parse_args()

    uid = user_id_for(args.email)
    acct, opened = account_of(uid)
    aid = acct["id"]
    if acct.get("status") == "FROZEN":
        sys.exit("The account is frozen. Unfreeze it first, or use another user.")
    wait_for_fraud_window(aid)
    start = balance(aid)
    say(f"User …{uid[-4:]}  account {aid}  ({'opened just now' if opened else 'already open'})")
    say(f"Starting balance: {money(start)}")
    involved = []

    # ---------------------------------------------------------------- 1
    act(1, "a top-up")
    step("The payment provider confirms a top-up of IDR 500,000 and calls the ledger.")
    key = new_key("topup")
    s, topup, _, _ = credit(aid, 500_000, "topup", key)
    involved.append(topup["id"])
    ok(f"{topup['status']} (HTTP {s}). Balance after: {money(topup.get('balance_after', 0))}")
    say("    It was recorded first (Requested), then applied (Settled), as one double entry:")
    show_entry(topup["id"])

    step("The provider's call timed out on its side, so it sends the same request again (same Idempotency-Key).")
    s, again, _, h = credit(aid, 500_000, "topup", key)
    replayed = h.get("Idempotent-Replayed") == "true"
    if again["id"] == topup["id"] and replayed:
        ok(f"Answered from the record (Idempotent-Replayed: true), same transaction …{again['id'][-4:]}. Not applied twice.")
    else:
        no(f"Unexpected: {s} replayed={replayed} id={again['id']}")
    ok(f"Balance still {money(balance(aid))}")

    # ---------------------------------------------------------------- 2
    act(2, "spending, and how each payment is accounted")
    purchases = [("coffee", 45_000), ("groceries", 230_500)]
    bought = {}
    for what, amount in purchases:
        step(f"Purchase: {what}, {money(amount)}.")
        s, tx, body = debit(aid, amount, what)
        if s == 201:
            bought[what] = tx
            involved.append(tx["id"])
            ok(f"{tx['status']}. Balance after: {money(tx['balance_after'])}")
        else:
            no(f"Refused: {refusal(body)}")
    if "groceries" in bought:
        say("    The groceries payment, as booked: money out of the account, into the settlement ledger.")
        show_entry(bought["groceries"]["id"])

    too_much = balance(aid) + 1
    step(f"Purchase: a laptop for {money(too_much)}, one rupiah more than the balance.")
    s, tx, body = debit(aid, too_much, "laptop")
    if s == 422 and refusal(body) == "insufficient_funds":
        if tx:
            involved.append(tx["id"])
        ok("Refused: insufficient_funds. The attempt is kept as a FAILED transaction; nothing moved.")
    else:
        no(f"Unexpected: {s} {body}")
    ok(f"Balance unchanged: {money(balance(aid))}")

    # ---------------------------------------------------------------- 3
    act(3, "disputes")
    g = bought.get("groceries")
    if g:
        step("The customer disputes the groceries payment (never delivered). Ops refunds it with a void.")
        s, v, body = void(g["id"], "customer dispute: groceries order never delivered")
        if s == 201:
            involved.append(v["id"])
            ok(f"Refunded by void …{v['id'][-4:]} ({v['status']}). Balance after: {money(v['balance_after'])}")
            say("    Nothing is deleted or edited: the original stays on record as VOIDED, and the void")
            say("    is a new transaction with the opposite entry:")
            show_entry(v["id"])
        else:
            no(f"Void refused: {refusal(body)}")

        step("The same dispute is submitted again by mistake.")
        s, _, body = void(g["id"], "customer dispute: duplicate submission")
        if s == 422 and refusal(body) == "not_voidable":
            ok("Refused: not_voidable. A payment can be refunded once, never twice.")
        else:
            no(f"Unexpected: {s} {body}")

    step("A second top-up of IDR 200,000 arrives by card.")
    s, card, _, _ = credit(aid, 200_000, "card-topup")
    involved.append(card["id"])
    ok(f"{card['status']}. Balance after: {money(card['balance_after'])}")
    spend = balance(aid) - 50_000
    step(f"The customer spends {money(spend)} of it right away, leaving {money(50_000)}.")
    s, tx, body = debit(aid, spend, "spend-all")
    if s == 201:
        involved.append(tx["id"])
        ok(f"{tx['status']}. Balance after: {money(tx['balance_after'])}")
    else:
        no(f"Unexpected: {s} {refusal(body)}")

    step("The card issuer charges back the IDR 200,000 top-up (stolen card). Ops tries to reverse it.")
    s, _, body = void(card["id"], "card chargeback: issuer reports a stolen card")
    if s == 422 and refusal(body) == "insufficient_funds":
        ok("Refused: insufficient_funds. The money is already spent, and the ledger never goes negative.")
    else:
        no(f"Unexpected: {s} {body}")

    step("Ops freezes the account while the chargeback is investigated.")
    s, body, _ = call("POST", f"/internal/v1/accounts/{aid}/freeze", "ops",
                      {"reason": "chargeback investigation"}, new_key("freeze"))
    if s == 200:
        ok("FROZEN: spending is blocked; money can still come in.")
    else:
        no(f"Freeze failed: {s} {body}")
    s, tx, body = debit(aid, 10_000, "while-frozen")
    if s == 422 and refusal(body) == "account_frozen":
        if tx:
            involved.append(tx["id"])
        ok("A purchase of IDR 10,000 is refused: account_frozen.")
    else:
        no(f"Unexpected: {s} {body}")

    step("The customer settles the debt with a bank transfer of IDR 150,000.")
    s, transfer, _, _ = credit(aid, 150_000, "bank-transfer")
    involved.append(transfer["id"])
    ok(f"{transfer['status']} while frozen. Balance after: {money(transfer['balance_after'])}")

    step("Ops reverses the charged-back top-up again; there's enough money now.")
    s, cb, body = void(card["id"], "card chargeback: issuer reports a stolen card")
    if s == 201:
        involved.append(cb["id"])
        ok(f"Chargeback settled by void …{cb['id'][-4:]}. Balance after: {money(cb['balance_after'])}")
    else:
        no(f"Void refused: {refusal(body)}")

    step("Dispute closed. Ops unfreezes the account.")
    s, body, _ = call("POST", f"/internal/v1/accounts/{aid}/unfreeze", "ops",
                      {"reason": "chargeback recovered"}, new_key("unfreeze"))
    ok("ACTIVE again." if s == 200 else f"Unfreeze failed: {s} {body}")

    # ---------------------------------------------------------------- books
    act(4, "do the books add up?")
    rows, cursor = [], ""
    while True:
        s, page, _ = call("GET", f"/internal/v1/accounts/{aid}/transactions?limit=100"
                          + (f"&cursor={cursor}" if cursor else ""), "read")
        rows += page.get("transactions", [])
        cursor = page.get("next_cursor", "")
        if not cursor:
            break
    user_sum, unbalanced = 0, 0
    for t in rows:
        lines = journal(t["id"])
        if lines and sum(l["amount"] for l in lines) != 0:
            unbalanced += 1
        user_sum += sum(l["amount"] for l in lines if l["ledger_kind"] == "USER" and l["account_id"] == aid)
    final = balance(aid)
    by_status = {}
    for t in rows:
        by_status[t["status"]] = by_status.get(t["status"], 0) + 1
    say(f"  The account has {len(rows)} transactions: " + ", ".join(f"{n} {s}" for s, n in sorted(by_status.items())))
    (ok if unbalanced == 0 else no)(f"Every transaction's two journal lines sum to zero ({unbalanced} don't)")
    (ok if user_sum == final else no)(f"The account's journal lines sum to {money(user_sum)}; its balance is {money(final)}")
    say(f"  This run: {money(start)} → {money(final)}")
    say("  The reconciler (ledger-worker) re-checks all of this on its own, every minute.")

    say(f"\nSee it in the console (signed in with the ops role: make grant-role):\n  {CONSOLE}/accounts/{aid}")
    for tid in involved[:3]:
        say(f"  {CONSOLE}/transactions/{tid}")
    say(f"  …and the rest on the account page, or live on {CONSOLE}/")
    if failures:
        sys.exit(f"\n{failures} step(s) didn't go as the story expects (marked ✗ above).")


if __name__ == "__main__":
    main()
