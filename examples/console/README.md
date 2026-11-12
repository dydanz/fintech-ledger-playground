# Web console screenshots

These come from the console running on an isolated compose stack, most of them during chaos drills (`make chaos`). Each folder is named after the PR that added it. The [Operations Guide §6](<../../spec/2026-11-10 Operations Guide.md#6-web-console>) explains the screens and labels.

## Drills

| Screenshot | What it shows |
| :--- | :--- |
| [c3.2-monitor/kafka-active.png](c3.2-monitor/kafka-active.png) | **Kafka drill, during:** "Incident in progress: Kafka unavailable". Payments keep working, and the events-to-Kafka tile shows the backlog growing |
| [c3.2-monitor/kafka-resolved.png](c3.2-monitor/kafka-resolved.png) | **Kafka drill, after:** resolved. The queued events were delivered once Kafka was back, and the backlog is 0 |
| [c3.2-monitor/loki-down.png](c3.2-monitor/loki-down.png) | **Loki stopped:** the Monitor still shows the ledger's own numbers, says "Activity signals unavailable", and claims no incident or recovery it can't see |
| [c3.3-transaction-accounts/recovered-transaction.png](c3.3-transaction-accounts/recovered-transaction.png) | **Postgres drill:** a transaction caught mid-way, labelled Recovered. Its timeline shows the recovery job completing it after the restart, and its double entry balances |
| [c3.3-transaction-accounts/account.png](c3.3-transaction-accounts/account.png) | An account's page: balances, freeze history (by whom, under which rule) and its transactions |

The Monitor through a Postgres drill (during the outage, in progress, resolved), the fraud step, and each step's actor are in `c3-followup/`, added by [#69](https://github.com/dydanz/fintech-ledger-playground/pull/69).

## Screens

The `c2.*` captures predate the ops role ([#56](https://github.com/dydanz/fintech-ledger-playground/pull/56)). Their navigation shows every screen and an "ops viewer" tag to any signed-in user; since then, only ops users see the ops screens (`ops-access/`).

| Screenshot | What it shows |
| :--- | :--- |
| [c3.3-transaction-accounts/transactions.png](c3.3-transaction-accounts/transactions.png) | Transactions: every account's, newest change first, each with its label |
| [c3.3-transaction-accounts/accounts.png](c3.3-transaction-accounts/accounts.png) | Accounts: status and balance per currency |
| [ops-access/ops-user.png](ops-access/ops-user.png) | An ops user's Monitor, with all the screens in the navigation |
| [ops-access/plain-user.png](ops-access/plain-user.png) | A user without the ops role on an ops screen: "Ops access needed", only My account in the navigation, and no ops API call made (the capture logged 0) |
| [ops-access/signed-out.png](ops-access/signed-out.png) | Signed out, opening an ops screen: sent to log in, with only My account in the navigation |
| [c2.3-my-account/no-account.png](c2.3-my-account/no-account.png) | My account before an account exists: the button to open one |
| [c2.3-my-account/refused.png](c2.3-my-account/refused.png) | My account: a spend refused for insufficient funds, with the balance unchanged |
| [c2.3-my-account/frozen.png](c2.3-my-account/frozen.png) | My account after a 4th top-up within 5 minutes: frozen by the fraud engine, so spending is blocked |
| [c2.2-auth/profile.png](c2.2-auth/profile.png) | Profile: name, bio and the password change |
| [c2.2-auth/password-changed.png](c2.2-auth/password-changed.png) | After a password change: every session ended, and the user is asked to log in again |
| [c2.2-auth/login-refused.png](c2.2-auth/login-refused.png) | A wrong password: "invalid email or password", the same answer auth-api gives for an unknown email |
| [c2.1-scaffold/monitor.png](c2.1-scaffold/monitor.png) | The first scaffold (C2.1), before the Monitor was built |
| [c2.1-scaffold/not-found.png](c2.1-scaffold/not-found.png) | An unknown path: the app's not-found page |
