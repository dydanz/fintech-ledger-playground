// Command bank-middleware-a adapts Bank-Stub-A (BI-SNAP, sync) to the
// canonical money-out contract (spec/money-out TRD §11.1,
// internal/bankmw/stuba/README.md). It accepts only MOM-signed requests and
// has no database.
package main

import (
	"os"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/bankmw/stuba"
)

func main() {
	bankmw.Main("bank-middleware-a", func(env bankmw.Env) (bankmw.Built, error) {
		cfg, err := stuba.ConfigFromEnv(os.Getenv)
		if err != nil {
			return bankmw.Built{}, err
		}
		b, err := stuba.New(cfg, env.Client)
		return bankmw.Built{Bank: b}, err
	})
}
