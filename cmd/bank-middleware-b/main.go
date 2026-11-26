// Command bank-middleware-b adapts Bank-Stub-B (corporate host-to-host,
// sync) to the canonical money-out contract (spec/money-out TRD §11.2,
// internal/bankmw/stubb/README.md). It accepts only MOM-signed requests and
// has no database.
package main

import (
	"os"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/bankmw/stubb"
)

func main() {
	bankmw.Main("bank-middleware-b", func(env bankmw.Env) (bankmw.Built, error) {
		cfg, err := stubb.ConfigFromEnv(os.Getenv)
		if err != nil {
			return bankmw.Built{}, err
		}
		b, err := stubb.New(cfg, env.Client)
		return bankmw.Built{Bank: b}, err
	})
}
