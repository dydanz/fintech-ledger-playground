// Command bank-middleware-d adapts Bank-Stub-D (multi-rail, sync submit,
// async settle) to the canonical money-out contract (spec/money-out TRD
// §11.4, internal/bankmw/stubd/README.md). It accepts only MOM-signed
// requests, has no database, and publishes PROCESSING and terminal statuses
// to money-out.bank-events.v1.
package main

import (
	"os"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/bankmw/stubd"
)

func main() {
	bankmw.Main("bank-middleware-d", func(env bankmw.Env) (bankmw.Built, error) {
		return stubd.Build(env, os.Getenv)
	})
}
