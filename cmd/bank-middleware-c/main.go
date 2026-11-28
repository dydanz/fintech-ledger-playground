// Command bank-middleware-c adapts Bank-Stub-C (async multi-ACK with AAA
// callback) to the canonical money-out contract (spec/money-out TRD §11.3,
// internal/bankmw/stubc/README.md). It accepts only MOM-signed requests on
// its internal routes, listens for the bank's ACK-1/ACK-2 webhooks, runs the
// handshake in memory, and publishes PROCESSING and terminal statuses to
// money-out.bank-events.v1. It has no database.
package main

import (
	"os"

	"fintech-ledger-playground/internal/bankmw"
	"fintech-ledger-playground/internal/bankmw/stubc"
)

func main() {
	bankmw.Main("bank-middleware-c", func(env bankmw.Env) (bankmw.Built, error) {
		return stubc.Build(env, os.Getenv)
	})
}
