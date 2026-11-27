package config

import (
	"errors"
	"fmt"
	"strings"
)

// MoneyOutClient is one caller of money-out-middleware's ops API: its id and
// the hex SHA-256 of its bearer token. The API is read-only, so a client has
// no grants beyond being known.
type MoneyOutClient struct {
	ID        string
	TokenHash string
}

// ParseMoneyOutClients parses MOM_CLIENTS: "id:sha256hex;id2:sha256hex". It
// rejects malformed ids and hashes, and duplicate ids or hashes. The error
// never contains a hash value.
func ParseMoneyOutClients(v string) ([]MoneyOutClient, error) {
	var out []MoneyOutClient
	ids, hashes := map[string]bool{}, map[string]bool{}
	for i, entry := range strings.Split(v, ";") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.Split(entry, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("MOM_CLIENTS entry %d: want id:sha256hex", i+1)
		}
		c := MoneyOutClient{ID: strings.TrimSpace(parts[0]), TokenHash: strings.ToLower(strings.TrimSpace(parts[1]))}
		switch {
		case !clientIDPattern.MatchString(c.ID):
			return nil, fmt.Errorf("MOM_CLIENTS entry %d: client id must match %s", i+1, clientIDPattern)
		case !sha256Pattern.MatchString(c.TokenHash):
			return nil, fmt.Errorf("MOM_CLIENTS entry %d (%s): token hash must be 64 hex characters", i+1, c.ID)
		case ids[c.ID]:
			return nil, fmt.Errorf("MOM_CLIENTS: duplicate client id %s", c.ID)
		case hashes[c.TokenHash]:
			return nil, fmt.Errorf("MOM_CLIENTS entry %d (%s): token hash already used by another client", i+1, c.ID)
		}
		ids[c.ID], hashes[c.TokenHash] = true, true
		out = append(out, c)
	}
	if len(out) == 0 {
		return nil, errors.New("MOM_CLIENTS is required")
	}
	return out, nil
}
