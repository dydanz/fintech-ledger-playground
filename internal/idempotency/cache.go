package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix namespaces cached responses: idem:ledger:<client_id>:<key_hash>.
const KeyPrefix = "idem:ledger:"

// Entry is a completed response, cached so a retry can be replayed without
// touching Postgres (TRD §7.1). Postgres stays the authority: only completed
// responses are cached, and an entry expires with its idempotency_keys row.
type Entry struct {
	Fingerprint string    `json:"fp"`
	Operation   string    `json:"op"`
	Status      int       `json:"status"`
	Body        []byte    `json:"body"` // exact bytes; base64 in JSON round-trips them unchanged
	ExpiresAt   time.Time `json:"-"`
}

// Cache is the fast path in front of ledger.idempotency_keys. A miss returns
// (Entry{}, false, nil). Errors mean the cache is unusable and the caller
// falls back to Postgres.
type Cache interface {
	Get(ctx context.Context, clientID, keyHash string) (Entry, bool, error)
	Set(ctx context.Context, clientID, keyHash string, e Entry) error
}

type RedisCache struct {
	rdb redis.UniversalClient
}

func NewRedisCache(rdb redis.UniversalClient) *RedisCache { return &RedisCache{rdb: rdb} }

func cacheKey(clientID, keyHash string) string { return KeyPrefix + clientID + ":" + keyHash }

func (c *RedisCache) Get(ctx context.Context, clientID, keyHash string) (Entry, bool, error) {
	raw, err := c.rdb.Get(ctx, cacheKey(clientID, keyHash)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("redis get: %w", err)
	}
	var e Entry
	if err := json.Unmarshal(raw, &e); err != nil || e.Status == 0 || e.Body == nil {
		// Unreadable entries are a miss: Postgres answers and overwrites it.
		return Entry{}, false, nil
	}
	return e, true, nil
}

// Set stores e until e.ExpiresAt. An entry already past it is not stored.
func (c *RedisCache) Set(ctx context.Context, clientID, keyHash string, e Entry) error {
	ttl := time.Until(e.ExpiresAt)
	if ttl < time.Millisecond {
		return nil
	}
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("encode cache entry: %w", err)
	}
	if err := c.rdb.Set(ctx, cacheKey(clientID, keyHash), raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set: %w", err)
	}
	return nil
}
