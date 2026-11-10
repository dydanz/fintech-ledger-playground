// Package session is the Redis-backed hot store for active bearer tokens.
package session

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyPrefix namespaces session keys: session:auth:<sha256(token)>.
const KeyPrefix = "session:auth:"

// UserIndexPrefix namespaces the per-user index: session:user:<user_id> is
// the set of that user's token hashes, so all of them can be ended at once
// (RevokeAll). It expires with the user's newest session; members of
// sessions that expired earlier are stale and skipped.
const UserIndexPrefix = "session:user:"

var ErrNotFound = errors.New("session not found")

type Session struct {
	UserID    string
	ExpiresAt time.Time
}

type RedisStore struct {
	rdb redis.UniversalClient
}

func NewRedisStore(rdb redis.UniversalClient) *RedisStore {
	return &RedisStore{rdb: rdb}
}

func key(tokenHash string) string { return KeyPrefix + tokenHash }

// Create stores an active session that expires after ttl. It refuses to
// overwrite an existing key.
//
// The session is also added to its user's index. If that fails, the session
// is deleted again: a session missing from the index would survive
// RevokeAll, for example after a password change.
func (s *RedisStore) Create(ctx context.Context, tokenHash, userID string, ttl time.Duration) error {
	ok, err := s.rdb.SetNX(ctx, key(tokenHash), userID, ttl).Result()
	if err != nil {
		return fmt.Errorf("redis setnx: %w", err)
	}
	if !ok {
		return errors.New("redis setnx: session key already exists")
	}
	if err := indexScript.Run(ctx, s.rdb, []string{UserIndexPrefix + userID}, tokenHash, ttl.Milliseconds()).Err(); err != nil {
		if _, derr := s.rdb.Del(context.WithoutCancel(ctx), key(tokenHash)).Result(); derr != nil {
			err = errors.Join(err, fmt.Errorf("undo session: %w", derr))
		}
		return fmt.Errorf("redis index session: %w", err)
	}
	return nil
}

// Get returns the session for tokenHash. GET and PTTL go out in one
// pipelined round trip. A key without a positive TTL is treated as invalid,
// so a session can never outlive its validity window by accident.
func (s *RedisStore) Get(ctx context.Context, tokenHash string) (Session, error) {
	var (
		get  *redis.StringCmd
		pttl *redis.DurationCmd
	)
	_, err := s.rdb.Pipelined(ctx, func(p redis.Pipeliner) error {
		get = p.Get(ctx, key(tokenHash))
		pttl = p.PTTL(ctx, key(tokenHash))
		return nil
	})
	if errors.Is(err, redis.Nil) {
		return Session{}, ErrNotFound
	}
	if err != nil {
		return Session{}, fmt.Errorf("redis get session: %w", err)
	}

	ttl := pttl.Val()
	if ttl <= 0 || get.Val() == "" {
		return Session{}, ErrNotFound
	}
	return Session{UserID: get.Val(), ExpiresAt: time.Now().Add(ttl)}, nil
}

// Delete removes the session and its entry in the user's index. It reports
// whether a session was removed.
func (s *RedisStore) Delete(ctx context.Context, tokenHash string) (bool, error) {
	n, err := deleteScript.Run(ctx, s.rdb, []string{key(tokenHash)}, UserIndexPrefix, tokenHash).Int64()
	if err != nil {
		return false, fmt.Errorf("redis del: %w", err)
	}
	return n > 0, nil
}

// RevokeAll ends every session of userID and drops its index. It returns
// how many live sessions it ended.
func (s *RedisStore) RevokeAll(ctx context.Context, userID string) (int, error) {
	n, err := revokeAllScript.Run(ctx, s.rdb, []string{UserIndexPrefix + userID}, KeyPrefix).Int()
	if err != nil {
		return 0, fmt.Errorf("redis revoke all: %w", err)
	}
	return n, nil
}

// The scripts run atomically. The index and session keys they derive from
// arguments aren't declared in KEYS, so they assume a single Redis node, as
// the rest of this store does.
var (
	// KEYS[1] index; ARGV[1] token hash, ARGV[2] session TTL in ms. The
	// index lives at least as long as its newest session.
	indexScript = redis.NewScript(`
redis.call('SADD', KEYS[1], ARGV[1])
if redis.call('PTTL', KEYS[1]) < tonumber(ARGV[2]) then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 1`)

	// KEYS[1] session key; ARGV[1] index prefix, ARGV[2] token hash.
	deleteScript = redis.NewScript(`
local uid = redis.call('GET', KEYS[1])
local n = redis.call('DEL', KEYS[1])
if uid then redis.call('SREM', ARGV[1] .. uid, ARGV[2]) end
return n`)

	// KEYS[1] index; ARGV[1] session key prefix.
	revokeAllScript = redis.NewScript(`
local n = 0
for _, h in ipairs(redis.call('SMEMBERS', KEYS[1])) do
  n = n + redis.call('DEL', ARGV[1] .. h)
end
redis.call('DEL', KEYS[1])
return n`)
)
