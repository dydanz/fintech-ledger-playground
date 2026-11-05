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
func (s *RedisStore) Create(ctx context.Context, tokenHash, userID string, ttl time.Duration) error {
	ok, err := s.rdb.SetNX(ctx, key(tokenHash), userID, ttl).Result()
	if err != nil {
		return fmt.Errorf("redis setnx: %w", err)
	}
	if !ok {
		return errors.New("redis setnx: session key already exists")
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

// Delete removes the session. It reports whether a key was removed.
func (s *RedisStore) Delete(ctx context.Context, tokenHash string) (bool, error) {
	n, err := s.rdb.Del(ctx, key(tokenHash)).Result()
	if err != nil {
		return false, fmt.Errorf("redis del: %w", err)
	}
	return n > 0, nil
}
