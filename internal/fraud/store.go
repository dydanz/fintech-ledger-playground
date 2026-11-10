package fraud

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"fintech-ledger-playground/internal/ledger"
)

// WindowStore keeps each rule's recent events per key, scored by event time.
type WindowStore interface {
	// Add records p under key (idempotent per transaction) and drops points
	// older than p.At − retain. The key expires after retain of inactivity.
	Add(ctx context.Context, key string, p Point, retain time.Duration) error
	// Range returns the points with from <= At <= to.
	Range(ctx context.Context, key string, from, to time.Time) ([]Point, error)
}

// RedisWindowStore is a ZSET per key: member "<transaction_id>:<amount>",
// score = occurred_at in milliseconds (TRD §7.4).
type RedisWindowStore struct {
	rdb redis.UniversalClient
}

func NewRedisWindowStore(rdb redis.UniversalClient) *RedisWindowStore {
	return &RedisWindowStore{rdb: rdb}
}

func (s *RedisWindowStore) Add(ctx context.Context, key string, p Point, retain time.Duration) error {
	member := p.TransactionID + ":" + strconv.FormatInt(p.Amount, 10)
	_, err := s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(p.At.UnixMilli()), Member: member})
		pipe.ZRemRangeByScore(ctx, key, "-inf", "("+strconv.FormatInt(p.At.Add(-retain).UnixMilli(), 10))
		pipe.PExpire(ctx, key, retain)
		return nil
	})
	if err != nil {
		return fmt.Errorf("redis window add: %w", err)
	}
	return nil
}

func (s *RedisWindowStore) Range(ctx context.Context, key string, from, to time.Time) ([]Point, error) {
	zs, err := s.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: strconv.FormatInt(from.UnixMilli(), 10), Max: strconv.FormatInt(to.UnixMilli(), 10)}).Result()
	if err != nil {
		return nil, fmt.Errorf("redis window range: %w", err)
	}
	out := make([]Point, 0, len(zs))
	for _, z := range zs {
		m, _ := z.Member.(string)
		id, amount, ok := strings.Cut(m, ":")
		n, err := strconv.ParseInt(amount, 10, 64)
		if !ok || err != nil {
			return nil, fmt.Errorf("redis window range: malformed member %q", m)
		}
		out = append(out, Point{TransactionID: id, At: time.UnixMilli(int64(z.Score)).UTC(), Amount: n})
	}
	return out, nil
}

// Evaluator applies the rules to settlements through a WindowStore.
type Evaluator struct {
	store WindowStore
	cfg   Config
}

func NewEvaluator(store WindowStore, cfg Config) (*Evaluator, error) {
	if cfg.TopupMax < 1 || cfg.TopupWindow <= 0 || cfg.DebitWindow <= 0 || cfg.DebitMaxCount < 1 {
		return nil, fmt.Errorf("fraud: invalid config %+v", cfg)
	}
	for c, l := range cfg.DebitLimits {
		if l <= 0 {
			return nil, fmt.Errorf("fraud: debit limit for %s must be positive", c)
		}
	}
	return &Evaluator{store: store, cfg: cfg}, nil
}

// Evaluate records s and returns the decision if its rule now fires. Events
// are kept for twice the window, so an event up to one window late is still
// judged against its neighbours.
func (e *Evaluator) Evaluate(ctx context.Context, s Settlement) (Decision, bool, error) {
	var key string
	var w time.Duration
	switch s.Type {
	case ledger.Credit:
		key, w = "fraud:topup:"+s.AccountID, e.cfg.TopupWindow
	case ledger.Debit:
		key, w = "fraud:debit:"+s.AccountID+":"+s.Currency, e.cfg.DebitWindow
	default:
		return Decision{}, false, nil
	}
	p := Point{TransactionID: s.TransactionID, At: s.OccurredAt.UTC().Truncate(time.Millisecond), Amount: s.Amount}
	if err := e.store.Add(ctx, key, p, 2*w); err != nil {
		return Decision{}, false, err
	}
	points, err := e.store.Range(ctx, key, p.At.Add(-w), p.At.Add(w))
	if err != nil {
		return Decision{}, false, err
	}
	if s.Type == ledger.Credit {
		d, ok := TopupVelocity(s.AccountID, points, p.At, e.cfg)
		return d, ok, nil
	}
	d, ok := DebitVolume(s.AccountID, s.Currency, points, p.At, e.cfg)
	return d, ok, nil
}
