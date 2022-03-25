package locker

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"time"
)

const maxRetries = 3

type RedisLocker struct {
	rlc *redislock.Client
}

func NewRedisLocker(rdb *redis.Client) *RedisLocker {
	return &RedisLocker{rlc: redislock.New(rdb)}
}

func (rl *RedisLocker) Acquire(ctx context.Context, key string, ttl time.Duration, metadata string) (Lock, error) {
	return rl.acquireLockWithRetry(ctx, key, ttl, metadata, 0)
}

func (rl *RedisLocker) acquireLockWithRetry(ctx context.Context, key string, ttl time.Duration, metadata string, retry int) (*RedisLock, error) {
	opts := &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 1*time.Second),
		Metadata:      metadata,
	}
	lock, err := rl.rlc.Obtain(ctx, rl.getLockKey(key), ttl, opts)
	if err == redislock.ErrNotObtained {
		if retry < maxRetries {
			return rl.acquireLockWithRetry(ctx, key, ttl, metadata, retry+1)
		}
	}
	if err != nil {
		return nil, err
	}
	return &RedisLock{l: lock}, nil
}

func (rl *RedisLocker) getLockKey(key string) string {
	return fmt.Sprintf("__lock:%s", key)
}
