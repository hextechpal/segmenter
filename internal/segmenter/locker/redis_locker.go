package locker

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"time"
)

const maxRetries = 3

// RedisLocker : Implementation of Locker interface based on redis
type RedisLocker struct {
	rlc *redislock.Client
}

// NewRedisLocker : Returns a new RedisLocker based on supplied client
func NewRedisLocker(rdb *redis.Client) *RedisLocker {
	return &RedisLocker{rlc: redislock.New(rdb)}
}

// Acquire : Attempts to acquire the redis lock for the key appending __lock: prefix
func (rl *RedisLocker) Acquire(ctx context.Context, key string, ttl time.Duration, metadata string) (Lock, error) {
	return rl.acquireLockWithRetry(ctx, key, ttl, metadata, 0)
}

func (rl *RedisLocker) acquireLockWithRetry(ctx context.Context, key string, ttl time.Duration, metadata string, retry int) (*redisLock, error) {
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
	return &redisLock{l: lock}, nil
}

func (rl *RedisLocker) getLockKey(key string) string {
	return fmt.Sprintf("__lock:%s", key)
}
