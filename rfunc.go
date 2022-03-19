package segmenter

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"time"
)

const retryAttempts = 3

func AcquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, metadata string) (*redislock.Lock, error) {
	return acquireLockWithRetry(ctx, rdb, key, ttl, metadata, 0)
}

func AcquireAdminLock(ctx context.Context, rdb *redis.Client, ns string, ttl time.Duration) (*redislock.Lock, error) {
	return acquireLockWithRetry(ctx, rdb, adminKey(ns), ttl, "admin", 0)
}

func acquireLockWithRetry(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, metadata string, attempt int) (*redislock.Lock, error) {
	lock, err := acquireLock(ctx, rdb, key, ttl, metadata)
	if err == redislock.ErrNotObtained {
		if attempt < retryAttempts {
			return acquireLockWithRetry(ctx, rdb, key, ttl, metadata, attempt+1)
		}
	}
	if err != nil {
		return nil, err
	}
	return lock, nil
}

func acquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, metadata string) (*redislock.Lock, error) {
	locker := redislock.New(rdb)
	opts := &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 1*time.Second),
		Metadata:      metadata,
	}
	return locker.Obtain(ctx, fmt.Sprintf("__lock:%s", key), ttl, opts)
}

func adminKey(ns string) string {
	return fmt.Sprintf("__%s:admin", ns)
}
