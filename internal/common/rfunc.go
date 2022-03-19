package common

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

func AcquireAdminLock(ctx context.Context, rdb *redis.Client, ns string, ttl time.Duration) (*redislock.Lock, error) {
	locker := redislock.New(rdb)
	opts := &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 500*time.Millisecond),
	}
	return locker.Obtain(ctx, fmt.Sprintf("__lock:%s", AdminKey(ns)), ttl, opts)
}

func AdminKey(ns string) string {
	return fmt.Sprintf("__%s:admin", ns)
}

func QueryKey(ctx context.Context, rdb *redis.Client, key string) ([]byte, error) {
	return rdb.Get(ctx, key).Bytes()
}

func SetKey(ctx context.Context, rdb *redis.Client, key string, val interface{}) (string, error) {
	return rdb.Set(ctx, key, val, 0).Result()
}
