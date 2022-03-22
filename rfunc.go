package segmenter

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"time"
)

const retryAttempts = 3

func acquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, metadata string) (*redislock.Lock, error) {
	return acquireLockWithRetry(ctx, rdb, key, ttl, metadata, 0)
}

func acquireAdminLock(ctx context.Context, rdb *redis.Client, ns string, stream string, ttl time.Duration) (*redislock.Lock, error) {
	return acquireLockWithRetry(ctx, rdb, streamAdminKey(ns, stream), ttl, "admin", 0)
}

func acquireLockWithRetry(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, metadata string, attempt int) (*redislock.Lock, error) {
	lock, err := acquireLockInternal(ctx, rdb, key, ttl, metadata)
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

func acquireLockInternal(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration, metadata string) (*redislock.Lock, error) {
	locker := redislock.New(rdb)
	opts := &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 1*time.Second),
		Metadata:      metadata,
	}
	return locker.Obtain(ctx, fmt.Sprintf("__lock:%s", key), ttl, opts)
}

func streamAdminKey(ns string, stream string) string {
	return fmt.Sprintf("__%s:__%s:admin", ns, stream)
}

func heartBeatKey(ns, name, id string) string {
	return fmt.Sprintf("__%s:__%s:__beat:%s", ns, name, id)
}

func partitionedStreamKey(ns, stream string, pc partition) string {
	return fmt.Sprintf("__%s:__%s:strm_%d", ns, stream, pc)
}
