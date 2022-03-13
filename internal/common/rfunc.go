package common

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"time"
)

func AcquireLock(ctx context.Context, rdb *redis.Client, key string, ttl time.Duration) (*redislock.Lock, error) {
	locker := redislock.New(rdb)
	opts := &redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 500*time.Millisecond),
	}
	return locker.Obtain(ctx, fmt.Sprintf("__lock:%s", key), ttl, opts)
}

func QueryKey(ctx context.Context, rdb *redis.Client, key string) ([]byte, error) {
	return rdb.Get(ctx, key).Bytes()
}

func SetKey(ctx context.Context, rdb *redis.Client, key string, val interface{}) (string, error) {
	return rdb.Set(ctx, key, val, 0).Result()
}
