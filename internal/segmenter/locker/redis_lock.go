package locker

import (
	"context"
	"github.com/bsm/redislock"
	"time"
)

type RedisLock struct {
	l *redislock.Lock
}

func (r *RedisLock) Release(ctx context.Context) error {
	return r.l.Release(ctx)
}

func (r *RedisLock) Refresh(ctx context.Context, duration time.Duration) error {
	opts := redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(20*time.Millisecond, 100*time.Millisecond),
	}
	return r.l.Refresh(ctx, duration, &opts)
}

func (r *RedisLock) Key() string {
	return r.l.Key()
}
