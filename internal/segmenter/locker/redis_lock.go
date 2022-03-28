package locker

import (
	"context"
	"github.com/bsm/redislock"
	"time"
)

type redisLock struct {
	l *redislock.Lock
}

func (r *redisLock) Release(ctx context.Context) error {
	return r.l.Release(ctx)
}

func (r *redisLock) Refresh(ctx context.Context, duration time.Duration) error {
	opts := redislock.Options{
		RetryStrategy: redislock.ExponentialBackoff(20*time.Millisecond, 100*time.Millisecond),
	}
	return r.l.Refresh(ctx, duration, &opts)
}

func (r *redisLock) Key() string {
	return r.l.Key()
}
