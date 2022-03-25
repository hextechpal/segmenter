package locker

import (
	"context"
	"time"
)

type Lock interface {
	Release(ctx context.Context) error
	Refresh(ctx context.Context, duration time.Duration) error
	Key() string
}

type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration, metadata string) (Lock, error)
}
