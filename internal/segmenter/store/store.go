package store

import "context"

type Store interface {
	GetKey(ctx context.Context, key string, v any) error
	SetKey(ctx context.Context, key string, v any) error
}
