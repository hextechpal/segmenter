package store

import "context"

// Store : Provides a storage interface for segmenter key value store
type Store interface {
	GetKey(ctx context.Context, key string, v any) error
	SetKey(ctx context.Context, key string, v any) error
}
