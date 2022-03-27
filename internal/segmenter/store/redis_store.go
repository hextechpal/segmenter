package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"reflect"
)

type redisStore struct {
	rdb *redis.Client
}

func NewRedisStore(rdb *redis.Client) *redisStore {
	return &redisStore{rdb: rdb}
}

type InvalidTypeError struct {
	tp reflect.Type
}

func (i InvalidTypeError) Error() string {
	return fmt.Sprintf("Invalid type %q, must be a pointer type", i.tp)
}

func (r *redisStore) GetKey(ctx context.Context, key string, v any) error {
	bytes, err := r.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return err
	} else {
		rv := reflect.ValueOf(v)
		if rv.Kind() != reflect.Pointer || rv.IsNil() {
			return InvalidTypeError{tp: rv.Type()}
		}
		err = json.Unmarshal(bytes, v)
		if err != nil {
			return err
		}
		return nil
	}
}

func (r *redisStore) SetKey(ctx context.Context, key string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, key, data, 0).Err()
}
