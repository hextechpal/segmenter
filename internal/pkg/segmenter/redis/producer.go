package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/hash"
	"github.com/hextechpal/segmenter/internal/pkg/segmenter"
	"time"
)

type Producer struct {
	rdb   *redis.Client
	count int
	key   string
}

func NewProducer(ctx context.Context, rdb *redis.Client, topic string, maxLen int64, count int) (segmenter.Producer, error) {
	key := fmt.Sprintf("sg_%s", topic)
	locker := redislock.New(rdb)
	opts := &redislock.Options{RetryStrategy: redislock.ExponentialBackoff(100*time.Millisecond, 500*time.Millisecond)}
	lock, err := locker.Obtain(ctx, fmt.Sprintf("LOCK_%s", key), 1*time.Second, opts)
	if err != nil {
		return nil, err
	}
	defer lock.Release(ctx)
	err = createEmptyStreams(ctx, rdb, key, maxLen, count)
	if err != nil {
		return nil, err
	}
	return &Producer{rdb: rdb, count: count, key: key}, nil
}

func createEmptyStreams(ctx context.Context, rdb *redis.Client, key string, maxLen int64, partitions int) error {
	txf := func(tx *redis.Tx) error {
		_, err := tx.Get(ctx, key).Result()
		if err == redis.Nil {
			// Create event for every partition
			for i := 0; i < partitions; i++ {
				stream := fmt.Sprintf("%s_%d", key, i)
				_, err := tx.XAdd(ctx, &redis.XAddArgs{
					Stream: stream,
					MaxLen: maxLen,
					Values: map[string]interface{}{
						"i": 1,
					},
				}).Result()
				_, err = tx.XTrimMaxLen(ctx, stream, 0).Result()
				if err != nil {
					return err
				}
			}

			value, err := json.Marshal(map[string]interface{}{
				"count": partitions,
			})
			if err != nil {
				return err
			}
			_, err = rdb.Set(ctx, key, value, 0).Result()
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		return nil
	}

	for retries := 3; retries > 0; retries-- {
		err := rdb.Watch(ctx, txf)
		if err != redis.TxFailedErr {
			return err
		}
	}
	return errors.New("create streams reached maximum number of retries")
}

func (p Producer) getPartition(partitionKey string) int {
	return int(hash.Hash(partitionKey)) % p.count
}

func (p Producer) Produce(ctx context.Context, message *contracts.PMessage) (string, error) {
	partition := p.getPartition(message.GetPartitionKey())
	stream := fmt.Sprintf("%s_%d", p.key, partition)
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"m": message.GetData(),
		},
	}).Result()
}
