package api

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/pkg/segmenter"
	sr "github.com/hextechpal/segmenter/internal/pkg/segmenter/redis"
)

type Segmenter struct {
	ns  string
	rdb *redis.Client
}

func NewSegmenter(c *Config) (*Segmenter, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Address,
		Username: c.Username,
		Password: c.Password,
		DB:       1,
	})

	status := rdb.Ping(context.TODO())
	if status.Err() != nil {
		return nil, status.Err()
	}
	return &Segmenter{rdb: rdb}, nil
}

// RegisterProducer : Will create stream based on the partition count
// The format for the keys will be segmenter_${streamName}_#{0..partitionCount}
// It also registers this stream record (immutable) in redis so that re-registrations
// do not have any effect and appropriate errors can be thrown
func (s *Segmenter) RegisterProducer(ctx context.Context, topic string, maxLen int64, partitionCount int) (segmenter.Producer, error) {
	return sr.NewProducer(ctx, s.rdb, topic, maxLen, partitionCount)
}

// RegisterConsumer : Will a redis consumer with the specified consumer group
// This will cause a partitioning re-balancing
func (s *Segmenter) RegisterConsumer(topic, group string) segmenter.Consumer {
	return nil
}
