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
	return &Segmenter{rdb: rdb, ns: c.Namespace}, nil
}

// RegisterProducer : Will create stream based on the partition count
// The format for the keys will be segmenter_${streamName}_#{0..partitionCount}
// It also registers this stream record (immutable) in redis so that re-registrations
// do not have any effect and appropriate errors can be thrown
func (s *Segmenter) RegisterProducer(psize int64, pcount int, stream string) segmenter.Producer {
	return sr.NewProducer(s.rdb, s.ns, stream, psize, pcount)
}

// RegisterConsumer : Will a redis consumer with the specified consumer group
// This will cause a partitioning re-balancing
func (s *Segmenter) RegisterConsumer(ctx context.Context, stream string) segmenter.Consumer {
	return nil
}
