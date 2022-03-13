package redis

import (
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/common"
	"time"
)

type RedisConsumer struct {
	rdb         *redis.Client
	Id          string
	stream      string
	maxWaitTime time.Duration
	batchSize   int
	ns          string
}

func NewConsumer(rdb *redis.Client, maxWaitTime time.Duration, batchSize int, stream string, ns string) *RedisConsumer {
	return &RedisConsumer{
		rdb:         rdb,
		Id:          common.GenerateUuid(),
		maxWaitTime: maxWaitTime,
		batchSize:   batchSize,
		stream:      stream,
		ns:          ns,
	}

}

func (c *RedisConsumer) GetMessages(maxWaitDuration time.Duration) []contracts.CMessage {
	return nil
}
