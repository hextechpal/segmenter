package api

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/common"
	"time"
)

const heartBeatDuration = 120 * time.Second

type Consumer struct {
	rdb       *redis.Client
	id        string
	stream    string
	batchSize int
	ns        string
}

func NewConsumer(ctx context.Context, rdb *redis.Client, batchSize int, stream string, ns string) (*Consumer, error) {
	c := &Consumer{
		rdb:       rdb,
		id:        common.GenerateUuid(),
		batchSize: batchSize,
		stream:    stream,
		ns:        ns,
	}
	err := c.initiateHeartBeat(ctx)
	if err != nil {
		return nil, err
	}
	go c.beat()
	return c, nil
}

func (c *Consumer) GetMessages(maxWaitDuration time.Duration) []contracts.CMessage {
	return nil
}

func (c *Consumer) GetID() string {
	return c.id
}

func (c *Consumer) GetStream() string {
	return c.stream
}

func (c *Consumer) initiateHeartBeat(ctx context.Context) error {
	return c.rdb.Set(ctx, c.heartBeatKey(), "beat", heartBeatDuration).Err()
}

func (c *Consumer) heartBeatKey() string {
	return fmt.Sprintf("__%s:%s:__beat:%s", c.ns, c.stream, c.id)
}

func (c *Consumer) beat() {
	for {
		c.rdb.Set(context.Background(), c.heartBeatKey(), time.Now().UnixMilli(), heartBeatDuration)
		time.Sleep(heartBeatDuration)
	}
}
