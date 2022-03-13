package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/common"
	"github.com/hextechpal/segmenter/internal/pkg/segmenter"
)

type Producer struct {
	rdb    *redis.Client
	pcount int
	psize  int64
	name   string
	ns     string
}

func NewProducer(rdb *redis.Client, ns, name string, psize int64, pcount int) segmenter.Producer {
	return &Producer{rdb: rdb, pcount: pcount, psize: psize, name: name, ns: ns}
}

func (p Producer) Produce(ctx context.Context, message *contracts.PMessage) (string, error) {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream:     p.getStream(message.GetPartitionKey()),
		NoMkStream: false,
		MaxLen:     p.psize,
		Values: map[string]interface{}{
			"m": message.GetData(),
		},
	}).Result()
}

func (p Producer) getStream(partitionKey string) string {
	pc := int(common.Hash(partitionKey)) % p.pcount
	return fmt.Sprintf("__strm:%s:%s_%d", p.ns, p.name, pc)
}
