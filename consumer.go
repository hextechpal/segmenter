package segmenter

import (
	"context"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"log"
	"sync"
	"time"
)

const heartBeatDuration = 2 * time.Second
const lockDuration = 10 * time.Second

type Consumer struct {
	mu        sync.Mutex
	rdb       *redis.Client
	locks     map[Partition]*redislock.Lock
	id        string
	stream    string
	batchSize int
	ns        string
}

func NewConsumer(ctx context.Context, rdb *redis.Client, batchSize int, stream string, ns string) (*Consumer, error) {
	c := &Consumer{
		rdb:       rdb,
		id:        GenerateUuid(),
		batchSize: batchSize,
		stream:    stream,
		ns:        ns,
		locks:     make(map[Partition]*redislock.Lock),
	}
	err := c.initiateHeartBeat(ctx)
	if err != nil {
		return nil, err
	}
	go c.beat()
	go c.refreshLocks()
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
	return c.rdb.Set(ctx, c.heartBeatKey(), time.Now().UnixMilli(), heartBeatDuration).Err()
}

func (c *Consumer) heartBeatKey() string {
	return fmt.Sprintf("__%s:%s:__beat:%s", c.ns, c.stream, c.id)
}

func (c *Consumer) beat() {
	for {
		set := c.rdb.Set(context.Background(), c.heartBeatKey(), time.Now().UnixMilli(), heartBeatDuration)
		if set.Err() != nil {
			log.Printf("Error occured while refreshing heartbeat")
			time.Sleep(100 * time.Millisecond)
		}
		time.Sleep(heartBeatDuration)
	}
}

func (c *Consumer) RePartition(ctx context.Context, partitions Partitions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	toBeReleased := make([]Partition, 0)
	for p := range c.locks {
		if !partitions.contains(p) {
			toBeReleased = append(toBeReleased, p)
		}
	}
	for _, p := range toBeReleased {
		log.Printf("[%s] [%s] [%s] Releasing lock with key %s", c.ns, c.stream, c.id, c.locks[p].Key())
		_ = c.locks[p].Release(ctx)
		delete(c.locks, p)
	}

	for _, p := range partitions {
		if _, ok := c.locks[p]; !ok {
			lock, err := AcquireLock(ctx, c.rdb, c.partitionKey(p), lockDuration, c.id)
			if err != nil {
				log.Printf("[%s] [%s] [%s] Failed to Acquire lock with key %s, %v", c.ns, c.stream, c.id, c.partitionKey(p), err)
				return err
			}
			c.locks[p] = lock
			log.Printf("[%s] [%s] [%s] Acquired lock with key %s", c.ns, c.stream, c.id, c.partitionKey(p))
		}
	}
	return nil
}

func (c *Consumer) partitionKey(p Partition) string {
	return fmt.Sprintf("__%s:%s_%d", c.ns, c.stream, p)
}

func (c *Consumer) refreshLocks() {
	log.Printf("[%s] [%s] [%s] Refresh Lock loop Initiated", c.ns, c.stream, c.id)
	ctx := context.Background()
	for {
		c.mu.Lock()
		//log.Printf("[%s] [%s] [%s] Locks : %v", c.ns, c.stream, c.id, c.locks)
		for _, lock := range c.locks {
			err := lock.Refresh(ctx, lockDuration, nil)
			if err != nil {
				log.Printf("Error happened while refreshing lock %s Consumer %s, %v", lock.Key(), lock.Metadata(), err)
			}
		}
		c.mu.Unlock()
		time.Sleep(1000 * time.Millisecond)
	}
}
