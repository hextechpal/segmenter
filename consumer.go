package segmenter

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"log"
	"sync"
	"time"
)

const heartBeatDuration = 2 * time.Second

type Consumer struct {
	mu                sync.Mutex
	rdb               *redis.Client
	s                 *Stream
	id                string
	batchSize         int64
	group             string
	maxProcessingTime time.Duration
	segmentMap        map[partition]*segment
	shutDown          chan bool
	active            bool
}

func NewConsumer(_ context.Context, stream *Stream, batchSize int64, group string, maxProcessingTime time.Duration) (*Consumer, error) {
	c := &Consumer{
		rdb:               stream.rdb,
		id:                generateUuid(),
		batchSize:         batchSize,
		s:                 stream,
		group:             group,
		maxProcessingTime: maxProcessingTime,
		segmentMap:        make(map[partition]*segment),
		active:            true,
		shutDown:          make(chan bool),
	}
	go c.beat()
	return c, nil
}

func (c *Consumer) GetID() string {
	return c.id
}

func (c *Consumer) GetStreamName() string {
	return c.s.name
}

func (c *Consumer) initiateHeartBeat(ctx context.Context) error {
	return c.rdb.Set(ctx, heartBeatKey(c.s.ns, c.s.name, c.id), time.Now().UnixMilli(), heartBeatDuration).Err()
}

func (c *Consumer) beat() {
	ctx := context.Background()
	for {
		select {
		case <-c.shutDown:
			return
		default:
			set := c.rdb.Set(ctx, heartBeatKey(c.s.ns, c.s.name, c.id), time.Now().UnixMilli(), heartBeatDuration)
			if set.Err() != nil {
				log.Printf("Error occured while refreshing heartbeat")
				time.Sleep(100 * time.Millisecond)
			}
		}
		time.Sleep(heartBeatDuration)
	}
}

func (c *Consumer) rePartition(ctx context.Context, partitions partitions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	toBeReleased := make([]partition, 0)
	for p := range c.segmentMap {
		if !partitions.contains(p) {
			toBeReleased = append(toBeReleased, p)
		}
	}
	log.Printf("[%s] Need to Shutdown partitions : %v\n", c.id, toBeReleased)

	for _, p := range toBeReleased {
		c.segmentMap[p].ShutDown()
		delete(c.segmentMap, p)
	}

	log.Printf("[%s] Partions shut down Sucessfully : %v\n", c.id, toBeReleased)

	for _, p := range partitions {
		if _, ok := c.segmentMap[p]; !ok {
			log.Printf("[%s] Creating segment for partion : %v\n", c.id, p)
			sg, err := newSegment(ctx, c, p)
			if err != nil {
				return err
			}
			c.segmentMap[p] = sg
			log.Printf("[%s] Created segment for partion : %v\n", c.id, p)
		}
	}
	log.Printf("[%s] Rebalance Completed\n", c.id)
	return nil
}

func (c *Consumer) buildStreamsKey() []string {
	i := 0
	segmentCount := len(c.segmentMap)
	streams := make([]string, 2*segmentCount)
	for _, sg := range c.segmentMap {
		streams[i] = sg.partitionedStream()
		streams[i+segmentCount] = ">"
		i += 1
	}
	return streams
}

func (c *Consumer) GetMessages(ctx context.Context, maxWaitDuration time.Duration) ([]*contracts.CMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.active {
		return nil, errors.New("consumer shut down")
	}
	entries := c.getPendingEntries(ctx)
	if len(entries) == 0 {
		return c.getNewMessages(ctx, maxWaitDuration)
	}

	claimed := c.claimEntries(ctx, entries)
	if len(claimed) == 0 {
		return c.getNewMessages(ctx, maxWaitDuration)
	}
	return claimed, nil
}

func (c *Consumer) AckMessages(ctx context.Context, cmessage *contracts.CMessage) error {
	return c.rdb.XAck(ctx, c.s.getRedisStream(cmessage.PartitionKey), c.group, cmessage.Id).Err()
}

type pendingResponse struct {
	err        error
	partition  partition
	messageIds []string
}

func (c *Consumer) getPendingEntries(ctx context.Context) map[partition][]string {
	pending := make(map[partition][]string)
	ch := make(chan *pendingResponse)
	for _, sg := range c.segmentMap {
		go sg.pendingEntries(ctx, ch)
	}

	for i := 0; i < len(c.segmentMap); i++ {
		pr := <-ch
		if pr.err != nil {
			log.Printf("error while executing pending command, %v\n", pr.err)
			continue
		}
		if len(pr.messageIds) > 0 {
			pending[pr.partition] = pr.messageIds
		}
	}
	return pending
}

func (c *Consumer) getNewMessages(ctx context.Context, maxWaitDuration time.Duration) ([]*contracts.CMessage, error) {
	if len(c.segmentMap) == 0 {
		return []*contracts.CMessage{}, nil
	}
	streamsKey := c.buildStreamsKey()
	log.Printf("[%s] Consumer reading pending messages from streams %v\n", c.id, streamsKey)
	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.id,
		Streams:  c.buildStreamsKey(),
		Count:    c.batchSize,
		Block:    maxWaitDuration,
	}).Result()

	if err == redis.Nil {
		return []*contracts.CMessage{}, nil
	}

	if err != nil {
		return nil, err
	}

	return mapXStreamToCMessage(result), nil
}

type claimResponse struct {
	err       error
	partition partition
	messages  []*contracts.CMessage
}

func (c *Consumer) claimEntries(ctx context.Context, entries map[partition][]string) []*contracts.CMessage {
	messages := make([]*contracts.CMessage, 0)
	ch := make(chan *claimResponse)
	for partition, ids := range entries {
		sg := c.segmentMap[partition]
		go sg.claimEntries(ctx, ch, ids)
	}

	for i := 0; i < len(entries); i++ {
		cr := <-ch
		if cr.err != nil {
			log.Printf("error while executing pending command, %v\n", cr.err)
			continue
		}
		messages = append(messages, cr.messages...)
	}
	return messages
}

func mapXStreamToCMessage(result []redis.XStream) []*contracts.CMessage {
	cmessages := make([]*contracts.CMessage, 0)
	for _, xstream := range result {
		cmessages = append(cmessages, mapXMessageToCMessage(xstream.Messages)...)
	}
	return cmessages
}

func mapXMessageToCMessage(msgs []redis.XMessage) []*contracts.CMessage {
	cmessages := make([]*contracts.CMessage, 0)
	for _, m := range msgs {
		cm := contracts.CMessage{
			Id:           m.ID,
			PartitionKey: m.Values["partitionKey"].(string),
			Data:         []byte(m.Values["data"].(string)),
		}
		cmessages = append(cmessages, &cm)
	}
	return cmessages
}

func (c *Consumer) ShutDown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sg := range c.segmentMap {
		sg.ShutDown()
	}
	err := c.s.shutDownConsumer(ctx, c.GetID())
	if err != nil {
		return err
	}
	c.active = false
	return nil
}
