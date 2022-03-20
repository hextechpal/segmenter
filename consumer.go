package segmenter

import (
	"context"
	"errors"
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
	mu                sync.Mutex
	rdb               *redis.Client
	s                 *Stream
	locks             map[Partition]*redislock.Lock
	id                string
	batchSize         int
	group             string
	maxProcessingTime time.Duration
	active            bool
}

func NewConsumer(ctx context.Context, stream *Stream, batchSize int, group string, maxProcessingTime time.Duration) (*Consumer, error) {
	c := &Consumer{
		rdb:               stream.rdb,
		id:                GenerateUuid(),
		batchSize:         batchSize,
		s:                 stream,
		group:             group,
		maxProcessingTime: maxProcessingTime,
		locks:             make(map[Partition]*redislock.Lock),
		active:            true,
	}
	err := c.initiateHeartBeat(ctx)
	if err != nil {
		return nil, err
	}
	go c.beat()
	go c.refreshLocks()
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
	for c.active {
		set := c.rdb.Set(context.Background(), heartBeatKey(c.s.ns, c.s.name, c.id), time.Now().UnixMilli(), heartBeatDuration)
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
		log.Printf("[%s] [%s] [%s] Releasing lock with key %s", c.s.ns, c.s.name, c.id, c.locks[p].Key())
		_ = c.locks[p].Release(ctx)
		delete(c.locks, p)
	}

	for _, p := range partitions {
		if _, ok := c.locks[p]; !ok {
			lock, err := AcquireLock(ctx, c.rdb, c.partitionKey(p), lockDuration, c.id)
			if err != nil {
				log.Printf("[%s] [%s] [%s] Failed to Acquire lock with key %s, %v", c.s.ns, c.s.name, c.id, c.partitionKey(p), err)
				return err
			}
			c.locks[p] = lock
			log.Printf("[%s] [%s] [%s] Acquired lock with key %s", c.s.ns, c.s.name, c.id, c.partitionKey(p))
		}
	}
	return nil
}

func (c *Consumer) partitionKey(p Partition) string {
	return fmt.Sprintf("__%s:%s_%d", c.s.ns, c.s.name, p)
}

func (c *Consumer) refreshLocks() {
	log.Printf("[%s] [%s] [%s] Refresh Lock loop Initiated", c.s.ns, c.s.name, c.id)
	ctx := context.Background()
	for c.active {
		c.mu.Lock()
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

func (c *Consumer) buildStreamsKey() []string {
	i := 0
	streams := make([]string, 2*len(c.locks))
	for k, _ := range c.locks {
		streams[i] = StreamKey(c.s.ns, c.s.name, k)
		streams[i+len(c.locks)] = ">"
		i += 1
	}
	return streams
}

func (c *Consumer) assignedStreams() []string {
	i := 0
	streams := make([]string, len(c.locks))
	for k := range c.locks {
		streams[i] = StreamKey(c.s.ns, c.s.name, k)
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

type pendingResponse struct {
	err        error
	stream     string
	messageIds []string
}

func (c *Consumer) getPendingEntries(ctx context.Context) map[string][]string {
	pending := make(map[string][]string)
	streams := c.assignedStreams()
	ch := make(chan *pendingResponse)
	for _, stream := range streams {
		go c.pendingEntriesForStream(ctx, ch, stream)
	}

	for i := 0; i < len(streams); i++ {
		pr := <-ch
		if pr.err != nil {
			log.Printf("error while executing pending command, %v\n", pr.err)
			continue
		}
		pending[pr.stream] = pr.messageIds
	}
	return pending
}

func (c *Consumer) pendingEntriesForStream(ctx context.Context, ch chan *pendingResponse, stream string) {
	pending, err := c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream,
		Group:  c.group,
		Idle:   c.maxProcessingTime,
		Start:  "-",
		End:    "+",
		Count:  int64(c.batchSize),
	}).Result()

	if err == redis.Nil {
		ch <- &pendingResponse{
			err:        nil,
			stream:     stream,
			messageIds: []string{},
		}
		return
	}

	if err != nil {
		ch <- &pendingResponse{
			err:        err,
			stream:     stream,
			messageIds: nil,
		}
	}

	//TODO handle retry count
	messageIds := make([]string, 0)
	for _, xp := range pending {
		messageIds = append(messageIds, xp.ID)
	}

	ch <- &pendingResponse{
		err:        nil,
		stream:     stream,
		messageIds: messageIds,
	}

}

func (c *Consumer) getNewMessages(ctx context.Context, maxWaitDuration time.Duration) ([]*contracts.CMessage, error) {
	streamsKey := c.buildStreamsKey()
	if len(streamsKey) == 0 {
		return []*contracts.CMessage{}, nil
	}
	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.id,
		Streams:  c.buildStreamsKey(),
		Count:    int64(c.batchSize),
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
	err      error
	stream   string
	messages []*contracts.CMessage
}

func (c *Consumer) claimEntries(ctx context.Context, entries map[string][]string) []*contracts.CMessage {
	messages := make([]*contracts.CMessage, 0)
	ch := make(chan *claimResponse)
	for stream, ids := range entries {
		go c.claimEntriesForStream(ctx, ch, stream, ids)
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

func (c *Consumer) claimEntriesForStream(ctx context.Context, ch chan *claimResponse, stream string, ids []string) {
	result, err := c.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   stream,
		Group:    c.group,
		Consumer: c.id,
		Messages: ids,
	}).Result()

	if err == redis.Nil {
		ch <- &claimResponse{
			err:      nil,
			stream:   stream,
			messages: []*contracts.CMessage{},
		}
		return
	}

	if err != nil {
		ch <- &claimResponse{
			err:      err,
			stream:   stream,
			messages: nil,
		}
		return
	}

	ch <- &claimResponse{
		err:      nil,
		stream:   stream,
		messages: mapXMessageToCMessage(result),
	}
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
			Id:   m.ID,
			Pkey: m.Values["pkey"].(string),
			Data: []byte(m.Values["data"].(string)),
		}
		cmessages = append(cmessages, &cm)
	}
	return cmessages
}

func (c *Consumer) ShutDown(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, lock := range c.locks {
		_ = lock.Release(ctx)
	}
	err := c.s.shutDownConsumer(ctx, c.GetID())
	if err != nil {
		return err
	}
	c.active = false
	return nil
}
