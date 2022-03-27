package core

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/segmenter/utils"
	"github.com/rs/zerolog"
	"math"
	"sync"
	"time"
)

const heartBeatDuration = 2 * time.Second

var ConsumerDeadError = errors.New("consumer shut down")

type Consumer struct {
	mu     sync.Mutex
	s      *Stream
	logger *zerolog.Logger

	id                string
	batchSize         int64
	group             string
	maxProcessingTime time.Duration

	segmentMap map[Partition]*segment
	shutDown   chan bool
	active     bool
}

type NewConsumerArgs struct {
	Stream            *Stream
	BatchSize         int64
	Group             string
	MaxProcessingTime time.Duration
	Logger            *zerolog.Logger
}

// Public Functions

func NewConsumer(ctx context.Context, args *NewConsumerArgs) (*Consumer, error) {
	id := utils.GenerateUuid()
	nLogger := args.Logger.With().Str("stream", args.Stream.name).Str("consumerId", id).Str("group", args.Group).Int64("bsize", args.BatchSize).Logger()
	c := &Consumer{
		s:      args.Stream,
		logger: &nLogger,

		id:                id,
		batchSize:         args.BatchSize,
		group:             args.Group,
		maxProcessingTime: args.MaxProcessingTime,

		segmentMap: make(map[Partition]*segment),
		active:     true,
		shutDown:   make(chan bool),
	}

	err := c.checkConsumerGroup(ctx)
	if err != nil {
		return nil, err
	}
	go c.beat()
	return c, nil
}

func (c *Consumer) Read(ctx context.Context, maxWaitDuration time.Duration) ([]*contracts.CMessage, error) {
	if !c.isActive() {
		return nil, ConsumerDeadError
	}

	entries := c.getPendingEntries(ctx)
	if len(entries) == 0 {
		return c.readNewMessages(ctx, maxWaitDuration)
	}

	claimed := c.claimEntries(ctx, entries)
	if len(claimed) == 0 {
		return c.readNewMessages(ctx, maxWaitDuration)
	}
	return claimed, nil
}

func (c *Consumer) Ack(ctx context.Context, cmessage *contracts.CMessage) error {
	if !c.isActive() {
		return ConsumerDeadError
	}
	p := c.s.getPartitionFromKey(cmessage.PartitionKey)
	stream := PartitionedStream(c.s.ns, c.s.name, p)
	return c.s.rdb.XAck(ctx, stream, c.group, cmessage.Id).Err()
}

func (c *Consumer) ShutDown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sg := range c.segmentMap {
		sg.ShutDown()
	}
	c.shutDown <- true
	return nil
}

func (c *Consumer) GetID() string {
	return c.id
}

func (c *Consumer) GetStreamName() string {
	return c.s.name
}

func (c *Consumer) GetNameSpace() string {
	return c.s.ns
}

func (c *Consumer) rePartition(ctx context.Context, partitions Partitions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Debug().Msgf("Re Partitioning started, partitions %v", partitions)
	toBeReleased := make([]Partition, 0)
	for p := range c.segmentMap {
		if !partitions.Contains(p) {
			toBeReleased = append(toBeReleased, p)
		}
	}
	c.logger.Debug().Msgf("Need to Shutdown Partitions : %v", toBeReleased)

	for _, p := range toBeReleased {
		c.segmentMap[p].ShutDown()
		delete(c.segmentMap, p)
	}

	c.logger.Debug().Msgf("Partitions shut down Successfully : %v", toBeReleased)

	for _, p := range partitions {
		if _, ok := c.segmentMap[p]; !ok {
			c.logger.Debug().Msgf("Creating segment for Partition : %v", p)
			sg, err := newSegment(ctx, c, p)
			if err != nil {
				return err
			}
			c.segmentMap[p] = sg
			c.logger.Debug().Msgf("Created segment for Partition : %v", p)
		}
	}
	c.logger.Debug().Msg("Re Partitioning  completed")
	return nil
}

func (c *Consumer) buildStreamsKey() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	segmentCount := len(c.segmentMap)
	if segmentCount == 0 {
		return []string{}
	}
	i := 0
	streams := make([]string, 2*segmentCount)
	for _, sg := range c.segmentMap {
		streams[i] = PartitionedStream(c.GetNameSpace(), c.GetStreamName(), sg.partition)
		streams[i+segmentCount] = ">"
		i += 1
	}
	return streams
}

type pendingResponse struct {
	err        error
	partition  Partition
	messageIds []string
}

func (c *Consumer) getPendingEntries(ctx context.Context) map[Partition][]string {
	ch, pc := c.queuePending(ctx)
	pending := make(map[Partition][]string)
	for i := 0; i < pc; i++ {
		pr := <-ch
		if pr.err != nil {
			continue
		}
		if len(pr.messageIds) > 0 {
			pending[pr.partition] = pr.messageIds
		}
	}
	return pending
}

func (c *Consumer) queuePending(ctx context.Context) (chan *pendingResponse, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan *pendingResponse)
	pc := len(c.segmentMap)
	for _, sg := range c.segmentMap {
		go sg.pendingEntries(ctx, ch)
	}
	return ch, pc
}

func (c *Consumer) readNewMessages(ctx context.Context, maxWaitDuration time.Duration) ([]*contracts.CMessage, error) {
	streamsKey := c.buildStreamsKey()
	if len(streamsKey) == 0 {
		return []*contracts.CMessage{}, nil
	}
	c.logger.Debug().Msgf("Consumer reading new messages from streams %v", streamsKey)
	count := math.Round(float64(c.batchSize) / float64(len(c.segmentMap)))
	if count == 0 {
		count = 1
	}
	result, err := c.s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    c.group,
		Consumer: c.id,
		Streams:  c.buildStreamsKey(),
		Count:    int64(count),
		Block:    maxWaitDuration,
	}).Result()

	if err == redis.Nil {
		return []*contracts.CMessage{}, nil
	}

	if err != nil {
		return nil, err
	}
	messages := mapXStreamToCMessage(result)
	c.logger.Debug().Msgf("Result for new messages %v", len(messages))
	return messages, nil
}

type claimResponse struct {
	err       error
	partition Partition
	messages  []*contracts.CMessage
}

func (c *Consumer) claimEntries(ctx context.Context, entries map[Partition][]string) []*contracts.CMessage {
	messages := make([]*contracts.CMessage, 0)
	ch := make(chan *claimResponse)
	for partition, ids := range entries {
		sg := c.segmentMap[partition]
		go sg.claimEntries(ctx, ch, ids)
	}

	for i := 0; i < len(entries); i++ {
		cr := <-ch
		if cr.err != nil {
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

func (c *Consumer) beat() {
	ctx := context.Background()
	for {
		select {
		case <-c.shutDown:
			return
		default:
			set := c.s.rdb.Set(ctx, HeartBeat(c.GetNameSpace(), c.GetStreamName(), c.id, c.group), time.Now().UnixMilli(), heartBeatDuration)
			if set.Err() != nil {
				c.logger.Info().Msg("Error occurred while refreshing heartbeat")
				time.Sleep(100 * time.Millisecond)
			}
		}
		time.Sleep(heartBeatDuration)
	}
}

func (c *Consumer) checkConsumerGroup(ctx context.Context) error {
	for i := 0; i < c.s.pcount; i++ {
		key := PartitionedStream(c.s.ns, c.s.name, Partition(i))
		err := c.s.rdb.XGroupCreateMkStream(ctx, key, c.group, "$").Err()
		if err != nil {
			if err.Error() == "BUSYGROUP Consumer Group name already exists" {
				continue
			}
			c.logger.Debug().Msgf("Error while registering Consumer, %v", err)
			return err
		}
	}
	return nil
}

func (c *Consumer) isActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Debug().Msgf("Get Messages")
	return c.active
}
