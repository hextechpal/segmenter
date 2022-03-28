package segmenter

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

// Consumer : This is a segmenter consumer. When you register a consumer with segmenter you get an instance of the consumer.
// This will we used to read/ack messages in the stream
type Consumer struct {
	mu     sync.Mutex
	s      *Stream
	logger *zerolog.Logger

	id                string
	batchSize         int64
	group             string
	maxProcessingTime time.Duration

	segmentMap map[partition]*segment
	shutDown   chan bool
	active     bool
}

type newConsumerArgs struct {
	Stream            *Stream
	BatchSize         int64
	Group             string
	MaxProcessingTime time.Duration
	Logger            *zerolog.Logger
}

func newConsumer(ctx context.Context, args *newConsumerArgs) (*Consumer, error) {
	id := utils.GenerateUuid()
	nLogger := args.Logger.With().Str("stream", args.Stream.name).Str("consumerId", id).Str("group", args.Group).Int64("bsize", args.BatchSize).Logger()
	c := &Consumer{
		s:      args.Stream,
		logger: &nLogger,

		id:                id,
		batchSize:         args.BatchSize,
		group:             args.Group,
		maxProcessingTime: args.MaxProcessingTime,

		segmentMap: make(map[partition]*segment),
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

// Read : Reads rom the stream this consumer is registered with. It will only read from the partitions
// assigned to the consumer.
func (c *Consumer) Read(ctx context.Context, maxWaitDuration time.Duration) ([]*contracts.CMessage, error) {
	if !c.IsActive() {
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

// Ack : Acknowledge messages in the redis stream
func (c *Consumer) Ack(ctx context.Context, cmessage *contracts.CMessage) error {
	if !c.IsActive() {
		return ConsumerDeadError
	}
	p := c.s.getPartitionFromKey(cmessage.PartitionKey)
	stream := partitionedStream(c.s.ns, c.s.name, p)
	return c.s.rdb.XAck(ctx, stream, c.group, cmessage.Id).Err()
}

// ShutDown : This will shut down the consumer. You will no longer be able to read or ack messages via this consumer.
// As an effect of this the partitions which were assigned to this consumer will be rebalanced and assigned to other
// consumers in the group
func (c *Consumer) ShutDown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sg := range c.segmentMap {
		sg.ShutDown()
	}
	c.shutDown <- true
	return nil
}

// GetID : Returns the unique id assigned to the consumer
func (c *Consumer) GetID() string {
	return c.id
}

// GetStreamName : Returns the name of the stream against which this consumer is registered
func (c *Consumer) GetStreamName() string {
	return c.s.name
}

// GetNameSpace : Returns the name space of registered consumer. This will always be the same as the namespace of the
// stream
func (c *Consumer) GetNameSpace() string {
	return c.s.ns
}

func (c *Consumer) rePartition(ctx context.Context, partitions partitions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Debug().Msgf("Re Partitioning started, partitions %v", partitions)
	toBeReleased := make([]partition, 0)
	for p := range c.segmentMap {
		if !partitions.Contains(p) {
			toBeReleased = append(toBeReleased, p)
		}
	}
	c.logger.Debug().Msgf("Need to Shutdown partitions : %v", toBeReleased)

	for _, p := range toBeReleased {
		c.segmentMap[p].ShutDown()
		delete(c.segmentMap, p)
	}

	c.logger.Debug().Msgf("partitions shut down Successfully : %v", toBeReleased)

	for _, p := range partitions {
		if _, ok := c.segmentMap[p]; !ok {
			c.logger.Debug().Msgf("Creating segment for partition : %v", p)
			sg, err := newSegment(ctx, c, p)
			if err != nil {
				return err
			}
			c.segmentMap[p] = sg
			c.logger.Debug().Msgf("Created segment for partition : %v", p)
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
		streams[i] = partitionedStream(c.GetNameSpace(), c.GetStreamName(), sg.partition)
		streams[i+segmentCount] = ">"
		i += 1
	}
	return streams
}

type pendingResponse struct {
	err        error
	partition  partition
	messageIds []string
}

func (c *Consumer) getPendingEntries(ctx context.Context) map[partition][]string {
	ch, pc := c.queuePending(ctx)
	pending := make(map[partition][]string)
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
			set := c.s.rdb.Set(ctx, heartBeat(c.GetNameSpace(), c.GetStreamName(), c.id, c.group), time.Now().UnixMilli(), heartBeatDuration)
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
		key := partitionedStream(c.s.ns, c.s.name, partition(i))
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

// IsActive : Returns true if a consumer is active
func (c *Consumer) IsActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.Debug().Msgf("Get Messages")
	return c.active
}
