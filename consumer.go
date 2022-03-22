package segmenter

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"log"
	"sync"
	"time"
)

const controlLoopInterval = 100 * time.Millisecond
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

func NewConsumer(ctx context.Context, stream *Stream, batchSize int64, group string, maxProcessingTime time.Duration) (*Consumer, error) {
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

	err := c.startControlLoop(ctx)
	if err != nil {
		return nil, err
	}

	err = c.join(ctx)
	if err != nil {
		return nil, err
	}
	go c.beat()
	return c, nil
}

func (c *Consumer) join(ctx context.Context) error {
	return c.rebalance(ctx, &memberChangeInfo{
		Reason:     join,
		Group:      c.group,
		ConsumerId: c.id,
		Ts:         time.Now().UnixMilli(),
	})
}

func (c *Consumer) rebalance(ctx context.Context, changeInfo *memberChangeInfo) error {
	lock, err := acquireAdminLock(ctx, c.rdb, c.GetNameSpace(), c.GetStreamName(), 1*time.Second)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	members, err := c.s.members(ctx, changeInfo.Group)
	if err != nil {
		return err
	}

	if changeInfo.Reason == join && members.Contains(changeInfo.ConsumerId) {
		return nil
	}

	if changeInfo.Reason == leave && !members.Contains(changeInfo.ConsumerId) {
		return nil
	}

	// Add and sort members
	newMember := member{
		ConsumerId: changeInfo.ConsumerId,
		JoinedAt:   changeInfo.Ts,
		Group:      changeInfo.Group,
	}
	if changeInfo.Reason == join {
		members = members.Add(newMember)
	} else {
		members = members.Remove(newMember.ConsumerId)
	}

	return c.s.updateMembers(ctx, members)
}

func (c *Consumer) GetID() string {
	return c.id
}

func (c *Consumer) GetStreamName() string {
	return c.s.name
}

func (c *Consumer) beat() {
	ctx := context.Background()
	for {
		select {
		case <-c.shutDown:
			return
		default:
			set := c.rdb.Set(ctx, heartBeatKey(c.GetNameSpace(), c.GetStreamName(), c.id), time.Now().UnixMilli(), heartBeatDuration)
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
	log.Printf("[%s], Get Messages called \n", c.GetID())
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
		log.Printf(" [%s] Sending pending entries request for Partition, %d", c.id, sg.partition)
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
	log.Printf("[%s] Consumer reading new messages from streams %v\n", c.id, streamsKey)
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

	log.Printf("[%s] Result for new messages %v\n", c.id, result)
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

func (c *Consumer) ShutDown() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sg := range c.segmentMap {
		sg.ShutDown()
	}
	c.shutDown <- true
	return nil
}
func (c *Consumer) controlLoop(lastId string) {
	log.Printf("Control Loop Strted for consumer : %s\n", c.id)
	lastMessageId := lastId
	stream := c.s.controlKey()
	ctx := context.Background()
	for {
		select {
		case <-c.shutDown:
			err := c.stop(ctx)
			if err != nil {
				log.Printf("[%s] Error happened while stopping consumer, %v", c.id, err)
			}
			return
		default:
			lastMessageId = c.processControlMessage(ctx, stream, lastMessageId)
		}
		time.Sleep(controlLoopInterval)
	}

}

func (c *Consumer) stop(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.active = false
	return c.rebalance(ctx, &memberChangeInfo{
		Reason:     leave,
		ConsumerId: c.id,
		Group:      c.group,
		Ts:         time.Now().UnixMilli(),
	})

}

func (c *Consumer) processControlMessage(ctx context.Context, stream string, lastMessageId string) string {
	result, err := c.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream, lastMessageId},
		Count:   1,
		Block:   time.Second,
	}).Result()

	if err != nil || len(result) == 0 {
		if err != redis.Nil {
			log.Printf("[%s] Error Happened while fetching control key, %v\n", c.id, err)
		}
		return lastMessageId
	} else {
		message := result[0].Messages[0]
		data := message.Values["c"].(string)
		var members members
		_ = json.Unmarshal([]byte(data), &members)
		log.Printf("[%s] Control Loop: Recieved Control Messages %v \n", c.id, members)
		// We are requesting results from only one s so getting 0th result by default
		m := members.find(c.id)
		if members.Contains(c.id) {
			log.Printf("Starting to rebalance consumer %s", c.id)
			// TODO: Handle Error, not sure right now what to do on repartitioning error
			err = c.rePartition(ctx, m.Partitions)
			if err != nil {
				log.Printf("Error happened while repatitioning consumer %d, %v\n", c.id, err)
			}
		}
		return message.ID
	}
}

func (c *Consumer) startControlLoop(ctx context.Context) error {
	lastId, err := c.getLatestControlMessageId(ctx)
	if err != nil {
		return err
	}
	log.Printf("[%s] Received lastId as %s", c.id, lastId)
	go c.controlLoop(lastId)
	return nil
}

func (c *Consumer) getLatestControlMessageId(ctx context.Context) (string, error) {
	result, err := c.rdb.XRevRangeN(ctx, c.s.controlKey(), "+", "-", 1).Result()

	if err != nil && err != redis.Nil {
		return "", err
	}

	if err == redis.Nil || len(result) == 0 {
		return "0-0", nil
	}

	return result[0].ID, nil

}

func (c *Consumer) GetNameSpace() string {
	return c.s.ns
}
