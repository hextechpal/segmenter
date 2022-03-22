package segmenter

import (
	"context"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"log"
	"time"
)

const lockDuration = 10 * time.Second

type segment struct {
	c         *Consumer
	partition partition
	lock      *redislock.Lock
	shutDown  chan bool
}

func newSegment(ctx context.Context, c *Consumer, partition partition) (*segment, error) {
	sg := &segment{partition: partition, c: c, shutDown: make(chan bool)}

	err := sg.checkConsumerGroup()
	if err != nil {
		return nil, err
	}

	lock, err := acquireLock(ctx, c.rdb, sg.partitionedStream(), lockDuration, c.id)
	if err != nil {
		log.Printf("[%s] Failed to Acquire lock with key %s, %v", c.id, c.s.name, err)
		return nil, err
	}
	sg.lock = lock

	go sg.refreshLock()
	return sg, nil
}

func (sg *segment) refreshLock() {
	ctx := context.Background()
	for {
		select {
		case <-sg.shutDown:
			_ = sg.releaseLock(ctx)
			return
		default:
			err := sg.lock.Refresh(ctx, lockDuration, nil)
			if err != nil {
				log.Printf("Error happened while refreshing lock %s Consumer %s, %v", sg.lock.Key(), sg.lock.Metadata(), err)
			}
		}
		time.Sleep(1000 * time.Millisecond)
	}

}

func (sg *segment) releaseLock(ctx context.Context) error {
	log.Printf("Releasing lock with key stream %s, partition %d\n", sg.c.GetStreamName(), sg.partition)
	return sg.lock.Release(ctx)
}

func (sg *segment) pendingEntries(ctx context.Context, ch chan *pendingResponse) {
	pending, err := sg.c.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: sg.partitionedStream(),
		Group:  sg.c.group,
		Idle:   sg.c.maxProcessingTime,
		Start:  "-",
		End:    "+",
		Count:  sg.c.batchSize,
	}).Result()

	if err == redis.Nil {
		ch <- &pendingResponse{
			err:        nil,
			partition:  sg.partition,
			messageIds: []string{},
		}
		return
	}

	if err != nil {
		ch <- &pendingResponse{
			err:        err,
			partition:  sg.partition,
			messageIds: nil,
		}
		return
	}

	//TODO handle retry count and move to dead letter queue
	messageIds := make([]string, 0)
	for _, xp := range pending {
		messageIds = append(messageIds, xp.ID)
	}

	ch <- &pendingResponse{
		err:        nil,
		partition:  sg.partition,
		messageIds: messageIds,
	}
}

func (sg *segment) claimEntries(ctx context.Context, ch chan *claimResponse, ids []string) {
	result, err := sg.c.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   sg.partitionedStream(),
		Group:    sg.c.group,
		Consumer: sg.c.id,
		Messages: ids,
	}).Result()

	if err == redis.Nil {
		ch <- &claimResponse{
			err:       nil,
			partition: sg.partition,
			messages:  []*contracts.CMessage{},
		}
		return
	}

	if err != nil {
		ch <- &claimResponse{
			err:       err,
			partition: sg.partition,
			messages:  nil,
		}
		return
	}

	ch <- &claimResponse{
		err:       nil,
		partition: sg.partition,
		messages:  mapXMessageToCMessage(result),
	}
}

func (sg *segment) partitionedStream() string {
	return streamKey(sg.c.s.ns, sg.c.GetStreamName(), sg.partition)
}

func (sg *segment) ShutDown() {
	sg.shutDown <- true
}

func (sg *segment) checkConsumerGroup() error {
	key := sg.partitionedStream()
	err := sg.c.rdb.XGroupCreateMkStream(context.Background(), key, sg.c.group, "$").Err()
	if err != nil {
		if err.Error() == "BUSYGROUP Consumer Group name already exists" {
			return nil
		}
		log.Printf("Error while registering Consumer, %v", err)
		return err
	}
	return nil
}
