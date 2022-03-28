package segmenter

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/segmenter/locker"
	"github.com/rs/zerolog"
	"time"
)

const lockDuration = 10 * time.Second

type segment struct {
	c         *Consumer
	partition partition
	lock      locker.Lock
	shutDown  chan bool

	logger *zerolog.Logger
}

func newSegment(ctx context.Context, c *Consumer, partition partition) (*segment, error) {
	logger := c.logger.With().Int("partition", int(partition)).Logger()
	sg := &segment{partition: partition, c: c, shutDown: make(chan bool), logger: &logger}

	lock, err := c.s.locker.Acquire(ctx, sg.partitionLockKey(), lockDuration, c.id)
	if err != nil {
		logger.Error().Msgf("Failed to Acquire lock with key %s, %v", c.GetStreamName(), err)
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
			err := sg.lock.Release(ctx)
			if err != nil {
				sg.logger.Debug().Err(err).Msgf("Releasing lock with key stream %s, partition %d", sg.c.GetStreamName(), sg.partition)
			}
			return
		default:
			err := sg.lock.Refresh(ctx, lockDuration)
			if err != nil {
				sg.logger.Debug().Err(err).Msgf("Error happened while refreshing lock %s", sg.lock.Key())
			}
		}
		time.Sleep(1000 * time.Millisecond)
	}

}

func (sg *segment) pendingEntries(ctx context.Context, ch chan *pendingResponse) {
	pending, err := sg.c.s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: partitionedStream(sg.c.GetNameSpace(), sg.c.GetStreamName(), sg.partition),
		Group:  sg.c.group,
		Idle:   sg.c.maxProcessingTime,
		Start:  "-",
		End:    "+",
		Count:  sg.c.batchSize,
	}).Result()

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

	sg.logger.Debug().Msgf("Length of pending entries : %d", len(messageIds))
	ch <- &pendingResponse{
		err:        nil,
		partition:  sg.partition,
		messageIds: messageIds,
	}
}

func (sg *segment) claimEntries(ctx context.Context, ch chan *claimResponse, ids []string) {
	result, err := sg.c.s.rdb.XClaim(ctx, &redis.XClaimArgs{
		Stream:   partitionedStream(sg.c.GetNameSpace(), sg.c.GetStreamName(), sg.partition),
		Group:    sg.c.group,
		Consumer: sg.c.id,
		Messages: ids,
	}).Result()

	if err != nil {
		ch <- &claimResponse{
			err:       err,
			partition: sg.partition,
			messages:  nil,
		}
		return
	}

	sg.logger.Info().Msgf("Claimed %d messages", len(result))
	ch <- &claimResponse{
		err:       nil,
		partition: sg.partition,
		messages:  mapXMessageToCMessage(result),
	}
}

func (sg *segment) partitionLockKey() string {
	return fmt.Sprintf("__%s:__%s_%s:strm_%d", sg.c.s.ns, sg.c.s.name, sg.c.group, sg.partition)
}

func (sg *segment) ShutDown() {
	sg.shutDown <- true
}
