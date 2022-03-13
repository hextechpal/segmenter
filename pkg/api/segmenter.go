package api

import (
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/common"
	"github.com/hextechpal/segmenter/internal/pkg/segmenter"
	sr "github.com/hextechpal/segmenter/internal/pkg/segmenter/redis"
	"log"
	"time"
)

type Segmenter struct {
	ns        string
	rdb       *redis.Client
	consumers []segmenter.Consumer
}

func NewSegmenter(c *Config) (*Segmenter, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Address,
		Username: c.Username,
		Password: c.Password,
		DB:       1,
	})

	status := rdb.Ping(context.TODO())
	if status.Err() != nil {
		return nil, status.Err()
	}
	return &Segmenter{rdb: rdb, ns: c.Namespace}, nil
}

// RegisterProducer : Will create stream based on the partition count
// The format for the keys will be segmenter_${streamName}_#{0..partitionCount}
// It also registers this stream record (immutable) in redis so that re-registrations
// do not have any effect and appropriate errors can be thrown
func (s *Segmenter) RegisterProducer(ctx context.Context, psize int64, pcount int, stream string) (segmenter.Producer, error) {
	err := s.registerStream(ctx, stream, pcount)
	if err != nil {
		return nil, err
	}
	return sr.NewProducer(s.rdb, s.ns, stream, psize, pcount), nil
}

// RegisterConsumer : Will a redis consumer with the specified consumer group
// This will cause a partitioning re-balancing
func (s *Segmenter) RegisterConsumer(ctx context.Context, stream string, maxWaitTime time.Duration, batchSize int) (segmenter.Consumer, error) {
	c := sr.NewConsumer(s.rdb, maxWaitTime, batchSize, stream, s.ns)
	err := s.updateMemberShip(ctx, c.Id, stream)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (s *Segmenter) registerStream(ctx context.Context, stream string, pcount int) error {
	key := segmenter.MemberShipKey(s.ns, stream)
	lock, err := common.AcquireLock(ctx, s.rdb, key, 2*time.Second)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	membership, err := segmenter.QueryMembershipL(ctx, s.rdb, key)
	if err == redis.Nil {
		return segmenter.UpdateMembershipL(ctx, s.rdb, key, &segmenter.Membership{
			Stream:        stream,
			Pcount:        pcount,
			Distributions: []segmenter.Distribution{},
		})
	} else if err != nil {
		return err
	} else if membership.Pcount != pcount {
		return errors.New("trying to change partition count for stream " + stream)
	} else {
		return nil
	}
}

func (s *Segmenter) updateMemberShip(ctx context.Context, cid, stream string) error {
	key := segmenter.MemberShipKey(s.ns, stream)
	lock, err := common.AcquireLock(ctx, s.rdb, key, 2*time.Second)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	membership, err := segmenter.QueryMembershipL(ctx, s.rdb, key)
	if err != nil {
		if err == redis.Nil {
			//TODO: handle this better
			log.Printf("Stream does not exist. Stream should be created before consumer")
		}
		return err
	}

	// TODO: partition count cannot be zero add a validation for that
	cc := len(membership.Distributions) + 1
	dists := make([]segmenter.Distribution, cc)
	if membership.Pcount < cc {
		copy(dists[:], append(membership.Distributions, segmenter.Distribution{ConsumerId: cid, Partitions: []int{}}))
	} else if len(membership.Distributions) == 0 {
		p := make([]int, membership.Pcount)
		for i := 0; i < membership.Pcount; i++ {
			p[i] = i
		}
		dists[0] = segmenter.Distribution{
			ConsumerId: cid,
			Partitions: p,
		}
	} else {
		assigned := make([]int, 0)
		ppw := membership.Pcount / cc

		for i, dist := range membership.Distributions {
			if len(dist.Partitions) > ppw {
				dists[i] = segmenter.Distribution{
					ConsumerId: dist.ConsumerId,
					Partitions: dist.Partitions[:ppw],
				}
				assigned = append(assigned, dist.Partitions[ppw:]...)
			} else {
				dists[i] = dist
			}

		}
		dists[cc-1] = segmenter.Distribution{
			ConsumerId: cid,
			Partitions: assigned,
		}
	}
	err = segmenter.UpdateMembershipL(ctx, s.rdb, key, &segmenter.Membership{
		Stream:        membership.Stream,
		Pcount:        membership.Pcount,
		Distributions: dists,
	})
	if err != nil {
		return err
	}
	return nil
}
