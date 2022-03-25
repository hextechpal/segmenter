package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/segmenter/locker"
	"github.com/hextechpal/segmenter/internal/segmenter/store"
	"github.com/hextechpal/segmenter/internal/segmenter/utils"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protojson"
	"math"
	"sort"
	"sync"
	"time"
)

const maintenanceLoopInterval = 500 * time.Millisecond
const controlStreamSize = 256
const controlLoopInterval = 100 * time.Millisecond

type StreamDTO struct {
	Ns     string `json:"Ns"`
	Name   string `json:"Name"`
	Pcount int    `json:"Pcount"`
	Psize  int64  `json:"Psize"`
}

func NewStreamDTO(s *Stream) *StreamDTO {
	return &StreamDTO{
		Ns:     s.ns,
		Name:   s.name,
		Pcount: s.pcount,
		Psize:  s.psize,
	}
}

type Stream struct {
	mu        sync.Mutex
	rdb       *redis.Client
	store     store.Store
	locker    locker.Locker
	consumers map[string]*Consumer

	ns     string
	name   string
	pcount int
	psize  int64

	logger *zerolog.Logger
}

type NewStreamArgs struct {
	Rdb    *redis.Client
	Ns     string
	Name   string
	Pcount int
	Psize  int64
	Logger *zerolog.Logger
}

func NewStream(_ context.Context, args *NewStreamArgs) *Stream {
	nLogger := args.Logger.With().Str("stream", args.Name).Int("Pcount", args.Pcount).Logger()
	s := &Stream{
		rdb:       args.Rdb,
		store:     store.NewRedisStore(args.Rdb),
		locker:    locker.NewRedisLocker(args.Rdb),
		consumers: make(map[string]*Consumer),
		ns:        args.Ns,
		name:      args.Name,
		pcount:    args.Pcount,
		psize:     args.Psize,
		logger:    &nLogger,
	}
	go s.maintenanceLoop()
	go s.controlLoop()
	return s
}

func NewStreamFromDTO(ctx context.Context, rdb *redis.Client, dto *StreamDTO, logger *zerolog.Logger) *Stream {
	return NewStream(ctx, &NewStreamArgs{
		Rdb:    rdb,
		Ns:     dto.Ns,
		Name:   dto.Name,
		Pcount: dto.Pcount,
		Psize:  dto.Psize,
		Logger: logger,
	})
}

func (s *Stream) Send(ctx context.Context, m *contracts.PMessage) (string, error) {
	data, _ := protojson.Marshal(m)
	id, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.partitionedStream(m.GetPartitionKey()),
		MaxLen: s.psize,
		Values: map[string]interface{}{
			"data":         data,
			"partitionKey": m.GetPartitionKey(),
		},
	}).Result()

	if err != nil {
		s.logger.Error().Msgf("Error happened while sending message %v", err)
		return "", err
	}
	s.logger.Info().Msgf("Sent message with Id %v", id)
	return id, nil
}

func (s *Stream) GetName() string {
	return s.name
}

func (s *Stream) GetPartitionSize() int64 {
	return s.psize
}

func (s *Stream) GetPartitionCount() int {
	return s.pcount
}

func (s *Stream) partitionedStream(partitionKey string) string {
	return PartitionedStream(s.ns, s.name, s.getPartitionFromKey(partitionKey))
}

func (s *Stream) getPartitionFromKey(partitionKey string) Partition {
	return Partition(int(utils.Hash(partitionKey)) % s.pcount)
}

func (s *Stream) allMembers(ctx context.Context) (members, error) {
	members := make([]member, 0)
	err := s.store.GetKey(ctx, s.memberShipKey(), &members)
	if err != nil {
		return nil, err
	}
	return members, nil
}

func (s *Stream) members(ctx context.Context, group string) (members, error) {
	allMembers, err := s.allMembers(ctx)
	if err != nil {
		return nil, err
	}
	return allMembers.FilterBy(group), nil
}

func (s *Stream) memberShipKey() string {
	return fmt.Sprintf("__%s:__%s:__mbsh", s.ns, s.name)
}

func (s *Stream) updateMembers(ctx context.Context, oldMembers members) error {
	newMembers := s.computeMemberships(oldMembers)
	err := s.store.SetKey(ctx, s.memberShipKey(), newMembers)
	if err != nil {
		return err
	}
	return s.sendControlMessage(ctx, newMembers)
}

func (s *Stream) computeMemberships(members members) members {
	sort.Sort(members)
	allPartitions := make([]Partition, s.pcount)
	for i := 0; i < s.pcount; i++ {
		allPartitions[i] = Partition(i)
	}
	partitionLen := int(math.Round(float64(s.pcount) / float64(members.Len())))
	newMembers := make([]member, members.Len())
	for i := 0; i < members.Len(); i++ {
		var partitions Partitions
		if i == members.Len()-1 {
			partitions = allPartitions[i*partitionLen:]
		} else {
			partitions = allPartitions[i*partitionLen : (i+1)*partitionLen]
		}
		newMembers[i] = member{
			ID:         members[i].ID,
			JoinedAt:   members[i].JoinedAt,
			Group:      members[i].Group,
			Partitions: partitions,
		}
	}
	return newMembers
}

func (s *Stream) sendControlMessage(ctx context.Context, members members) error {
	val, err := json.Marshal(members)
	if err != nil {
		return err
	}
	return s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.controlKey(),
		MaxLen: controlStreamSize,
		Values: map[string]interface{}{
			"c": val,
		},
	}).Err()
}

func (s *Stream) controlKey() string {
	return fmt.Sprintf("__%s:__%s:__ctrl", s.ns, s.name)
}

func (s *Stream) maintenanceLoop() {
	for {
		err := s.performMaintenance(context.Background())
		if err != nil {
			time.Sleep(200 * time.Millisecond)
		}
		time.Sleep(maintenanceLoopInterval)
	}

}

func (s *Stream) performMaintenance(ctx context.Context) error {
	lock, err := s.locker.Acquire(ctx, StreamAdmin(s.ns, s.name), 100*time.Millisecond, "")
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	members, err := s.allMembers(ctx)
	if err != nil {
		return err
	}

	deadMembers := s.calculateDeadMembers(ctx, members)
	if len(deadMembers) == 0 {
		return nil
	}
	aliveMembers := members.RemoveAll(deadMembers)
	sort.Sort(aliveMembers)
	return s.updateMembers(ctx, aliveMembers)
}

func (s *Stream) calculateDeadMembers(ctx context.Context, members members) members {
	keys := make([]string, members.Len())
	for i := 0; i < members.Len(); i++ {
		keys[i] = HeartBeat(s.ns, s.name, members[i].ID)
	}
	res := s.rdb.MGet(ctx, keys...)
	dead := make([]member, 0)
	for i, hb := range res.Val() {
		if hb == nil {
			dead = append(dead, members[i])
		}
	}
	return dead
}

func (s *Stream) rebalance(ctx context.Context, changeInfo *memberChangeInfo) error {
	lock, err := s.locker.Acquire(ctx, StreamAdmin(s.ns, s.name), 1*time.Second, changeInfo.ConsumerId)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	members, err, done := s.computeMembers(ctx, changeInfo)
	if err != nil {
		return err
	}

	if done {
		return nil
	}

	return s.updateMembers(ctx, members)
}

func (s *Stream) computeMembers(ctx context.Context, changeInfo *memberChangeInfo) (members, error, bool) {
	members, err := s.members(ctx, changeInfo.Group)
	if err != nil {
		return nil, err, true
	}

	if changeInfo.Reason == join && members.Contains(changeInfo.ConsumerId) {
		return nil, nil, true
	}

	if changeInfo.Reason == leave && !members.Contains(changeInfo.ConsumerId) {
		return nil, nil, true
	}

	// Add and sort members
	newMember := member{
		ID:       changeInfo.ConsumerId,
		JoinedAt: changeInfo.Ts,
		Group:    changeInfo.Group,
	}
	if changeInfo.Reason == join {
		members = members.Add(newMember)
	} else {
		members = members.Remove(newMember.ID)
	}
	return members, nil, false
}

func (s *Stream) controlLoop() {
	s.logger.Debug().Msg("Control Loop Started")
	lastId := "$"
	for {
		lastId = s.processControlMessage(context.Background(), s.controlKey(), lastId)
		time.Sleep(controlLoopInterval)
	}
}

func (s *Stream) processControlMessage(ctx context.Context, stream string, lastId string) string {
	result, err := s.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream, lastId},
		Count:   1,
	}).Result()

	if err != nil || len(result) == 0 {
		if err != redis.Nil {
			s.logger.Error().Err(err).Msgf("Error Happened while fetching control key")
		}
		return lastId
	} else {
		message := result[0].Messages[0]
		data := message.Values["c"].(string)
		if message.ID == lastId {
			return lastId
		}
		var members members
		_ = json.Unmarshal([]byte(data), &members)
		s.logger.Debug().Msgf("Control Loop: Received Control Messages Members: %v", members)
		// We are requesting results from only one s so getting 0th result by default
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, m := range members {
			if c, ok := s.consumers[m.ID]; ok {
				c.logger.Debug().Msg("Starting to rebalance consumer")
				// TODO: Handle Error, not sure right now what to do on repartitioning error
				err = c.rePartition(ctx, m.Partitions)
				if err != nil {
					c.logger.Error().Msgf("Error happened while repartitioning consumer, %v", err)
				}
			}
		}
		return message.ID
	}
}

func (s *Stream) RegisterConsumer(ctx context.Context, group string, batchSize int64, maxProcessingTime time.Duration) (*Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := NewConsumer(ctx, &NewConsumerArgs{
		Store:             s.store,
		Locker:            s.locker,
		Stream:            s,
		Group:             group,
		BatchSize:         batchSize,
		MaxProcessingTime: maxProcessingTime,
		Logger:            s.logger,
	})

	if err != nil {
		return nil, err
	}
	s.consumers[c.id] = c
	go s.rebalance(ctx, &memberChangeInfo{
		Reason:     join,
		Group:      c.group,
		ConsumerId: c.id,
		Ts:         time.Now().UnixMilli(),
	})
	return c, nil
}
