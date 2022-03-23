package segmenter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/encoding/protojson"
	"math"
	"sort"
	"sync"
	"time"
)

const maintenanceLoopInterval = 500 * time.Millisecond
const controlStreamSize = 256

type streamDTO struct {
	Ns     string `json:"ns"`
	Name   string `json:"name"`
	Pcount int    `json:"pcount"`
	Psize  int64  `json:"psize"`
}

func newStreamDTO(s *Stream) *streamDTO {
	return &streamDTO{
		Ns:     s.ns,
		Name:   s.name,
		Pcount: s.pcount,
		Psize:  s.psize,
	}
}

type Stream struct {
	mu  sync.Mutex
	rdb *redis.Client

	ns     string
	name   string
	pcount int
	psize  int64

	logger *zerolog.Logger
}

func newStream(rdb *redis.Client, ns string, name string, pcount int, psize int64, logger *zerolog.Logger) *Stream {
	nLogger := logger.With().Str("stream", name).Int("pcount", pcount).Logger()
	s := &Stream{rdb: rdb, ns: ns, name: name, pcount: pcount, psize: psize, logger: &nLogger}
	err := s.performMaintenance(context.Background())
	if err != nil {
		return nil
	}
	go s.maintenanceLoop()
	return s
}

func newStreamFromDTO(rdb *redis.Client, dto *streamDTO, logger *zerolog.Logger) *Stream {
	return newStream(rdb, dto.Ns, dto.Name, dto.Pcount, dto.Psize, logger)
}

func (s *Stream) Send(ctx context.Context, m *contracts.PMessage) (string, error) {
	data, _ := protojson.Marshal(m)
	id, err := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.getRedisStream(m.GetPartitionKey()),
		MaxLen: s.psize,
		Values: map[string]interface{}{
			"data":         data,
			"partitionKey": m.GetPartitionKey(),
		},
	}).Result()

	if err != nil {
		s.logger.Error().Msgf("Error happened while sending message &v", err)
		return "", err
	}
	s.logger.Info().Msgf("Sent message with Id %v", id)
	return id, nil
}

func (s *Stream) getRedisStream(partitionKey string) string {
	return partitionedStreamKey(s.ns, s.name, s.getPartitionFromKey(partitionKey))
}

func (s *Stream) getPartitionFromKey(partitionKey string) partition {
	return partition(int(hash(partitionKey)) % s.pcount)
}

func (s *Stream) allMembers(ctx context.Context) (members, error) {
	bytes, err := s.rdb.Get(ctx, s.memberShipKey()).Bytes()
	if err == redis.Nil {
		return []member{}, nil
	} else if err != nil {
		return nil, err
	} else {
		var members members
		err = json.Unmarshal(bytes, &members)
		if err != nil {
			return nil, err
		}
		return members, nil
	}
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
	data, err := json.Marshal(newMembers)
	if err != nil {
		return err
	}
	err = s.rdb.Set(ctx, s.memberShipKey(), data, 0).Err()
	if err != nil {
		return err
	}
	return s.sendControlMessage(ctx, newMembers)
}

func (s *Stream) computeMemberships(members members) members {
	sort.Sort(members)
	allPartitions := make([]partition, s.pcount)
	for i := 0; i < s.pcount; i++ {
		allPartitions[i] = partition(i)
	}
	partitionLen := int(math.Round(float64(s.pcount) / float64(members.Len())))
	newMembers := make([]member, members.Len())
	for i := 0; i < members.Len(); i++ {
		var partitions partitions
		if i == members.Len()-1 {
			partitions = allPartitions[i*partitionLen:]
		} else {
			partitions = allPartitions[i*partitionLen : (i+1)*partitionLen]
		}
		newMembers[i] = member{
			ConsumerId: members[i].ConsumerId,
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
	lock, err := acquireAdminLock(ctx, s.rdb, s.ns, s.name, 100*time.Millisecond)
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
		keys[i] = heartBeatKey(s.ns, s.name, members[i].ConsumerId)
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
