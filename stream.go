package segmenter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"google.golang.org/protobuf/encoding/protojson"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

const controlLoopInterval = 100 * time.Millisecond
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
	mu        sync.Mutex
	rdb       *redis.Client
	consumers map[string]*Consumer

	ns     string
	name   string
	pcount int
	psize  int64
}

func newStream(rdb *redis.Client, ns string, name string, pcount int, psize int64) *Stream {
	s := &Stream{rdb: rdb, ns: ns, name: name, pcount: pcount, psize: psize, consumers: make(map[string]*Consumer)}
	go s.controlLoop()
	go s.maintenanceLoop()
	return s
}

func newStreamFromDTO(rdb *redis.Client, dto *streamDTO) *Stream {
	return newStream(rdb, dto.Ns, dto.Name, dto.Pcount, dto.Psize)
}

func (s *Stream) Send(ctx context.Context, m *contracts.PMessage) (string, error) {
	data, _ := protojson.Marshal(m)
	r := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.getRedisStream(m.GetPartitionKey()),
		MaxLen: s.psize,
		Values: map[string]interface{}{
			"data":         data,
			"partitionKey": m.GetPartitionKey(),
		},
	})
	return r.Result()
}

func (s *Stream) getRedisStream(partitionKey string) string {
	pc := partition(int(hash(partitionKey)) % s.pcount)
	return streamKey(s.ns, s.name, pc)
}

// RegisterConsumer : Will a redis consumer with the specified consumer group
// This will cause a partitioning re-balancing
func (s *Stream) registerConsumer(ctx context.Context, group string, batchSize int64, maxProcessingTime time.Duration) (*Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c, err := NewConsumer(ctx, s, batchSize, group, maxProcessingTime)
	if err != nil {
		return nil, err
	}

	err = s.join(ctx, c)
	if err != nil {
		return nil, err
	}
	s.consumers[c.id] = c
	return c, nil
}

func (s *Stream) join(ctx context.Context, c *Consumer) error {
	return s.rebalance(ctx, &memberChangeInfo{
		Reason:     join,
		Group:      c.group,
		ConsumerId: c.id,
		Ts:         time.Now().UnixMilli(),
	})
}

func (s *Stream) rebalance(ctx context.Context, changeInfo *memberChangeInfo) error {
	lock, err := acquireAdminLock(ctx, s.rdb, s.ns, s.name, 1*time.Second)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	members, err := s.members(ctx, changeInfo.Group)
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
	sort.Sort(members)

	newMembers := s.computeMemberships(members)
	err = s.updateMembers(ctx, newMembers)
	if err != nil {
		return err
	}
	log.Printf("[Rebalance] Sending control message %v\n", newMembers)
	return s.sendControlMessage(ctx, newMembers)
}
func (s *Stream) allMembers(ctx context.Context) (Members, error) {
	bytes, err := s.rdb.Get(ctx, s.memberShipKey()).Bytes()
	if err == redis.Nil {
		return []member{}, nil
	} else if err != nil {
		return nil, err
	} else {
		var members Members
		err = json.Unmarshal(bytes, &members)
		if err != nil {
			return nil, err
		}
		return members, nil
	}
}
func (s *Stream) members(ctx context.Context, group string) (Members, error) {
	allMembers, err := s.allMembers(ctx)
	if err != nil {
		return nil, err
	}
	return allMembers.FilterBy(group), nil
}

func (s *Stream) memberShipKey() string {
	return fmt.Sprintf("__%s:__%s:__mbsh", s.ns, s.name)
}

func (s *Stream) computeMemberships(members Members) Members {
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

func (s *Stream) updateMembers(ctx context.Context, newMembers Members) error {
	data, err := json.Marshal(newMembers)
	if err != nil {
		return err
	}
	err = s.rdb.Set(ctx, s.memberShipKey(), data, 0).Err()
	if err != nil {
		return err
	}
	return nil
}

func (s *Stream) sendControlMessage(ctx context.Context, members Members) error {
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

func (s *Stream) controlLoop() {
	log.Printf("Control Loop Strted for s: %s\n", s.name)
	lastMessageId := "$"
	stream := s.controlKey()
	ctx := context.Background()
	for {
		result, err := s.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, lastMessageId},
			Count:   1,
			Block:   1 * time.Second,
		}).Result()

		if err == redis.Nil {
			// Do nothing
		} else if err != nil {
			log.Printf(" Error Happened while fetching control key, %v\n", err)
		} else {
			message := result[0].Messages[0]
			data := message.Values["c"].(string)
			var members Members
			_ = json.Unmarshal([]byte(data), &members)
			log.Printf("Control Loop: Recieved Control Messages %v \n", members)
			// We are requesting results from only one s so getting 0th result by default
			s.mu.Lock()
			for _, m := range members {
				if c, ok := s.consumers[m.ConsumerId]; ok {
					log.Printf("Starting to rebalance consumer %s", m.ConsumerId)
					// TODO: Handle Error, not sure right now what to do on repartitioning error
					err = c.rePartition(ctx, m.Partitions)
					if err != nil {
						log.Printf("Error happened while repatitioning consumer %d, %v\n", c.id, err)
					}
				}
			}
			lastMessageId = message.ID
			s.mu.Unlock()
		}
		time.Sleep(controlLoopInterval)
	}
}

func (s *Stream) maintenanceLoop() {
	for {
		ctx := context.Background()
		lock, err := acquireAdminLock(ctx, s.rdb, s.ns, s.name, 100*time.Millisecond)
		if err != nil {
			time.Sleep(maintenanceLoopInterval / 2)
			continue
		}
		members, err := s.allMembers(ctx)
		if err != nil {
			_ = lock.Release(ctx)
			time.Sleep(maintenanceLoopInterval / 2)
			continue
		}
		deadMembers := s.calculateDeadMembers(ctx, members)
		if len(deadMembers) == 0 {
			_ = lock.Release(ctx)
			time.Sleep(maintenanceLoopInterval)
			continue
		}
		aliveMembers := members.RemoveAll(deadMembers)
		newMembers := s.computeMemberships(aliveMembers)
		err = s.updateMembers(ctx, newMembers)
		if err != nil {
			_ = lock.Release(ctx)
			time.Sleep(maintenanceLoopInterval / 2)
			continue
		}
		log.Printf("[Maintainence Loop] : Sending control Message %v\n", newMembers)
		err = s.sendControlMessage(ctx, newMembers)
		if err != nil {
			_ = lock.Release(ctx)
			time.Sleep(maintenanceLoopInterval / 2)
			continue
		}
		_ = lock.Release(ctx)
		time.Sleep(maintenanceLoopInterval)
	}

}

func (s *Stream) calculateDeadMembers(ctx context.Context, members Members) Members {
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

func (s *Stream) shutDownConsumer(ctx context.Context, consumerId string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.consumers[consumerId]
	delete(s.consumers, consumerId)
	return s.rebalance(ctx, &memberChangeInfo{
		Reason:     leave,
		ConsumerId: consumerId,
		Group:      c.group,
		Ts:         time.Now().UnixMilli(),
	})
}
