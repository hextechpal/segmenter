package api

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/common"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

const controlLoopInterval = 100 * time.Millisecond
const maintenanceLoopInterval = 500 * time.Millisecond
const controlStreamSize = 256

type StreamDTO struct {
	Ns     string `json:"ns"`
	Name   string `json:"name"`
	Pcount int    `json:"pcount"`
	Psize  int64  `json:"psize"`
}

type Stream struct {
	mu        sync.Mutex
	rdb       *redis.Client
	consumers map[string]*Consumer

	Ns     string
	Name   string
	Pcount int
	Psize  int64
}

func NewStream(rdb *redis.Client, ns string, name string, pcount int, psize int64) *Stream {
	s := &Stream{rdb: rdb, Ns: ns, Name: name, Pcount: pcount, Psize: psize, consumers: make(map[string]*Consumer)}
	s.start()
	return s
}

func NewStreamFromDTO(rdb *redis.Client, dto *StreamDTO) *Stream {
	s := &Stream{rdb: rdb, Ns: dto.Ns, Name: dto.Name, Pcount: dto.Pcount, Psize: dto.Psize, consumers: make(map[string]*Consumer)}
	s.start()
	return s
}

func (s *Stream) start() {
	go s.StartControlLoop()
	go s.StartMaintenanceLoop()
}

func (s *Stream) Send(ctx context.Context, m *contracts.PMessage) (string, error) {
	r := s.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: s.getRedisStream(m.GetPartitionKey()),
		MaxLen: s.Psize,
		Values: map[string]interface{}{
			"m": map[string]interface{}{
				"metadata": m.GetMetadata(),
				"data":     m.GetData(),
			},
		},
	})
	return r.Result()
}

func (s *Stream) getRedisStream(partitionKey string) string {
	pc := int(common.Hash(partitionKey)) % s.Pcount
	return fmt.Sprintf("__%s:__strm:%s_%d", s.Ns, s.Name, pc)
}

func (s *Stream) Save(ctx context.Context, key string) error {
	val := map[string]interface{}{
		"Name":   s.Name,
		"Pcount": s.Pcount,
		"Psize":  s.Psize,
	}
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, key, data, 0).Err()
}

// RegisterConsumer : Will a redis consumer with the specified consumer group
// This will cause a partitioning re-balancing
func (s *Stream) registerConsumer(ctx context.Context, batchSize int) (*Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, err := NewConsumer(ctx, s.rdb, batchSize, s.Name, s.Ns)
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
	return s.rebalance(ctx, &MemberChangeInfo{
		Reason:     JOIN,
		ConsumerId: c.GetID(),
		Ts:         time.Now().UnixMilli(),
	})
}

func (s *Stream) rebalance(ctx context.Context, changeInfo *MemberChangeInfo) error {
	lock, err := common.AcquireAdminLock(ctx, s.rdb, s.Ns, 1*time.Second)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)

	members, err := s.members(ctx)
	if err != nil {
		return err
	}

	if changeInfo.Reason == JOIN && members.Contains(changeInfo.ConsumerId) {
		return nil
	}

	if changeInfo.Reason == LEAVE && !members.Contains(changeInfo.ConsumerId) {
		return nil
	}

	// Add and sort members
	newMember := Member{
		ConsumerId: changeInfo.ConsumerId,
		JoinedAt:   changeInfo.Ts,
	}
	if changeInfo.Reason == JOIN {
		members = members.Add(newMember)
	} else {
		members = members.Remove(newMember)
	}
	sort.Sort(members)
	newMembers := s.computeMemberships(members)
	err = s.updateMembers(ctx, newMembers)
	if err != nil {
		return err
	}
	return s.sendControlMessage(ctx, newMembers)
}

func (s *Stream) members(ctx context.Context) (Members, error) {
	bytes, err := common.QueryKey(ctx, s.rdb, s.memberShipKey())
	if err == redis.Nil {
		return []Member{}, nil
	} else if err != nil {
		return nil, err
	} else {
		var members Members
		err = json.Unmarshal(bytes, &members)
		if err != nil {
			return nil, err
		}
		return members, err
	}
}

func (s *Stream) memberShipKey() string {
	return fmt.Sprintf("__%s:__mbsh:%s", s.Ns, s.Name)
}

func (s *Stream) computeMemberships(members Members) Members {
	allPartitions := make([]Partition, s.Pcount)
	for i := 0; i < s.Pcount; i++ {
		allPartitions[i] = Partition(i)
	}
	partitionLen := int(math.Round(float64(s.Pcount) / float64(members.Len())))
	newMembers := make([]Member, members.Len())
	for i := 0; i < members.Len(); i++ {
		if i == members.Len()-1 {
			partitions := allPartitions[i*partitionLen:]
			newMembers[i] = Member{
				ConsumerId: members[i].ConsumerId,
				JoinedAt:   members[i].JoinedAt,
				Partitions: partitions,
			}
		} else {
			partitions := allPartitions[i*partitionLen : (i+1)*partitionLen]
			newMembers[i] = Member{
				ConsumerId: members[i].ConsumerId,
				JoinedAt:   members[i].JoinedAt,
				Partitions: partitions,
			}
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

func (s *Stream) sendControlMessage(ctx context.Context, changeInfo Members) error {
	val, err := json.Marshal(changeInfo)
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
	return fmt.Sprintf("__%s:__ctrl:%s", s.Ns, s.Name)
}

func (s *Stream) Start() {

}

func (s *Stream) StartControlLoop() {
	log.Printf("Control Loop Strted for stream: %s\n", s.Name)
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
			// We are requesting results from only one stream so getting 0th result by default
			s.mu.Lock()
			for _, m := range members {
				if c, ok := s.consumers[m.ConsumerId]; ok {
					log.Printf("Starting to rebalance consumer %s", m.ConsumerId)
					c.RePartition(ctx, m.Partitions)
				}
			}
			s.mu.Unlock()
			lastMessageId = message.ID
		}
		time.Sleep(controlLoopInterval)
	}
}

func (s *Stream) StartMaintenanceLoop() {
	for {
		ctx := context.Background()
		lock, err := common.AcquireAdminLock(ctx, s.rdb, s.Ns, 100*time.Millisecond)
		if err != nil {
			time.Sleep(maintenanceLoopInterval / 2)
			continue
		}
		members, err := s.members(ctx)
		if err != nil {
			_ = lock.Release(ctx)
			time.Sleep(maintenanceLoopInterval / 2)
			continue
		}
		deadMembers := s.CalculateDeadMembers(ctx, members)
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

func (s *Stream) CalculateDeadMembers(ctx context.Context, members Members) Members {
	keys := make([]string, members.Len())
	for i := 0; i < members.Len(); i++ {
		keys[i] = members[i].heartBeatKey(s.Ns, s.Name)
	}
	res := s.rdb.MGet(ctx, keys...)
	dead := make([]Member, 0)
	for i, hb := range res.Val() {
		if hb == nil {
			dead = append(dead, members[i])
		}
	}
	return dead
}
