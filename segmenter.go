package segmenter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"sync"
	"time"
)

type Segmenter struct {
	mu      sync.Mutex
	ns      string
	rdb     *redis.Client
	streams map[string]*Stream
}

func NewSegmenter(c *Config) (*Segmenter, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     c.Address,
		Username: c.Username,
		Password: c.Password,
	})
	status := rdb.Ping(context.TODO())
	if status.Err() != nil {
		return nil, status.Err()
	}
	s := &Segmenter{rdb: rdb, ns: c.Namespace, streams: make(map[string]*Stream)}
	return s, nil
}

func (s *Segmenter) RegisterConsumer(ctx context.Context, name string, batchSize int) (*Consumer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, err := s.findStream(ctx, name)
	if err != nil || stream == nil {
		// TODO handle the case when stream do not exist
		return nil, errors.New("no Stream exist")
	}
	return stream.registerConsumer(ctx, batchSize)
}

func (s *Segmenter) RegisterStream(ctx context.Context, name string, pcount int, psize int64) (*Stream, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, ok := s.streams[name]
	if ok {
		return stream, nil
	}

	streamDTO, err := s.fetchStreamDTO(ctx, name)
	if err != nil {
		return nil, err
	}

	// This will happen when we fetched the stream from redis hence initiating it
	if streamDTO != nil {
		stream = NewStreamFromDTO(s.rdb, streamDTO)
		s.streams[stream.Name] = stream
		return stream, nil
	}

	stream = NewStream(s.rdb, s.ns, name, pcount, psize)
	err = s.saveStream(ctx, name, stream)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (s *Segmenter) findStream(ctx context.Context, name string) (*Stream, error) {
	stream, ok := s.streams[name]
	if ok {
		return stream, nil
	}
	streamDTO, err := s.fetchStreamDTO(ctx, name)
	if err != nil {
		return nil, err
	}

	if streamDTO == nil {
		return nil, nil
	}
	stream = NewStreamFromDTO(s.rdb, streamDTO)
	s.streams[name] = stream
	return stream, nil
}

func (s *Segmenter) fetchStreamDTO(ctx context.Context, name string) (*StreamDTO, error) {
	res, err := s.rdb.Get(ctx, s.streamKey(name)).Bytes()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var st StreamDTO
	err = json.Unmarshal(res, &st)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Segmenter) saveStream(ctx context.Context, name string, stream *Stream) error {
	lock, err := AcquireAdminLock(ctx, s.rdb, s.ns, 100*time.Millisecond)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	val, err := json.Marshal(stream)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.streamKey(name), val, 0).Err()
}

func (s *Segmenter) streamKey(name string) string {
	return fmt.Sprintf("__%s:__strm:%s", s.ns, name)
}

//func (s *Segmenter) GroupedStreamMembers(ctx context.Context, keys []string) map[string]Members {
//	res := s.rdb.MGet(ctx, keys...).Val()
//	groupedMembers := make(map[string]Members)
//	for i := 0; i < len(res); i++ {
//		var m Member
//		_ = json.Unmarshal([]byte(res[i].(string)), &m)
//		if _, ok := groupedMembers[m.Stream]; ok {
//			groupedMembers[m.Stream] = append(groupedMembers[m.Stream], m)
//		} else {
//			groupedMembers[m.Stream] = []Member{m}
//		}
//	}
//	return groupedMembers
//}
