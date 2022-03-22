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

var EmptyStreamName = errors.New("stream name cannot be empty")
var EmptyGroupName = errors.New("group cannot be empty")
var InvalidBatchSize = errors.New("batch size cannot less than 1")
var InvalidPartitionCount = errors.New("partition count cannot less than 1")
var InvalidPartitionSize = errors.New("partition size cannot less than 1")

func NewSegmenter(c *Config) (*Segmenter, error) {
	rdb := redis.NewClient(c.RedisOptions)
	err := rdb.Ping(context.TODO()).Err()
	if err != nil {
		return nil, err
	}

	s := &Segmenter{rdb: rdb, ns: c.NameSpace, streams: make(map[string]*Stream)}
	return s, nil
}

func (s *Segmenter) RegisterConsumer(ctx context.Context, name string, group string, batchSize int64, maxProcessingTime time.Duration) (*Consumer, error) {

	if name == "" {
		return nil, EmptyStreamName
	}

	if group == "" {
		return nil, EmptyGroupName
	}

	if batchSize < 1 {
		return nil, InvalidBatchSize
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stream, err := s.findStream(ctx, name)
	if err != nil || stream == nil {
		// TODO handle the case when stream do not exist
		return nil, errors.New("no Stream exist")
	}
	return stream.registerConsumer(ctx, group, batchSize, maxProcessingTime)
}

func (s *Segmenter) RegisterStream(ctx context.Context, name string, pcount int, psize int64) (*Stream, error) {
	if name == "" {
		return nil, EmptyStreamName
	}

	if pcount < 1 {
		return nil, InvalidPartitionCount
	}

	if psize < 1 {
		return nil, InvalidPartitionSize
	}

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
		stream = newStreamFromDTO(s.rdb, streamDTO)
		s.streams[stream.name] = stream
		return stream, nil
	}

	stream = newStream(s.rdb, s.ns, name, pcount, psize)
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
	stream = newStreamFromDTO(s.rdb, streamDTO)
	s.streams[name] = stream
	return stream, nil
}

func (s *Segmenter) fetchStreamDTO(ctx context.Context, name string) (*streamDTO, error) {
	res, err := s.rdb.Get(ctx, s.streamStorageKey(name)).Bytes()
	if err == redis.Nil {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	var st streamDTO
	err = json.Unmarshal(res, &st)
	if err != nil {
		return nil, err
	}
	return &st, nil
}

func (s *Segmenter) saveStream(ctx context.Context, name string, stream *Stream) error {
	lock, err := acquireAdminLock(ctx, s.rdb, s.ns, "", 100*time.Millisecond)
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	streamDTO := newStreamDTO(stream)
	val, err := json.Marshal(streamDTO)
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, s.streamStorageKey(name), val, 0).Err()
}

func (s *Segmenter) streamStorageKey(name string) string {
	return fmt.Sprintf("__%s:__strm:%s", s.ns, name)
}
