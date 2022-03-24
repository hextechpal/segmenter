package segmenter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"time"
)

var EmptyStreamName = errors.New("stream name cannot be empty")
var EmptyGroupName = errors.New("group cannot be empty")
var InvalidBatchSize = errors.New("batch size cannot less than 1")
var InvalidPartitionCount = errors.New("partition count cannot less than 1")
var InvalidPartitionSize = errors.New("partition size cannot less than 1")

type Segmenter struct {
	mu      sync.Mutex
	ns      string
	rdb     *redis.Client
	streams map[string]*Stream
	logger  *zerolog.Logger
}

func NewSegmenter(c *Config) (*Segmenter, error) {
	rdb := redis.NewClient(c.RedisOptions)
	err := rdb.Ping(context.TODO()).Err()
	if err != nil {
		return nil, err
	}
	logger := setupLogger(c.Debug, c.NameSpace)
	s := &Segmenter{rdb: rdb, ns: c.NameSpace, streams: make(map[string]*Stream), logger: logger}
	return s, nil
}

func setupLogger(debug bool, space string) *zerolog.Logger {
	logLevel := zerolog.InfoLevel
	if debug {
		logLevel = zerolog.DebugLevel
	}

	zerolog.SetGlobalLevel(logLevel)
	logger := zerolog.New(os.Stderr).With().Timestamp().Str("ns", space).Logger().Output(zerolog.ConsoleWriter{Out: os.Stderr})
	return &logger
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
		return nil, errors.New("no stream exist")
	}
	s.logger.Debug().Msgf("registering new consumer for stream %s", stream.name)
	return NewConsumer(ctx, stream, batchSize, group, maxProcessingTime, s.logger)
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
		stream = newStreamFromDTO(s.rdb, streamDTO, s.logger)
		s.streams[stream.name] = stream
		return stream, nil
	}

	stream = newStream(s.rdb, s.ns, name, pcount, psize, s.logger)
	err = s.saveStream(ctx, name, stream)
	if err != nil {
		return nil, err
	}
	s.logger.Info()
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
	stream = newStreamFromDTO(s.rdb, streamDTO, s.logger)
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
	lock, err := acquireAdminLock(ctx, s.rdb, s.ns, name, 100*time.Millisecond)
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
