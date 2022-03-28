package segmenter

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/segmenter/locker"
	"github.com/hextechpal/segmenter/internal/segmenter/store"
	"github.com/rs/zerolog"
	"os"
	"sync"
	"time"
)

type Segmenter struct {
	mu      sync.Mutex
	rdb     *redis.Client
	streams map[string]*Stream
	logger  *zerolog.Logger
	store   store.Store
	locker  locker.Locker

	ns string
}

func NewSegmenter(c *Config) (*Segmenter, error) {
	rdb := redis.NewClient(c.RedisOptions)
	err := rdb.Ping(context.TODO()).Err()
	if err != nil {
		return nil, err
	}
	s := &Segmenter{
		rdb:     rdb,
		streams: make(map[string]*Stream),
		logger:  setupLogger(c.Debug, c.NameSpace),
		store:   store.NewRedisStore(rdb),
		locker:  locker.NewRedisLocker(rdb),
		ns:      c.NameSpace,
	}
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
		return nil, NonExistentStream
	}

	s.logger.Debug().Msgf("registering new consumer for stream %s", stream.GetName())
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
		stream = newStreamFromDTO(ctx, s.rdb, streamDTO, s.store, s.locker, s.logger)
		s.streams[stream.GetName()] = stream
		return stream, nil
	}

	stream = newStream(ctx, &newStreamArgs{
		Rdb:    s.rdb,
		Ns:     s.ns,
		Name:   name,
		Pcount: pcount,
		Psize:  psize,
		Logger: s.logger,
		Store:  s.store,
		Locker: s.locker,
	})
	err = s.saveStream(ctx, name, stream)
	if err != nil {
		return nil, err
	}
	s.streams[stream.GetName()] = stream
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
	stream = newStreamFromDTO(ctx, s.rdb, streamDTO, s.store, s.locker, s.logger)
	s.streams[name] = stream
	return stream, nil
}

func (s *Segmenter) fetchStreamDTO(ctx context.Context, name string) (*streamDTO, error) {
	var st streamDTO
	err := s.store.GetKey(ctx, s.streamStorageKey(name), &st)
	if err == redis.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &st, nil
}

func (s *Segmenter) saveStream(ctx context.Context, name string, stream *Stream) error {
	lock, err := s.locker.Acquire(ctx, s.streamAdmin(s.ns, name), 100*time.Millisecond, "")
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	streamDTO := newStreamDTO(stream)

	return s.store.SetKey(ctx, s.streamStorageKey(name), streamDTO)
}

func (s *Segmenter) streamStorageKey(name string) string {
	return fmt.Sprintf("__%s:%s:info", s.ns, name)
}

func (s *Segmenter) streamAdmin(ns string, stream string) string {
	return fmt.Sprintf("__%s:%s:admin", ns, stream)
}
