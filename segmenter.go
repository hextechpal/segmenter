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

// Segmenter : Struck exposed by the library to register streams and consumer
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

	logger := zerolog.
		New(os.Stderr).
		With().Timestamp().
		Str("ns", space).
		Logger().
		Output(zerolog.ConsoleWriter{Out: os.Stderr})

	return &logger
}

// RegisterConsumer : Registers a consumer with segmenter against a stream and group with given batchSize and processingTime
func (seg *Segmenter) RegisterConsumer(ctx context.Context, name string, group string, batchSize int64, maxProcessingTime time.Duration) (*Consumer, error) {

	if name == "" {
		return nil, ErrorEmptyStreamName
	}

	if group == "" {
		return nil, ErrorEmptyGroupName
	}

	if batchSize < 1 {
		return nil, ErrorInvalidBatchSize
	}
	seg.mu.Lock()
	defer seg.mu.Unlock()
	stream, err := seg.findStream(ctx, name)
	if err != nil || stream == nil {
		// TODO handle the case when stream do not exist
		return nil, ErrorNonExistentStream
	}

	seg.logger.Debug().Msgf("registering new consumer for stream %seg", stream.GetName())
	return stream.registerConsumer(ctx, group, batchSize, maxProcessingTime)
}

// RegisterStream : Registers a stream with segmenter with given partition count and partition size
func (seg *Segmenter) RegisterStream(ctx context.Context, name string, pcount int, psize int64) (*Stream, error) {
	if name == "" {
		return nil, ErrorEmptyStreamName
	}

	if pcount < 1 {
		return nil, ErrorInvalidPartitionCount
	}

	if psize < 1 {
		return nil, ErrorInvalidPartitionSize
	}

	seg.mu.Lock()
	defer seg.mu.Unlock()
	stream, ok := seg.streams[name]
	if ok {
		return stream, nil
	}

	streamDTO, err := seg.fetchStreamDTO(ctx, name)
	if err != nil {
		return nil, err
	}

	// This will happen when we fetched the stream from redis hence initiating it
	if streamDTO != nil {
		stream = newStreamFromDTO(ctx, seg.rdb, streamDTO, seg.store, seg.locker, seg.logger)
		seg.streams[stream.GetName()] = stream
		return stream, nil
	}

	stream = newStream(ctx, &newStreamArgs{
		Rdb:    seg.rdb,
		Ns:     seg.ns,
		Name:   name,
		Pcount: pcount,
		Psize:  psize,
		Logger: seg.logger,
		Store:  seg.store,
		Locker: seg.locker,
	})
	err = seg.saveStream(ctx, name, stream)
	if err != nil {
		return nil, err
	}
	seg.streams[stream.GetName()] = stream
	return stream, nil
}

func (seg *Segmenter) findStream(ctx context.Context, name string) (*Stream, error) {
	stream, ok := seg.streams[name]
	if ok {
		return stream, nil
	}
	streamDTO, err := seg.fetchStreamDTO(ctx, name)
	if err != nil {
		return nil, err
	}

	if streamDTO == nil {
		return nil, nil
	}
	stream = newStreamFromDTO(ctx, seg.rdb, streamDTO, seg.store, seg.locker, seg.logger)
	seg.streams[name] = stream
	return stream, nil
}

func (seg *Segmenter) fetchStreamDTO(ctx context.Context, name string) (*streamDTO, error) {
	var st streamDTO
	err := seg.store.GetKey(ctx, seg.streamStorageKey(name), &st)
	if err == redis.Nil {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	return &st, nil
}

func (seg *Segmenter) saveStream(ctx context.Context, name string, stream *Stream) error {
	lock, err := seg.locker.Acquire(ctx, seg.streamAdmin(seg.ns, name), 100*time.Millisecond, "")
	if err != nil {
		return err
	}
	defer lock.Release(ctx)
	streamDTO := newStreamDTO(stream)

	return seg.store.SetKey(ctx, seg.streamStorageKey(name), streamDTO)
}

func (seg *Segmenter) streamStorageKey(name string) string {
	return fmt.Sprintf("__%seg:%seg:info", seg.ns, name)
}

func (seg *Segmenter) streamAdmin(ns string, stream string) string {
	return fmt.Sprintf("__%seg:%seg:admin", ns, stream)
}
