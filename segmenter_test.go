package segmenter

import (
	"context"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/hextechpal/segmenter/internal/segmenter/core"
	"github.com/hextechpal/segmenter/internal/segmenter/locker"
	"github.com/hextechpal/segmenter/internal/segmenter/store"
	"github.com/rs/zerolog"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestSegmenter_RegisterConsumer(t *testing.T) {

	type args struct {
		ctx               context.Context
		name              string
		group             string
		batchSize         int64
		maxProcessingTime time.Duration
	}
	tests := []struct {
		name    string
		args    args
		want    *core.Consumer
		wantErr bool
	}{
		{
			name: "Invalid name",
			args: args{
				ctx:               context.Background(),
				name:              "",
				group:             "group",
				batchSize:         5,
				maxProcessingTime: time.Second,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid group",
			args: args{
				ctx:               context.Background(),
				name:              "stname",
				group:             "",
				batchSize:         5,
				maxProcessingTime: time.Second,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid batch size",
			args: args{
				ctx:               context.Background(),
				name:              "stname",
				group:             "group",
				batchSize:         0,
				maxProcessingTime: time.Second,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Non Existent Stream",
			args: args{
				ctx:               context.Background(),
				name:              "stname",
				group:             "group",
				batchSize:         5,
				maxProcessingTime: time.Second,
			},
			want:    nil,
			wantErr: true,
		},
	}
	rdb, _ := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createSegmenter(t, rdb)
			got, err := s.RegisterConsumer(tt.args.ctx, tt.args.name, tt.args.group, tt.args.batchSize, tt.args.maxProcessingTime)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RegisterConsumer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSegmenter_RegisterStream(t *testing.T) {

	type args struct {
		ctx    context.Context
		name   string
		pcount int
		psize  int64
	}
	tests := []struct {
		name    string
		args    args
		want    *core.Stream
		wantErr bool
	}{
		{
			name: "Invalid name",
			args: args{
				ctx:    context.Background(),
				name:   "",
				pcount: 5,
				psize:  100,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid partition Count",
			args: args{
				ctx:    context.Background(),
				name:   "sgroot",
				pcount: 0,
				psize:  100,
			},
			want:    nil,
			wantErr: true,
		},
		{
			name: "Invalid partition size",
			args: args{
				ctx:    context.Background(),
				name:   "sgroot",
				pcount: 5,
				psize:  0,
			},
			want:    nil,
			wantErr: true,
		},
	}
	rdb, _ := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createSegmenter(t, rdb)
			got, err := s.RegisterStream(tt.args.ctx, tt.args.name, tt.args.pcount, tt.args.psize)
			if (err != nil) != tt.wantErr {
				t.Errorf("RegisterConsumer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RegisterConsumer() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func createSegmenter(t *testing.T, rdb *redis.Client) *Segmenter {
	t.Helper()
	l := zerolog.New(os.Stderr).With().Logger()
	return &Segmenter{
		rdb:     rdb,
		streams: make(map[string]*core.Stream),
		logger:  &l,
		store:   store.NewRedisStore(rdb),
		locker:  locker.NewRedisLocker(rdb),
		ns:      "sgroot",
	}
}
