package segmenter

import (
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/rs/zerolog"
	"os"
	"reflect"
	"testing"
	"time"
)

func createStream(t *testing.T, rdb *redis.Client, pc int, ns string, name string) *Stream {
	t.Helper()
	logger := zerolog.New(os.Stderr).With().Logger()
	return &Stream{
		rdb:    rdb,
		ns:     ns,
		name:   name,
		pcount: pc,
		psize:  100,
		logger: &logger,
	}
}

func TestStream_computeMemberships(t *testing.T) {
	type args struct {
		members members
	}
	tests := []struct {
		name string
		pc   int
		args args
		want members
	}{
		{
			name: "2 members - 4 partitions",
			pc:   4,
			args: args{
				members: []member{
					{
						ConsumerId: "consumer1",
						JoinedAt:   time.Now().UnixMilli(),
						Group:      "group1",
					},
					{
						ConsumerId: "consumer2",
						JoinedAt:   time.Now().UnixMilli() + 100,
						Group:      "group1",
					},
				},
			},
			want: []member{
				{
					ConsumerId: "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(0), partition(1)},
					Group:      "group1",
				},
				{
					ConsumerId: "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(2), partition(3)},
					Group:      "group1",
				},
			},
		},
		{
			name: "2 members - 3 partitions",
			pc:   3,
			args: args{
				members: []member{
					{
						ConsumerId: "consumer1",
						JoinedAt:   time.Now().UnixMilli(),
						Group:      "group1",
					},
					{
						ConsumerId: "consumer2",
						JoinedAt:   time.Now().UnixMilli() + 100,
						Group:      "group1",
					},
				},
			},
			want: []member{
				{
					ConsumerId: "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(0), partition(1)},
					Group:      "group1",
				},
				{
					ConsumerId: "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(2)},
					Group:      "group1",
				},
			},
		},
		{
			name: "2 members - 1 partitions",
			pc:   1,
			args: args{
				members: []member{
					{
						ConsumerId: "consumer1",
						JoinedAt:   time.Now().UnixMilli(),
						Group:      "group1",
					},
					{
						ConsumerId: "consumer2",
						JoinedAt:   time.Now().UnixMilli() + 100,
						Group:      "group1",
					},
				},
			},
			want: []member{
				{
					ConsumerId: "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(0)},
					Group:      "group1",
				},
				{
					ConsumerId: "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{},
					Group:      "group1",
				},
			},
		},
		{
			name: "3 members - 10 partitions",
			pc:   10,
			args: args{
				members: []member{
					{
						ConsumerId: "consumer1",
						JoinedAt:   time.Now().UnixMilli(),
						Group:      "group1",
					},
					{
						ConsumerId: "consumer2",
						JoinedAt:   time.Now().UnixMilli() + 100,
						Group:      "group1",
					},
					{
						ConsumerId: "consumer3",
						JoinedAt:   time.Now().UnixMilli() + 100,
						Group:      "group1",
					},
				},
			},
			want: []member{
				{
					ConsumerId: "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(0), partition(1), partition(2)},
					Group:      "group1",
				},
				{
					ConsumerId: "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(3), partition(4), partition(5)},
					Group:      "group1",
				},
				{
					ConsumerId: "consumer3",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(6), partition(7), partition(8), partition(9)},
					Group:      "group1",
				},
			},
		},
	}
	rdb, _ := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createStream(t, rdb, tt.pc, "sgNs", "sgTestStream")
			got := s.computeMemberships(tt.args.members)
			for i := 0; i < len(tt.want); i++ {
				if !reflect.DeepEqual(got[i].Partitions, tt.want[i].Partitions) {
					t.Errorf("Consumer %s, contains() = %v, want %v", got[i].ConsumerId, got[i].Partitions, tt.want[i].Partitions)
				}
			}
		})
	}
}

func TestStream_controlKey(t *testing.T) {
	type fields struct {
		pc   int
		ns   string
		name string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"Test1", fields{1, "ns1", "name1"}, "__ns1:__name1:__ctrl"},
		{"Test2", fields{1, "ns2", "name2"}, "__ns2:__name2:__ctrl"},
	}
	rdb, _ := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createStream(t, rdb, tt.fields.pc, tt.fields.ns, tt.fields.name)
			if got := s.controlKey(); got != tt.want {
				t.Errorf("controlKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
