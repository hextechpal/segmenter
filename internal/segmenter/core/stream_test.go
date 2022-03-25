package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redismock/v8"
	"github.com/hextechpal/segmenter/internal/segmenter/locker"
	"github.com/hextechpal/segmenter/internal/segmenter/store"
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
		store:  store.NewRedisStore(rdb),
		locker: locker.NewRedisLocker(rdb),
		ns:     ns,
		name:   name,
		pcount: pc,
		psize:  100,
		logger: &logger,
	}
}

func setupMembers(t *testing.T, pc int, ns, sName string, mCount int) members {
	t.Helper()
	allPartitions := make([]Partition, pc)
	allMembers := make([]member, mCount)

	for i := 0; i < pc; i++ {
		allPartitions[i] = Partition(i)
	}
	ppm := pc / mCount
	for i := 0; i < mCount; i++ {
		var p Partitions
		if i == mCount-1 {
			p = allPartitions[i*ppm:]
		} else {
			p = allPartitions[i*ppm : (i+1)*ppm]
		}
		m := member{
			ID:         fmt.Sprintf("consume%d", i),
			JoinedAt:   time.Now().UnixMilli(),
			Partitions: p,
			Group:      "group1",
		}
		allMembers[i] = m

	}
	return allMembers
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
			name: "2 members - 4 Partitions",
			pc:   4,
			args: args{
				members: []member{
					{
						ID:       "consumer1",
						JoinedAt: time.Now().UnixMilli(),
						Group:    "group1",
					},
					{
						ID:       "consumer2",
						JoinedAt: time.Now().UnixMilli() + 100,
						Group:    "group1",
					},
				},
			},
			want: []member{
				{
					ID:         "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(0), Partition(1)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(2), Partition(3)},
					Group:      "group1",
				},
			},
		},
		{
			name: "2 members - 3 Partitions",
			pc:   3,
			args: args{
				members: []member{
					{
						ID:       "consumer1",
						JoinedAt: time.Now().UnixMilli(),
						Group:    "group1",
					},
					{
						ID:       "consumer2",
						JoinedAt: time.Now().UnixMilli() + 100,
						Group:    "group1",
					},
				},
			},
			want: []member{
				{
					ID:         "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(0), Partition(1)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(2)},
					Group:      "group1",
				},
			},
		},
		{
			name: "2 members - 1 Partitions",
			pc:   1,
			args: args{
				members: []member{
					{
						ID:       "consumer1",
						JoinedAt: time.Now().UnixMilli(),
						Group:    "group1",
					},
					{
						ID:       "consumer2",
						JoinedAt: time.Now().UnixMilli() + 100,
						Group:    "group1",
					},
				},
			},
			want: []member{
				{
					ID:         "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(0)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{},
					Group:      "group1",
				},
			},
		},
		{
			name: "3 members - 10 Partitions",
			pc:   10,
			args: args{
				members: []member{
					{
						ID:       "consumer1",
						JoinedAt: time.Now().UnixMilli(),
						Group:    "group1",
					},
					{
						ID:       "consumer2",
						JoinedAt: time.Now().UnixMilli() + 100,
						Group:    "group1",
					},
					{
						ID:       "consumer3",
						JoinedAt: time.Now().UnixMilli() + 100,
						Group:    "group1",
					},
				},
			},
			want: []member{
				{
					ID:         "consumer1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(0), Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4), Partition(5)},
					Group:      "group1",
				},
				{
					ID:         "consumer3",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(6), Partition(7), Partition(8), Partition(9)},
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
					t.Errorf("Consumer %s, contains() = %v, want %v", got[i].ID, got[i].Partitions, tt.want[i].Partitions)
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

func TestStream_calculateDeadMembers(t *testing.T) {
	ns := "sgNs"
	sname := "sgStreamName"
	pc := 7
	mCount := 3
	allMembers := setupMembers(t, pc, ns, sname, mCount)

	type args struct {
		ctx     context.Context
		members members
	}
	tests := []struct {
		name  string
		alive []int
		args  args
		want  members
	}{
		{
			name:  "All members Alive",
			alive: []int{0, 1, 2},
			args: args{
				ctx:     context.Background(),
				members: allMembers,
			},
			want: []member{},
		},
		{
			name:  "One Member dead",
			alive: []int{0, 1},
			args: args{
				ctx:     context.Background(),
				members: allMembers,
			},
			want: allMembers[2:],
		},
		{
			name:  "All members dead",
			alive: []int{},
			args: args{
				ctx:     context.Background(),
				members: allMembers,
			},
			want: allMembers,
		},
	}
	rdb, mock := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createStream(t, rdb, pc, ns, sname)
			hbs := make([]string, mCount)
			alive := make([]interface{}, mCount)
			for i, m := range allMembers {
				hbs[i] = fmt.Sprintf("__%s:__%s:__beat:%s", ns, sname, m.ID)
			}
			for i, mc := range tt.alive {
				alive[i] = allMembers[mc]
			}
			mock.ExpectMGet(hbs...).SetVal(alive)
			if got := s.calculateDeadMembers(tt.args.ctx, tt.args.members); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("calculateDeadMembers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStream_allMembers(t *testing.T) {
	ns := "sgNs"
	sname := "sgStreamName"
	pc := 7
	mCount := 3
	allMembers := setupMembers(t, pc, ns, sname, mCount)
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		mockErr error
		mockVal members
		args    args
		want    members
		wantErr bool
	}{
		{
			name: "Nil key",
			args: args{
				ctx: context.Background(),
			},
			mockErr: redis.Nil,
			want:    []member{},
			wantErr: false,
		},
		{
			name: "Correct Key",
			args: args{
				ctx: context.Background(),
			},
			mockVal: allMembers,
			want:    allMembers,
			wantErr: false,
		},
		{
			name: "Unknown Error",
			args: args{
				ctx: context.Background(),
			},
			mockErr: errors.New("unknown error"),
			mockVal: allMembers,
			want:    nil,
			wantErr: true,
		},
	}
	rdb, mock := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createStream(t, rdb, pc, ns, sname)
			if tt.mockErr != nil {
				mock.ExpectGet(s.memberShipKey()).SetErr(tt.mockErr)
			} else {
				val, err := json.Marshal(tt.mockVal)
				if err != nil {
					t.Fatalf("Error serializing members")
					return
				}
				mock.ExpectGet(s.memberShipKey()).SetVal(string(val))
			}
			got, err := s.allMembers(tt.args.ctx)
			if (err != nil) != tt.wantErr {
				t.Errorf("allMembers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("allMembers() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStream_members(t *testing.T) {
	ns := "sgNs"
	sname := "sgStreamName"
	pc := 7
	mCount := 3
	allMembers := setupMembers(t, pc, ns, sname, mCount)
	type args struct {
		ctx   context.Context
		group string
	}
	tests := []struct {
		name    string
		mockErr error
		mockVal members
		args    args
		want    members
		wantErr bool
	}{
		{
			name: "Nil key",
			args: args{
				ctx: context.Background(),
			},
			mockErr: redis.Nil,
			want:    []member{},
			wantErr: false,
		},
		{
			name: "Correct Group",
			args: args{
				ctx:   context.Background(),
				group: "group1",
			},
			mockVal: allMembers,
			want:    allMembers,
			wantErr: false,
		},
		{
			name: "InCorrect Group",
			args: args{
				ctx:   context.Background(),
				group: "group2",
			},
			mockVal: allMembers,
			want:    []member{},
			wantErr: false,
		},
		{
			name: "Unknown Error",
			args: args{
				ctx:   context.Background(),
				group: "group1",
			},
			mockErr: errors.New("unknown error"),
			mockVal: allMembers,
			want:    nil,
			wantErr: true,
		},
	}
	rdb, mock := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createStream(t, rdb, pc, ns, sname)
			if tt.mockErr != nil {
				mock.ExpectGet(s.memberShipKey()).SetErr(tt.mockErr)
			} else {
				val, err := json.Marshal(tt.mockVal)
				if err != nil {
					t.Fatalf("Error serializing members")
					return
				}
				mock.ExpectGet(s.memberShipKey()).SetVal(string(val))
			}
			got, err := s.members(tt.args.ctx, tt.args.group)
			if (err != nil) != tt.wantErr {
				t.Errorf("allMembers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("allMembers() got = %v, want %v", got, tt.want)
			}
		})
	}
}
