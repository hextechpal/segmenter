package segmenter

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

const psize = 100

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
		psize:  psize,
		logger: &logger,
	}
}

func setupMembers(t *testing.T, pc int, ns, sName, group string, mCount int) members {
	t.Helper()
	allPartitions := make([]partition, pc)
	allMembers := make([]member, mCount)

	for i := 0; i < pc; i++ {
		allPartitions[i] = partition(i)
	}
	ppm := pc / mCount
	for i := 0; i < mCount; i++ {
		var p partitions
		if i == mCount-1 {
			p = allPartitions[i*ppm:]
		} else {
			p = allPartitions[i*ppm : (i+1)*ppm]
		}
		m := member{
			ID:         fmt.Sprintf("consume%d", i),
			JoinedAt:   time.Now().UnixMilli(),
			Partitions: p,
			Group:      group,
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
			name: "2 members - 4 partitions",
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
					Partitions: []partition{partition(0), partition(1)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
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
					Partitions: []partition{partition(0), partition(1)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
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
					Partitions: []partition{partition(0)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
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
					Partitions: []partition{partition(0), partition(1), partition(2)},
					Group:      "group1",
				},
				{
					ID:         "consumer2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []partition{partition(3), partition(4), partition(5)},
					Group:      "group1",
				},
				{
					ID:         "consumer3",
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
		{"Test1", fields{1, "ns1", "name1"}, "__ns1:name1:ctrl"},
		{"Test2", fields{1, "ns2", "name2"}, "__ns2:name2:ctrl"},
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
	group := "group"
	allMembers := setupMembers(t, pc, ns, sname, group, mCount)

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
			name:  "One member dead",
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
				hbs[i] = fmt.Sprintf("__%s:%s:%s:beat:%s", ns, sname, group, m.ID)
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

func TestStream_members(t *testing.T) {
	ns := "sgNs"
	sname := "sgStreamName"
	pc := 7
	mCount := 3
	allMembers := setupMembers(t, pc, ns, sname, "", mCount)
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
				mock.ExpectGet(s.memberShipGroupKey(tt.args.group)).SetErr(tt.mockErr)
			} else {
				val, err := json.Marshal(tt.mockVal)
				if err != nil {
					t.Fatalf("Error serializing members")
					return
				}
				mock.ExpectGet(s.memberShipGroupKey(tt.args.group)).SetVal(string(val))
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

func TestStream_GetPartitionSize(t *testing.T) {
	rdb, _ := redismock.NewClientMock()
	st := createStream(t, rdb, 10, "ns", "stream")

	if st.GetPartitionSize() != psize {
		t.Fatalf("GetPartitionSize(), got = %v, want : %v", st.GetPartitionSize(), psize)
	}
}

func TestStream_GetPartitionCount(t *testing.T) {
	rdb, _ := redismock.NewClientMock()
	st := createStream(t, rdb, 10, "ns", "stream")

	if st.GetPartitionCount() != 10 {
		t.Fatalf("GetPartitionSize(), got = %v, want : %v", st.GetPartitionCount(), 10)
	}
}

func TestStream_GetName(t *testing.T) {
	rdb, _ := redismock.NewClientMock()
	st := createStream(t, rdb, 10, "ns", "stream")

	if st.GetName() != "stream" {
		t.Fatalf("GetPartitionSize(), got = %v, want : %v", st.GetName(), "stream")
	}
}

func TestStream_computeMembers(t *testing.T) {
	jt := time.Now().UnixMilli()
	type args struct {
		ctx        context.Context
		changeInfo *memberChangeInfo
	}
	tests := []struct {
		name            string
		originalMembers members
		args            args
		mockErr         error
		want            members
		want1           error
		want2           bool
	}{
		{
			name:            "member Added",
			originalMembers: []member{},
			args: args{
				ctx: context.Background(),
				changeInfo: &memberChangeInfo{
					Reason:     join,
					ConsumerId: "consumer1",
					Group:      "grp1",
					Ts:         jt,
				},
			},
			want: []member{
				{
					ID:       "consumer1",
					JoinedAt: jt,
					Group:    "grp1",
				},
			},
		},
		{
			name: "member Added - Already present",
			originalMembers: []member{
				{
					ID:       "consumer1",
					JoinedAt: jt,
					Group:    "grp1",
				},
			},
			args: args{
				ctx: context.Background(),
				changeInfo: &memberChangeInfo{
					Reason:     join,
					ConsumerId: "consumer1",
					Group:      "grp1",
					Ts:         jt,
				},
			},
			want:  nil,
			want1: nil,
			want2: true,
		},
		{
			name: "member leave",
			originalMembers: []member{
				{
					ID:       "consumer1",
					JoinedAt: jt,
					Group:    "grp1",
				},
			},
			args: args{
				ctx: context.Background(),
				changeInfo: &memberChangeInfo{
					Reason:     leave,
					ConsumerId: "consumer1",
					Group:      "grp1",
					Ts:         jt,
				},
			},
			want:  []member{},
			want1: nil,
			want2: false,
		},
		{
			name: "member Absent",
			originalMembers: []member{
				{
					ID:       "consumer2",
					JoinedAt: jt,
					Group:    "grp1",
				},
			},
			args: args{
				ctx: context.Background(),
				changeInfo: &memberChangeInfo{
					Reason:     leave,
					ConsumerId: "consumer1",
					Group:      "grp1",
					Ts:         jt,
				},
			},
			want:  nil,
			want1: nil,
			want2: true,
		},
		{
			name:    "members with error",
			mockErr: errors.New("members query error"),
			originalMembers: []member{
				{
					ID:       "consumer2",
					JoinedAt: jt,
					Group:    "grp1",
				},
			},
			args: args{
				ctx: context.Background(),
				changeInfo: &memberChangeInfo{
					Reason:     leave,
					ConsumerId: "consumer1",
					Group:      "grp1",
					Ts:         jt,
				},
			},
			want:  nil,
			want1: errors.New("members query error"),
			want2: true,
		},
	}
	rdb, mock := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := createStream(t, rdb, 5, "sgNs", "sgSteam")
			data, _ := json.Marshal(tt.originalMembers)
			if tt.mockErr != nil {
				mock.ExpectGet(s.memberShipGroupKey(tt.args.changeInfo.Group)).SetErr(tt.mockErr)
			} else {
				mock.ExpectGet(s.memberShipGroupKey(tt.args.changeInfo.Group)).SetVal(string(data))

			}
			got, got1, got2 := s.computeMembers(tt.args.ctx, tt.args.changeInfo)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("computeMembers() got = %v, want %v", got, tt.want)
			}

			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("computeMembers() got1 = %v, want %v", got1, tt.want1)
			}

			if got2 != tt.want2 {
				t.Errorf("computeMembers() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func TestNewStreamFromDTO(t *testing.T) {
	rdb, _ := redismock.NewClientMock()
	logger := zerolog.New(os.Stderr).With().Logger()
	store := store.NewRedisStore(rdb)
	locker := locker.NewRedisLocker(rdb)
	type args struct {
		ctx context.Context
		dto *streamDTO
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "Test Stream from DTO",
			args: args{
				ctx: context.Background(),
				dto: &streamDTO{
					Ns:     "ns",
					Name:   "test1",
					Pcount: 10,
					Psize:  100,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newStreamFromDTO(tt.args.ctx, rdb, tt.args.dto, store, locker, &logger)
			if got.GetName() != tt.args.dto.Name {
				t.Errorf("newStreamFromDTO() name = %v, want %v", got.GetName(), tt.args.dto.Name)
			}

			if got.GetPartitionCount() != tt.args.dto.Pcount {
				t.Errorf("newStreamFromDTO() pcount = %v, want %v", got.GetPartitionCount(), tt.args.dto.Pcount)
			}

			if got.GetPartitionSize() != tt.args.dto.Psize {
				t.Errorf("newStreamFromDTO() psize = %v, want %v", got.GetPartitionSize(), tt.args.dto.Psize)
			}
		})
	}
}
