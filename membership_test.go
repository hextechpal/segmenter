package segmenter

import (
	"reflect"
	"testing"
	"time"
)

func TestMembers_Add(t *testing.T) {
	type args struct {
		member Member
	}
	tests := []struct {
		name string
		ms   Members
		args args
		want Members
	}{
		{
			name: "Test1",
			ms:   []Member{},
			args: args{
				member: Member{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			want: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ms.Add(tt.args.member); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMembers_Contains(t *testing.T) {
	type args struct {
		memberId string
	}
	tests := []struct {
		name string
		ms   Members
		args args
		want bool
	}{
		{
			name: "Test1 - Member exist",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{memberId: "cid1"},
			want: true,
		},
		{
			name: "Test2 - Member do not exist",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{memberId: "cid2"},
			want: false,
		},
		{
			name: "Test3 - Empty Members",
			ms:   []Member{},
			args: args{memberId: "cid2"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ms.Contains(tt.args.memberId); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMembers_FilterBy(t *testing.T) {
	type args struct {
		group string
	}
	tests := []struct {
		name string
		ms   Members
		args args
		want Members
	}{
		{
			name: "Test1 - Member exist",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{group: "group1"},
			want: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
		},
		{
			name: "Test2 - Member do not exist",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{group: "group2"},
			want: []Member{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ms.FilterBy(tt.args.group); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("FilterBy() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMembers_RemoveAll(t *testing.T) {
	type args struct {
		members Members
	}
	tests := []struct {
		name string
		ms   Members
		args args
		want Members
	}{
		{
			name: "Test1",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ConsumerId: "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
			args: args{
				members: []Member{
					{
						ConsumerId: "cid1",
						JoinedAt:   time.Now().UnixMilli(),
						Partitions: []Partition{Partition(1), Partition(2)},
						Group:      "group1",
					},
					{
						ConsumerId: "cid2",
						JoinedAt:   time.Now().UnixMilli(),
						Partitions: []Partition{Partition(3), Partition(4)},
						Group:      "group1",
					},
				},
			},
			want: []Member{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ms.RemoveAll(tt.args.members); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RemoveAll() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMembers_Remove(t *testing.T) {
	type args struct {
		mid string
	}
	tests := []struct {
		name string
		ms   Members
		args args
		want Members
	}{
		{
			name: "Test1",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ConsumerId: "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
			args: args{mid: "cid2"},
			want: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
		},
		{
			name: "Test2 - Empty Members",
			ms:   []Member{},
			args: args{mid: "cid2"},
			want: []Member{},
		},
		{
			name: "Test3 - Member do no exist",
			ms: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ConsumerId: "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
			args: args{mid: "cid3"},
			want: []Member{
				{
					ConsumerId: "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ConsumerId: "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ms.Remove(tt.args.mid); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Remove() = %v, want %v", got, tt.want)
			}
		})
	}
}
