package core

import (
	"reflect"
	"testing"
	"time"
)

func TestMembers_Add(t *testing.T) {
	type args struct {
		member member
	}
	tests := []struct {
		name string
		ms   members
		args args
		want members
	}{
		{
			name: "Test1",
			ms:   []member{},
			args: args{
				member: member{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			want: []member{
				{
					ID:         "cid1",
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
		ms   members
		args args
		want bool
	}{
		{
			name: "Test1 - member exist",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{memberId: "cid1"},
			want: true,
		},
		{
			name: "Test2 - member do not exist",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{memberId: "cid2"},
			want: false,
		},
		{
			name: "Test3 - Empty members",
			ms:   []member{},
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
		ms   members
		args args
		want members
	}{
		{
			name: "Test1 - member exist",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{group: "group1"},
			want: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
		},
		{
			name: "Test2 - member do not exist",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
			args: args{group: "group2"},
			want: []member{},
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
		members members
	}
	tests := []struct {
		name string
		ms   members
		args args
		want members
	}{
		{
			name: "Test1",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ID:         "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
			args: args{
				members: []member{
					{
						ID:         "cid1",
						JoinedAt:   time.Now().UnixMilli(),
						Partitions: []Partition{Partition(1), Partition(2)},
						Group:      "group1",
					},
					{
						ID:         "cid2",
						JoinedAt:   time.Now().UnixMilli(),
						Partitions: []Partition{Partition(3), Partition(4)},
						Group:      "group1",
					},
				},
			},
			want: []member{},
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
		ms   members
		args args
		want members
	}{
		{
			name: "Test1",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ID:         "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
			args: args{mid: "cid2"},
			want: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
			},
		},
		{
			name: "Test2 - Empty members",
			ms:   []member{},
			args: args{mid: "cid2"},
			want: []member{},
		},
		{
			name: "Test3 - member do no exist",
			ms: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ID:         "cid2",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(3), Partition(4)},
					Group:      "group1",
				},
			},
			args: args{mid: "cid3"},
			want: []member{
				{
					ID:         "cid1",
					JoinedAt:   time.Now().UnixMilli(),
					Partitions: []Partition{Partition(1), Partition(2)},
					Group:      "group1",
				},
				{
					ID:         "cid2",
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
