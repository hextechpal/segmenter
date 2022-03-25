package core

import "testing"

func TestPartitions_contains(t *testing.T) {
	type args struct {
		t Partition
	}
	tests := []struct {
		name string
		p    Partitions
		args args
		want bool
	}{
		{
			name: "Test1 - Partition present",
			p:    []Partition{Partition(1), Partition(2), Partition(3)},
			args: args{Partition(1)},
			want: true,
		},
		{
			name: "Test2 - Partition Absent",
			p:    []Partition{Partition(1), Partition(2), Partition(3)},
			args: args{Partition(4)},
			want: false,
		},
		{
			name: "Test3 - Empty Partitions",
			p:    []Partition{},
			args: args{Partition(4)},
			want: false,
		},
		{
			name: "Test3 - Nil Partitions",
			p:    nil,
			args: args{Partition(4)},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.Contains(tt.args.t); got != tt.want {
				t.Errorf("Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}
