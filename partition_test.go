package segmenter

import "testing"

func TestPartitions_contains(t *testing.T) {
	type args struct {
		t partition
	}
	tests := []struct {
		name string
		p    partitions
		args args
		want bool
	}{
		{
			name: "Test1 - partition present",
			p:    []partition{partition(1), partition(2), partition(3)},
			args: args{partition(1)},
			want: true,
		},
		{
			name: "Test2 - partition Absent",
			p:    []partition{partition(1), partition(2), partition(3)},
			args: args{partition(4)},
			want: false,
		},
		{
			name: "Test3 - Empty partitions",
			p:    []partition{},
			args: args{partition(4)},
			want: false,
		},
		{
			name: "Test3 - Nil partitions",
			p:    nil,
			args: args{partition(4)},
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
