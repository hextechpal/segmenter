package utils

import "testing"

func TestHash(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want uint32
	}{
		{
			name: "Test1",
			args: args{s: "hextech"},
			want: 3513945414,
		},
		{
			name: "Test2",
			args: args{s: "1"},
			want: 873244444,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Hash(tt.args.s); got != tt.want {
				t.Errorf("Hash() = %v, want %v", got, tt.want)
			}
		})
	}
}
