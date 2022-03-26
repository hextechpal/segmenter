package store

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redismock/v8"
	"reflect"
	"testing"
)

type testStruct struct {
	Msg string
}

func TestInvalidTypeError_Error(t *testing.T) {
	type fields struct {
		tp reflect.Type
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "Type test",
			fields: fields{tp: reflect.TypeOf(testStruct{})},
			want:   "Invalid type \"store.testStruct\", must be a pointer type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &InvalidTypeError{
				tp: tt.fields.tp,
			}
			if got := i.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewRedisStore(t *testing.T) {
	rdb, _ := redismock.NewClientMock()
	rs := &redisStore{rdb: rdb}
	t.Run("TestConstructor", func(t *testing.T) {
		if got := NewRedisStore(rdb); !reflect.DeepEqual(got, rs) {
			t.Errorf("NewRedisStore() = %v, want %v", got, rs)
		}
	})
}

func Test_redisStore_GetKey(t *testing.T) {
	type args struct {
		ctx context.Context
		key string
		v   any
	}
	tests := []struct {
		name    string
		mockErr error
		args    args
		want    any
		wantErr bool
	}{
		{
			name: "Get No Error",
			args: args{
				ctx: context.Background(),
				key: "key",
				v:   &testStruct{},
			},
			want: &testStruct{Msg: "abc"},
		},

		{
			name:    "Get With Error",
			mockErr: errors.New(" get key error"),
			args: args{
				ctx: context.Background(),
				key: "key",
				v:   &testStruct{},
			},
			want:    nil,
			wantErr: true,
		},
	}
	rdb, mock := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &redisStore{rdb: rdb}
			if tt.mockErr != nil {
				mock.ExpectGet(tt.args.key).SetErr(tt.mockErr)
			} else {
				data, _ := json.Marshal(tt.want)
				mock.ExpectGet(tt.args.key).SetVal(string(data))
			}
			err := r.GetKey(tt.args.ctx, tt.args.key, tt.args.v)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetKey() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if err == nil && !reflect.DeepEqual(tt.args.v, tt.want) {
				t.Errorf("GetKey(), got %v, wantErr %v", tt.args.v, tt.want)
			}
		})
	}
}

func Test_redisStore_SetKey(t *testing.T) {

	type args struct {
		ctx context.Context
		key string
		v   any
	}
	tests := []struct {
		name    string
		args    args
		mockErr error
		wantErr bool
	}{
		{
			name: "Set Key - No error",
			args: args{
				ctx: context.Background(),
				key: "key",
				v:   &testStruct{Msg: "abc"},
			},
		},
		{
			name: "Set Key - with error",
			args: args{
				ctx: context.Background(),
				key: "key",
				v:   &testStruct{Msg: "abc"},
			},
			mockErr: errors.New("set key error"),
			wantErr: true,
		},
	}
	rdb, mock := redismock.NewClientMock()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &redisStore{rdb: rdb}
			if tt.mockErr != nil {
				mock.ExpectSet(tt.args.key, tt.args.v, 0).SetErr(tt.mockErr)
			} else {
				data, _ := json.Marshal(tt.args.v)
				mock.ExpectSet(tt.args.key, data, 0).SetVal("OK")
			}
			if err := r.SetKey(tt.args.ctx, tt.args.key, tt.args.v); (err != nil) != tt.wantErr {
				t.Errorf("SetKey() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
