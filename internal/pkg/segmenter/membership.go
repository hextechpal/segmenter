package segmenter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter/internal/common"
)

type Membership struct {
	Stream        string
	Pcount        int
	Distributions []Distribution
}

type Distribution struct {
	ConsumerId string
	Partitions []int
}

func MemberShipKey(ns, stream string) string {
	return fmt.Sprintf("__%s:__mbsh:%s", ns, stream)

}

func QueryMembershipL(ctx context.Context, rdb *redis.Client, key string) (*Membership, error) {
	var memberShip Membership
	bytes, err := common.QueryKey(ctx, rdb, key)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(bytes, &memberShip)
	if err != nil {
		return nil, err
	}
	return &memberShip, err
}

func UpdateMembershipL(ctx context.Context, rdb *redis.Client, key string, val *Membership) error {
	mVal, err := json.Marshal(val)
	if err != nil {
		return err
	}
	_, err = common.SetKey(ctx, rdb, key, mVal)
	if err != nil {
		return err
	}
	return nil
}
