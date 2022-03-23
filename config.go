package segmenter

import "github.com/go-redis/redis/v8"

type Config struct {
	RedisOptions *redis.Options
	NameSpace    string

	Debug bool
}
