package segmenter

import "github.com/go-redis/redis/v8"

// Config : Config to initialize the segmenter
type Config struct {
	// RedisOptions : This is same as proved by golang redis/v8 package
	RedisOptions *redis.Options

	// NameSpace : All the redis in redis for this segmenter instance will be prefixed by __nameSpace
	NameSpace string

	// Debug : boolean flag to enabled debug logs in the segmenter
	Debug bool
}
