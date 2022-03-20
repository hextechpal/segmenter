package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/examples"
	"log"
	"math/rand"
	"sync"
	"time"
)

func main() {
	rand.Seed(time.Now().UnixMilli())
	gn := rand.Intn(10)
	ctx := context.TODO()
	c := segmenter.Config{
		RedisOptions: &redis.Options{Addr: "localhost:6379"},
		NameSpace:    examples.Namespace,
	}
	s, err := segmenter.NewSegmenter(&c)
	if err != nil {
		log.Fatalf("Error occurred while initializing segmenter, %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.RegisterConsumer(ctx, examples.StreamName, fmt.Sprintf("grp%d", gn), 10)
			if err != nil {
				log.Printf("Error happened while registering Consumer, %v", err)
			} else {
				log.Printf("Registered Consumer with id %s\n", c.GetID())
			}

		}()
	}
	wg.Wait()
	time.Sleep(3 * time.Minute)
}
