package main

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/api/proto/contracts"
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

	st, err := s.RegisterStream(ctx, examples.StreamName, 20, 2500)
	if err != nil {
		log.Fatalf("Error occurred while registering streamName, %v", err)
	}

	go sendMessages(st, 100, 100*time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(routine int) {
			defer wg.Done()
			c, err := s.RegisterConsumer(ctx, examples.StreamName, fmt.Sprintf("grp%d", gn), 10)
			if err != nil {
				log.Printf("Error happened while registering Consumer, %v", err)
			} else {
				for start := time.Now(); time.Since(start) < 60*time.Second; {
					messages, err := c.GetMessages(ctx, 200*time.Millisecond)
					if err != nil {
						log.Printf("Routine %d errored whole reading messages, %v\n", routine, err)
					} else {
						for _, m := range messages {
							log.Printf("Routine %d got Message message %v\n", routine, m)
						}
					}
					time.Sleep(2 * time.Second)
				}

			}

		}(i)
	}
	wg.Wait()
	time.Sleep(1 * time.Minute)
}

func sendMessages(st *segmenter.Stream, messageCount int, sleep time.Duration) {
	for i := 0; i < messageCount; i++ {
		uuid := segmenter.GenerateUuid()
		id, err := st.Send(context.TODO(), &contracts.PMessage{
			Data:         []byte(fmt.Sprintf("Message with uuid : %s", uuid)),
			PartitionKey: uuid,
		})
		if err != nil {
			log.Printf("Error happened while sending message, %v\n", err)
		} else {
			log.Printf("Message Sent with Id, %v\n", id)
		}
		time.Sleep(sleep)
	}
}
