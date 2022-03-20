package integration

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestGetMessages(t *testing.T) {
	ns := "ppal"
	streamName := fmt.Sprintf("user%d", rand.Intn(10))
	ctx := context.Background()
	c := segmenter.Config{
		RedisOptions: &redis.Options{Addr: "localhost:6379"},
		NameSpace:    ns,
	}
	s, err := segmenter.NewSegmenter(&c)
	if err != nil {
		log.Fatalf("Error occurred while initializing segmenter, %v", err)
	}

	st, err := s.RegisterStream(ctx, streamName, 2, 250)
	if err != nil {
		log.Fatalf("Error occurred while registering streamName, %v", err)
	}

	// Sending 20 messages in the stream that will be divided across partitions
	for i := 0; i < 10; i++ {
		uuid := fmt.Sprintf("uuid_%d", rand.Intn(1000))
		id, err := st.Send(context.TODO(), &contracts.PMessage{
			Data:         []byte(fmt.Sprintf("Message with uuid : %s", uuid)),
			PartitionKey: uuid,
		})
		if err != nil {
			log.Printf("Error happened while sending message, %v\n", err)
		} else {
			log.Printf("Message Sent with Id, %v\n", id)
		}
	}

	c1, err := s.RegisterConsumer(ctx, streamName, "group1", 10, time.Second)
	if err != nil {
		log.Fatalf("Error happened while registering Consumer c1, %v", err)
	}

	c2, err := s.RegisterConsumer(ctx, streamName, "group1", 10, time.Second)
	if err != nil {
		log.Fatalf("Error happened while registering Consumer c2, %v", err)
	}

	// Now the partitions should be divided among these two consumer.
	// Should be -> c1:[0, 1], c2:[2]
	time.Sleep(2 * time.Second)
	c1m, err := c1.GetMessages(ctx, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Error happened while reading messages from Consumer c1, %v", err)
	}
	log.Printf("Comsumer 1 has claimed %d messages from stream", len(c1m))
	for _, m := range c1m {
		err := c1.AckMessages(ctx, m)
		if err != nil {
			log.Fatalf("Error happened while Acking c1's message, %v\n", err)
		}
	}

	c2m, err := c2.GetMessages(ctx, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Error happened while reading messages from Consumer c2, %v", err)
	}
	ids := ""
	for _, m := range c2m {
		ids += m.Id + "|"
	}
	log.Printf("Comsumer 2 has claimed %d messages from stream ids %s\n", len(c2m), ids)

	// Shutting down Consumer 2
	err = c2.ShutDown(ctx)
	if err != nil {
		log.Fatalf("Error happened while shutting down c2, %v", err)
	}

	time.Sleep(2 * time.Second)

	c1mNew, err := c1.GetMessages(ctx, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Error happened while reading messages from Consumer c1, %v", err)
	}
	ids = ""
	for _, m := range c2m {
		ids += m.Id + "|"
	}
	log.Printf("Comsumer 1 has claimed %d new messages from stream, ids : %v", len(c1mNew), ids)

	time.Sleep(1 * time.Minute)
}
