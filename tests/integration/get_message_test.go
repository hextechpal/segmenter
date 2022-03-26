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
	ns := "hextech"
	streamName := fmt.Sprintf("user%d", rand.Intn(10))
	ctx := context.Background()

	// Register the segmenter
	// It takes a config object and a namespace. You can have multiple segmenter instances
	// with diff namespace in same application
	c := segmenter.Config{
		RedisOptions: &redis.Options{Addr: "localhost:6379"},
		NameSpace:    ns,
		Debug:        true,
	}
	s, err := segmenter.NewSegmenter(&c)
	if err != nil {
		t.Fatalf("NewSegmenter(), err = %v", err)
	}

	// Here we are registering the stream. It takes a
	// streamName : Name of the stream while you want to register with segmenter
	// pcount : cont of the partition for the steam
	// psize : size of partitions
	// Please note streams are immutable. You cannot edit the partitions/size once created

	st, err := s.RegisterStream(ctx, streamName, 2, 250)
	if err != nil {
		t.Fatalf("RegisterStream(), err = %v", err)
	}

	// This is how you can register a consumer
	// As more and more consumers join your stream partitions are redistributed among these consumers
	// It takes the following arguments
	// stream : Name of the stream against which this consumer would be registered
	// group : Name of the consumer group. If it doesn't exist it will be created for you
	// batchSize : batch size of messages
	// maxProcessingTime : Maximum time before the message is eligible for redelivery. This will come into picture when
	// a consumer dies, then after re-balancing partitions. the messages will be delivered to new consumers
	c1, err := s.RegisterConsumer(ctx, streamName, "group1", 10, time.Second)
	if err != nil {
		t.Fatalf("Consumer1 : RegisterConsumer() err = %v", err)
	}
	t.Logf("Consumer1 Registered id: %s", c1.GetID())

	// Register One more consumer
	c2, err := s.RegisterConsumer(ctx, streamName, "group1", 10, time.Second)
	if err != nil {
		t.Fatalf("Consumer2 : RegisterConsumer() err = %v", err)
	}
	t.Logf("Consumer1 Registered id: %s", c2.GetID())

	// Now the partitions should be divided among these two consumer.
	// Should be -> c1:[0], c2:[2]
	time.Sleep(time.Second)

	// This is how you send data to the stream
	// It takes a PMessage (producer message). It has only 2 fields
	//
	// data : []byte, bytes of data which you want to send to stream
	// partitionKey : based in the partitionKey your data will be sent to appropriate partition based on a pre-defined hash function
	//
	// If your message is sent successfully we return you the id of the message
	//
	// Sending 10 messages in the stream that will be divided across 2 partitions
	for i := 0; i < 10; i++ {
		uuid := fmt.Sprintf("uuid_%d", rand.Intn(1000))
		_, _ = st.Send(context.TODO(), &contracts.PMessage{
			Data:         []byte(fmt.Sprintf("Message with uuid : %s", uuid)),
			PartitionKey: uuid,
		})
	}

	// Reading messages from the consumer
	// It returns a []CMessage (consumer message). It has 3 fields
	// id : id of the message
	// partitionKey : This is same as provided in the input
	// data : []byte the data of the message
	c1m, err := c1.Read(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Consumer1 : Read() err = %v", err)
	}
	t.Logf("Comsumer1 : Claimed %d messages Messages : %v", len(c1m), c1m)

	// Consumer also exposes the API to ack the messages
	// Here we are acking all the messages delivered to consumer 1
	for _, m := range c1m {
		err := c1.Ack(ctx, m)
		if err != nil {
			t.Fatalf("Error happened while Acking c1's message, %v\n", err)
		}
	}

	// Reading messaged from consumer 2
	// this will effectively read messaged from the second partition of the stream
	c2m, err := c2.Read(ctx, 100*time.Millisecond)
	if err != nil {
		log.Fatalf("Error happened while reading messages from Consumer c2, %v", err)
	}
	ids := ""
	for _, m := range c2m {
		ids += m.Id + "|"
	}

	// Logging the ids off the message
	//t.Logf("Comsumer 2 has claimed %d messages from stream ids %s\n", len(c2m), ids)

	// Shutting down Consumer 2 without acking the above messages
	// This will cause the second partition to be assigned again to consumer 1
	err = c2.ShutDown()
	if err != nil {
		log.Fatalf("Error happened while shutting down c2, %v", err)
	}

	// Just adding sleep to reach steady state. In production, we read messages periodically
	// and hence we do not need this
	time.Sleep(10 * time.Second)

	// Reading messages again from consumer 1
	// This should give you messages which consumer 2 read but didn't ack
	// As consumer 2 has died, consumer 1 will be reassigned it partition and now owns the messaged from partition 2
	// as well
	c1mNew, err := c1.Read(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Error happened while reading messages from Consumer c1, %v", err)
	}
	t.Logf("Comsumer 1 has claimed %d new messages from stream, ids : %v", len(c1mNew), c1mNew)

	for _, m := range c1mNew {
		err := c1.Ack(ctx, m)
		if err != nil {
			t.Fatalf("Error happened while Acking c1's message, %v\n", err)
		}
	}

	c1mFinal, err := c1.Read(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatalf("Error happened while reading messages from Consumer c1, %v", err)
	}

	if len(c1mFinal) != 0 {
		t.Fatalf("Expected 0 messages got %d", len(c1mFinal))
	}

	time.Sleep(30 * time.Second)
}
