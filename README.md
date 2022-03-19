# segmenter

This library implements kafka like partitions(segments) over redis streams. You can define the number of partitions for a stream and it makes sure that
the messages belonging to single segment is processed in order on a single consumer

The usage is 

```
package main

import (
	"context"
	"fmt"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"log"
	"time"
)

func main() {

	stream := "myStream" // Name of the stream
	group := "group1"    // Name of the consumer group

	ctx := context.TODO()

	// Register the segmenter
	// It takes a config object and a namespace. You can have multiple segmenter instances
	// with diff namespace in same application

	s, err := segmenter.NewSegmenter(&segmenter.Config{Address: "localhost:6379", Namespace: "example"})
	if err != nil {
		log.Fatalf("Error occurred while initializing segmenter, %v", err)
	}

	// Here we are registering the stream. It takes a
	// streamName : Name of the stream while you want to register with segmenter
	// pcount : cont of the partition for the steam
	// psize : size of partitions
	// Please note streams are immutable. You cannoe edit the partitions/size once created

	st, err := s.RegisterStream(ctx, stream, 20, 2500)
	if err != nil {
		log.Fatalf("Error occurred while registering streamName, %v", err)
	}

	// This is how you send data to the stream
	// It takes a PMessage (producer message). It has only 2 fields
	// data : []byte, bytes of data which you want to sent to stream
	// partitionKey : based in the partitionKey your data will be send to approprite function
	// based on a pre-defined hash function
	// If your message is sent successfully we return you the id of the message

	id, err := st.Send(context.TODO(), &contracts.PMessage{
		Data:         []byte(fmt.Sprintf("Some Message can be anything")),
		PartitionKey: "partitionKey",
	})
	if err != nil {
		log.Printf("Error happened while sending message, %v\n", err)
	} else {
		log.Printf("Message Sent with Id, %v\n", id)
	}

	// This is how you can register a consumer
	// As more and more consumers join your stream partitions are redistributed among these consumers
	// It takes the following arguments
	// stream : Name of the stream against which this consumer would be registered
	// group : Name of the consumer group. If it doesnt exist it will be created for you
	// batchSize : batch size of messages
	c, err := s.RegisterConsumer(ctx, stream, group, 10)
	if err != nil {
		log.Fatalf("Error happened while registering Consumer, %v", err)
	}

	// This is how you consume messages from the consumer
	// It takes a block duration as the argument
	messages, err := c.GetMessages(ctx, 200*time.Millisecond)
	if err != nil {
		log.Fatalf("Error while reading messages, %v\n", err)
	}
}


```
