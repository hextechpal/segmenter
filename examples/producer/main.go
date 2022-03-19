package main

import (
	"context"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/examples"
	"log"
	"sync"
	"time"
)

func main() {
	ctx := context.TODO()

	s, err := segmenter.NewSegmenter(&segmenter.Config{Address: "localhost:6379", Namespace: examples.Namespace})
	if err != nil {
		log.Fatalf("Error occurred while initializing segmenter, %v", err)
	}

	_, err = s.RegisterStream(ctx, examples.StreamName, 20, 2500)
	if err != nil {
		log.Fatalf("Error occurred while registering streamName, %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.RegisterConsumer(ctx, examples.StreamName, 10)
			if err != nil {
				log.Printf("Error happened while registering Consumer, %v", err)
			} else {
				log.Printf("Registered Consumer with id %s\n", c.GetID())
			}

		}()
	}
	wg.Wait()
	time.Sleep(1 * time.Minute)
}
