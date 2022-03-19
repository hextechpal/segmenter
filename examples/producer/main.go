package main

import (
	"context"
	"github.com/hextechpal/segmenter/examples"
	"github.com/hextechpal/segmenter/pkg/api"
	"log"
	"sync"
	"time"
)

func main() {
	ctx := context.TODO()

	s, err := api.NewSegmenter(&api.Config{Address: "localhost:6379", Namespace: examples.Namespace})
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
			_, err := s.RegisterConsumer(ctx, examples.StreamName, 10)
			if err != nil {
				log.Printf("Error happened while registering Consumer, %v", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	time.Sleep(1 * time.Minute)
}
