package main

import (
	"context"
	"fmt"
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
	s, err := segmenter.NewSegmenter(&segmenter.Config{Address: "localhost:6379", Namespace: examples.Namespace})
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
