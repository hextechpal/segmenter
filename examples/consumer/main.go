package main

import (
	"context"
	"github.com/hextechpal/segmenter/pkg/api"
	"log"
	"time"
)

func main() {
	ctx := context.TODO()
	s, err := api.NewSegmenter(&api.Config{Address: "localhost:6379", Namespace: "uqLV"})
	if err != nil {
		log.Fatalf("Error occurred while initializing segmenter, %v", err)
	}

	_, err = s.RegisterConsumer(ctx, "account", 10*time.Millisecond, 2)
	if err != nil {
		log.Fatalf("Error occurred while registering producer, %v", err)
	}
}
