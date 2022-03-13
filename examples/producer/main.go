package main

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/common"
	"github.com/hextechpal/segmenter/pkg/api"
	"log"
	"strconv"
)

func main() {
	ctx := context.TODO()
	s, err := api.NewSegmenter(&api.Config{Address: "localhost:6379", Namespace: common.GenerateUuid()[:4]})
	if err != nil {
		log.Fatalf("Error occurred while initializing segmenter, %v", err)
	}

	p, err := s.RegisterProducer(ctx, 50, 5, "account")
	if err != nil {
		log.Fatalf("Error occurred while registering producer, %v", err)
	}
	for i := 0; i < 100; i++ {
		var buf bytes.Buffer
		acc := &Account{ID: i, Name: fmt.Sprintf("Name_%d", i)}
		err := gob.NewEncoder(&buf).Encode(acc)
		if err != nil {
			log.Fatalf("Error while encoding account, %v", err)
		}
		mid, err := p.Produce(ctx, &contracts.PMessage{
			Metadata:     nil,
			Data:         buf.Bytes(),
			PartitionKey: strconv.Itoa(i),
		})
		if err != nil {
			log.Fatalf("Error while produving message, %v", err)
		}
		log.Printf("Message Enqueued with ID : %s", mid)
	}

}

type Account struct {
	ID   int
	Name string
}
