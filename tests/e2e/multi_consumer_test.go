package e2e

import (
	"context"
	"math/rand"
	"testing"
	"time"
)

var ctx = context.Background()

func init() {
	rand.Seed(time.Now().UnixMilli())
}

func TestMultiConsumer(t *testing.T) {
	t.Run("MultiConsumer", func(t *testing.T) {
		streamName := "sgStream"
		group := "multiConsumerTestGrp"
		seg := createSegmenter(t)
		st, err := seg.RegisterStream(ctx, streamName, pcount, psize)
		if err != nil {
			t.Fatalf("RegisterStream(), err = %s", err)
		}
		t.Logf("Stream Registered %s", st.GetName())

		c1, err := seg.RegisterConsumer(ctx, streamName, group, batchSize, 3*time.Second)
		if err != nil {
			t.Fatalf("RegisterStream(), err = %s", err)
		}
		t.Logf("Comsumer Registered : %s\n", c1.GetID())

		c2, err := seg.RegisterConsumer(ctx, streamName, group, batchSize, 3*time.Second)
		if err != nil {
			t.Fatalf("RegisterStream(), err = %s", err)
		}
		t.Logf("Comsumer Registered : %s\n", c2.GetID())

		time.Sleep(100 * time.Millisecond)
		// startConsumer routine should ack the read messages or not
		// this also is indicative of the number of consumers we will spawn

		sp1 := spawnInfo{
			c:   c1,
			ch:  make(chan []string),
			ack: true,
		}

		sp2 := spawnInfo{
			c:   c2,
			ch:  make(chan []string),
			ack: true,
		}

		for _, sp := range []spawnInfo{sp1, sp2} {
			go Read(t, ctx, sp.c, sp.ch, sp.ack)
		}

		msgCount := 1000
		go sendMessages(t, st, msgCount)
		//time.Sleep(5 * time.Second)

		c1Count := 0
		c2Count := 0

		start := time.Now()
		for time.Since(start) < 500*time.Second && c1Count+c2Count < msgCount {
			select {
			case c1Ids := <-sp1.ch:
				t.Logf("Msgs from consumer 1 %v", len(c1Ids))
				c1Count += len(c1Ids)
			case c2Ids := <-sp2.ch:
				c2Count += len(c2Ids)
				t.Logf("Msgs from consumer 2 %v", len(c2Ids))
			default:

			}
		}

		t.Logf("Total Messages c1(%s)=%d, c2(%s)=%d", c1.GetID(), c1Count, c2.GetID(), c2Count)
		if c1Count+c2Count != msgCount {
			t.Fatalf("No all messages consumed in 5 seconds")
		}
		time.Sleep(30 * time.Second)
	})

}
