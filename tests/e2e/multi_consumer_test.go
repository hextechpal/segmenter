package e2e

import (
	"testing"
	"time"
)

func TestMultiConsumer(t *testing.T) {
	streamName := "sgStream"
	group := "multiConsumerTestGrp"
	seg := createSegmenter(t)
	st, err := seg.RegisterStream(ctx, streamName, pcount, psize)
	ptime := 200 * time.Millisecond
	if err != nil {
		t.Fatalf("RegisterStream(), err = %s", err)
	}

	sp1 := registerConsumer(t, ctx, seg, streamName, group, ptime, true)
	sp2 := registerConsumer(t, ctx, seg, streamName, group, ptime, true)

	time.Sleep(100 * time.Millisecond)

	for _, sp := range []spawnInfo{sp1, sp2} {
		go read(t, ctx, sp.c, sp.ch, sp.ack)
	}

	msgCount := 1000
	go sendMessages(t, st, msgCount)

	c1Count := 0
	c2Count := 0

	start := time.Now()
	for time.Since(start) < 30*time.Second && c1Count+c2Count < msgCount {
		select {
		case c1Ids := <-sp1.ch:
			c1Count += len(c1Ids)
		case c2Ids := <-sp2.ch:
			c2Count += len(c2Ids)
			t.Logf("Msgs from consumer 2 %v", len(c2Ids))
		default:

		}
	}

	t.Logf("Total Messages c1(%s)=%d, c2(%s)=%d", sp1.c.GetID(), c1Count, sp2.c.GetID(), c2Count)
	if c1Count+c2Count != msgCount {
		t.Fatalf("No all messages consumed in 5 seconds")
	}

}
