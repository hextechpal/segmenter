package e2e

import (
	"testing"
	"time"
)

// This test attempts to showcase that segmenter can work with multiple consumer groups
// In each consumer group the messages belonging to a particular partition will be delivered to
// a single consumer in that particular group

func TestMultiConsumerGroups(t *testing.T) {

	streamName := "sgStream"
	group1 := "multiCgGrp1"
	group2 := "multiCgGrp2"
	seg := createSegmenter(t)
	ptime := 100 * time.Millisecond

	// Register the stream
	st, err := seg.RegisterStream(ctx, streamName, pcount, psize)
	if err != nil {
		t.Fatalf("RegisterStream(), err = %s", err)
	}
	t.Logf("Stream Registered %s", st.GetName())

	// Register consumers in consumer group1
	sp11 := registerConsumer(t, ctx, seg, streamName, group1, ptime, true)
	sp12 := registerConsumer(t, ctx, seg, streamName, group1, ptime, true)

	// Register consumers in consumer group3
	sp21 := registerConsumer(t, ctx, seg, streamName, group2, ptime, true)
	sp22 := registerConsumer(t, ctx, seg, streamName, group2, ptime, true)

	// Spawn go routines which makes these consumers read from the stream
	// There routines return the read messages via channels
	for _, sp := range []spawnInfo{sp11, sp12, sp21, sp22} {
		go read(t, ctx, sp.c, sp.ch, sp.ack)
	}

	// Just Sleeping 500ms here so that re-balancing should reach steady state
	// This is only needed for test to make deterministic assertions
	time.Sleep(500 * time.Millisecond)

	// Sending 1000 messages in the stream
	msgCount := 1000
	go sendMessages(t, st, msgCount)

	// Count of messages read by consumers in group1
	c11Count := 0
	c12Count := 0

	// Count of messages read by consumers in group2
	c21Count := 0
	c22Count := 0

	start := time.Now()
	for time.Since(start) < 30*time.Second && (c11Count+c12Count < msgCount || c21Count+c22Count < msgCount) {
		select {
		case c11Ids := <-sp11.ch:
			c11Count += len(c11Ids)
		case c12Ids := <-sp12.ch:
			c12Count += len(c12Ids)
		case c21Ids := <-sp21.ch:
			c21Count += len(c21Ids)
		case c22Ids := <-sp22.ch:
			c22Count += len(c22Ids)
		default:

		}
	}

	t.Logf("Total Messages c11(%s)=%d, c12(%s)=%d", sp11.c.GetID(), c11Count, sp12.c.GetID(), c12Count)

	t.Logf("Total Messages c21(%s)=%d, c22(%s)=%d", sp21.c.GetID(), c21Count, sp22.c.GetID(), c22Count)

	// Making sure both the groups read all the messages and there was no overlap of messages
	if c11Count+c12Count != msgCount || c21Count+c22Count != msgCount {
		t.Fatalf("No all messages consumed in 5 seconds")
	}

}
