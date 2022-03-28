package e2e

import (
	"github.com/hextechpal/segmenter"
	"testing"
	"time"
)

// This test attempts to showcase that segmenter can work with multiple streams
// Each stream is independent

func TestMultiStream(t *testing.T) {
	seg := createSegmenter(t)
	ch := make(chan bool)
	go testForStream(t, "sgStream1", "group1", seg, ch)
	go testForStream(t, "sgStream2", "group2", seg, ch)

	res := true
	for i := 0; i < 2; i++ {
		res = res && <-ch
	}

	if !res {
		t.Fatalf("Test failed")
	}

}

func testForStream(t *testing.T, streamName, group string, seg *segmenter.Segmenter, done chan bool) {
	ptime := 100 * time.Millisecond
	st, err := seg.RegisterStream(ctx, streamName, pcount, psize)
	if err != nil {
		done <- false
		t.Fatalf("RegisterStream(), err = %s", err)
	}

	sp1 := registerConsumer(t, ctx, seg, streamName, group, ptime, true)
	sp2 := registerConsumer(t, ctx, seg, streamName, group, ptime, true)
	time.Sleep(time.Second)

	for _, sp := range []spawnInfo{sp1, sp2} {
		go Read(t, ctx, sp.c, sp.ch, sp.ack)
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
		default:

		}
	}

	t.Logf("Total Messages c1(%s)=%d, c2(%s)=%d", sp1.c.GetID(), c1Count, sp2.c.GetID(), c2Count)
	if c1Count+c2Count != msgCount {
		done <- false
		t.Fatalf("No all messages consumed in 5 seconds")
	}
	done <- true
}
