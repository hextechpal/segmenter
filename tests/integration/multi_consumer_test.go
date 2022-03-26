package integration

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"github.com/hextechpal/segmenter/internal/segmenter/core"
	"math/rand"
	"testing"
	"time"
)

const (
	// Stream Settings
	streamName = "sgStream"
	pcount     = 5
	psize      = 500

	// Consumer Settings
	group     = "multiConsumerTestGrp"
	batchSize = 2
	ptime     = 500 * time.Millisecond
)

var ctx = context.Background()

func init() {
	rand.Seed(time.Now().UnixMilli())
}

func createSegmenter(t *testing.T) *segmenter.Segmenter {
	t.Helper()

	ns := fmt.Sprintf("hextech%d", rand.Intn(20))
	//ns := "hextech"
	t.Logf("Starting segmenter for Namespace %s", ns)
	// Register the segmenter
	c := segmenter.Config{
		RedisOptions: &redis.Options{Addr: "localhost:6379"},
		NameSpace:    ns,
		Debug:        false,
	}
	s, err := segmenter.NewSegmenter(&c)
	if err != nil {
		t.Fatalf("NewSegmenter(), err = %v", err)
	}
	return s
}

func TestMultiConsumer(t *testing.T) {
	seg := createSegmenter(t)
	st, err := seg.RegisterStream(ctx, streamName, pcount, psize)
	if err != nil {
		t.Fatalf("RegisterStream(), err = %s", err)
	}
	t.Logf("Stream Registered %s", st.GetName())

	c1, err := seg.RegisterConsumer(ctx, streamName, group, batchSize, ptime)
	if err != nil {
		t.Fatalf("RegisterStream(), err = %s", err)
	}
	t.Logf("Comsumer Registered : %s\n", c1.GetID())

	c2, err := seg.RegisterConsumer(ctx, streamName, group, batchSize, ptime)
	if err != nil {
		t.Fatalf("RegisterStream(), err = %s", err)
	}
	t.Logf("Comsumer Registered : %s\n", c2.GetID())

	// startConsumer routine should ack the read messages or not
	// this also is indicative of the number of consumers we will spawn
	type spawnInfo struct {
		c   *core.Consumer
		ch  chan []string
		ack bool
	}

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
		go Read(t, sp.c, sp.ch, sp.ack)
	}

	// Sleeping so that consumers can rebalance else all the messages will be
	// consumed by consumers who registered first
	//time.Sleep(3000 * time.Millisecond)

	msgCount := 10
	go sendMessages(st, msgCount)
	//time.Sleep(5 * time.Second)

	c1Count := 0
	c2Count := 0

	start := time.Now()
	for time.Since(start) < 500*time.Second && c1Count+c2Count < msgCount {
		select {
		case c1Ids := <-sp1.ch:
			t.Logf("Msgs from consumer 1 %v", c1Ids)
			c1Count += len(c1Ids)
		case c2Ids := <-sp2.ch:
			c2Count += len(c2Ids)
			t.Logf("Msgs from consumer 2 %v", c2Ids)
		default:

		}
	}

	if c1Count+c2Count != msgCount {
		t.Fatalf("No all messages consumed in 5 seconds")
	}
}

func sendMessages(st *core.Stream, count int) {
	for i := 0; i < count; i++ {
		uuid := fmt.Sprintf("uuid_%d", rand.Intn(1000))
		_, _ = st.Send(context.TODO(), &contracts.PMessage{
			Data:         []byte(fmt.Sprintf("Message with uuid : %s", uuid)),
			PartitionKey: uuid,
		})
	}
}

func Read(t *testing.T, c *core.Consumer, ch chan []string, ack bool) {
	for {
		msgs, err := c.Read(ctx, 100*time.Millisecond)
		if err != nil {
			if err == core.ConsumerDeadError {
				t.Logf("[%s] Consumer Dead, Returning", c.GetID())
				return
			}
			t.Fatalf("[%s] Read(), err = %s", c.GetID(), err)
		}
		//t.Logf(" [%d] Messages received from [%s]", len(msgs), c.GetID())
		if len(msgs) > 0 {
			ids := make([]string, len(msgs))
			for i, m := range msgs {
				ids[i] = m.Id
				if ack {
					_ = c.Ack(ctx, m)
				}
			}
			ch <- ids
		}
		time.Sleep(100 * time.Millisecond)
	}

}
