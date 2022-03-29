package e2e

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/hextechpal/segmenter"
	"github.com/hextechpal/segmenter/api/proto/contracts"
	"math/rand"
	"testing"
	"time"
)

const (
	pcount = 5
	psize  = 500

	// Consumer Settings
	batchSize = 200
)

type spawnInfo struct {
	c   *segmenter.Consumer
	ch  chan []string
	ack bool
}

var ctx = context.Background()

func init() {
	rand.Seed(time.Now().UnixMilli())
}

func createSegmenter(t *testing.T) *segmenter.Segmenter {
	t.Helper()

	ns := fmt.Sprintf("hextech%d", rand.Intn(20000))
	t.Logf("Starting segmenter for Namespace %s", ns)
	// Register the segmenter
	c := segmenter.Config{
		RedisOptions: &redis.Options{Addr: "localhost:6379"},
		NameSpace:    ns,
		Debug:        true,
	}
	s, err := segmenter.NewSegmenter(&c)
	if err != nil {
		t.Fatalf("NewSegmenter(), err = %v", err)
	}
	return s
}

func sendMessages(t *testing.T, st *segmenter.Stream, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		uuid := fmt.Sprintf("uuid_%d", rand.Intn(1000))
		// This is how you send data to the stream
		// It takes a PMessage (producer message). It has only 2 fields
		//
		// data : []byte, bytes of data which you want to send to stream
		//
		// partitionKey : based in the partitionKey your data will be sent to appropriate partition based on a
		// pre-defined hash function. This Hash function can be fed as an input to segmenter in future

		// If your message is sent successfully we return you the id of the message (ignored below)
		_, _ = st.Send(context.TODO(), &contracts.PMessage{
			Data:         []byte(fmt.Sprintf("Message with uuid : %s", uuid)),
			PartitionKey: uuid,
		})
	}
}

func registerConsumer(ctx context.Context, t *testing.T, seg *segmenter.Segmenter, streamName string, group string, ptime time.Duration, ack bool) spawnInfo {
	c1, err := seg.RegisterConsumer(ctx, streamName, group, batchSize, ptime)
	if err != nil {
		t.Fatalf("RegisterStream(), err = %s", err)
	}
	t.Logf("Comsumer Registered : %s\n", c1.GetID())
	return spawnInfo{
		c:   c1,
		ch:  make(chan []string),
		ack: ack,
	}
}

func read(t *testing.T, ctx context.Context, c *segmenter.Consumer, ch chan []string, ack bool) {
	t.Helper()
	for {
		msgs, err := c.Read(ctx, 100*time.Millisecond)
		if err != nil {
			if err == segmenter.ConsumerDeadError {
				t.Logf("[%s] Consumer Dead, Returning", c.GetID())
				return
			}
			t.Fatalf("[%s] read(), err = %s", c.GetID(), err)
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
