package e2e

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
	pcount = 5
	psize  = 500

	// Consumer Settings
	batchSize = 200
)

type spawnInfo struct {
	c   *core.Consumer
	ch  chan []string
	ack bool
}

func createSegmenter(t *testing.T) *segmenter.Segmenter {
	t.Helper()

	ns := fmt.Sprintf("hextech%d", rand.Intn(20000))
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

func sendMessages(t *testing.T, st *core.Stream, count int) {
	t.Helper()
	for i := 0; i < count; i++ {
		uuid := fmt.Sprintf("uuid_%d", rand.Intn(1000))
		_, _ = st.Send(context.TODO(), &contracts.PMessage{
			Data:         []byte(fmt.Sprintf("Message with uuid : %s", uuid)),
			PartitionKey: uuid,
		})
	}
}

func Read(t *testing.T, ctx context.Context, c *core.Consumer, ch chan []string, ack bool) {
	t.Helper()
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
