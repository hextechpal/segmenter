package segmenter

import (
	"context"
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
)

type Producer interface {
	Produce(ctx context.Context, message *contracts.PMessage) (string, error)
}
