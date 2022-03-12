package segmenter

import (
	"github.com/hextechpal/segmenter/internal/api/proto/contracts"
	"time"
)

type Consumer interface {
	GetMessages(maxWaitDuration time.Duration) []contracts.CMessage
}
