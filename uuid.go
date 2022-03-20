package segmenter

import (
	"encoding/base64"
	"github.com/google/uuid"
)

func generateUuid() string {
	id := uuid.New()
	return base64.RawURLEncoding.EncodeToString(id[:])
}
