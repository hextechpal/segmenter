package utils

import (
	"encoding/base64"
	"github.com/google/uuid"
)

// GenerateUuid : Generate a unique string id based on google/uuid
func GenerateUuid() string {
	id := uuid.New()
	return base64.RawURLEncoding.EncodeToString(id[:])
}
