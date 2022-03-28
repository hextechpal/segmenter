package utils

import (
	"hash/fnv"
)

// Hash : Calculates 32-bit FNV-1a hash from a string
func Hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}
