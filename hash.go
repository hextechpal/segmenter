package segmenter

import (
	"fmt"
	"hash/fnv"
)

func Hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func StreamKey(ns, stream string, pc Partition) string {
	return fmt.Sprintf("__%s:__%s:strm_%d", ns, stream, pc)
}
