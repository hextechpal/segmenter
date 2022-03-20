package segmenter

import (
	"fmt"
	"hash/fnv"
)

func hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

func streamKey(ns, stream string, pc partition) string {
	return fmt.Sprintf("__%s:__%s:strm_%d", ns, stream, pc)
}
