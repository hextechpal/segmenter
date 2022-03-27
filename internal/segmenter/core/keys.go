package core

import (
	"fmt"
)

func HeartBeat(ns, name, id, group string) string {
	return fmt.Sprintf("__%s:%s:%s:beat:%s", ns, name, group, id)
}

func PartitionedStream(ns, stream string, p Partition) string {
	return fmt.Sprintf("__%s:%s:partition_%d", ns, stream, p)
}
