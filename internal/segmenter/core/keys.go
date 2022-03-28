package core

import (
	"fmt"
)

func heartBeat(ns, name, id, group string) string {
	return fmt.Sprintf("__%s:%s:%s:beat:%s", ns, name, group, id)
}

func partitionedStream(ns, stream string, p Partition) string {
	return fmt.Sprintf("__%s:%s:partition_%d", ns, stream, p)
}
