package core

import (
	"fmt"
)

func StreamAdmin(ns string, stream string) string {
	return fmt.Sprintf("__%s:__%s:admin", ns, stream)
}

func HeartBeat(ns, name, id string) string {
	return fmt.Sprintf("__%s:__%s:__beat:%s", ns, name, id)
}

func PartitionedStream(ns, stream string, pc Partition) string {
	return fmt.Sprintf("__%s:__%s:strm_%d", ns, stream, pc)
}
