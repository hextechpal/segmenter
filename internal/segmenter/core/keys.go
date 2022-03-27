package core

import (
	"fmt"
)

func StreamAdmin(ns string, stream string) string {
	return fmt.Sprintf("__%s:__%s:admin", ns, stream)
}

func HeartBeat(ns, name, id, group string) string {
	return fmt.Sprintf("__%s:__%s_%s:__beat:%s", ns, name, group, id)
}

func PartitionedStream(ns, stream string, pc Partition) string {
	return fmt.Sprintf("__%s:__%s:strm_%d", ns, stream, pc)
}
