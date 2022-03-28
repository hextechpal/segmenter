package segmenter

import (
	"fmt"
)

var NonExistentStream = fmt.Errorf("stream do not exist")
var EmptyStreamName = fmt.Errorf("stream name cannot be empty")
var EmptyGroupName = fmt.Errorf("group cannot be empty")
var InvalidBatchSize = fmt.Errorf("batch size cannot less than 1")
var InvalidPartitionCount = fmt.Errorf("partition count cannot less than 1")
var InvalidPartitionSize = fmt.Errorf("partition size cannot less than 1")
