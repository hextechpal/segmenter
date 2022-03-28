package segmenter

import (
	"fmt"
)

var ErrorNonExistentStream = fmt.Errorf("stream do not exist")
var ErrorEmptyStreamName = fmt.Errorf("stream name cannot be empty")
var ErrorEmptyGroupName = fmt.Errorf("group cannot be empty")
var ErrorInvalidBatchSize = fmt.Errorf("batch size cannot less than 1")
var ErrorInvalidPartitionCount = fmt.Errorf("partition count cannot less than 1")
var ErrorInvalidPartitionSize = fmt.Errorf("partition size cannot less than 1")
