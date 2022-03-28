/*
Package segmenter implements partition over redis streams

Redis streams are great, fast and easy to use. But sometimes we want to ensure ordering in processing of events
This library guarantees that all the events based on a partition key are processed in order on a single partition
It also allows automatic re-balancing i.e. if a consumer is added/removed(dies) then the partitions are rebalanced
and the ordering property is followed

*/
package segmenter
