package segmenter

type partition int

type partitions []partition

func (p partitions) Contains(t partition) bool {
	for _, el := range p {
		if el == t {
			return true
		}
	}
	return false
}
