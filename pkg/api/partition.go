package api

type Partition int

type Partitions []Partition

func (p Partitions) contains(t Partition) bool {
	for _, el := range p {
		if el == t {
			return true
		}
	}
	return false
}
