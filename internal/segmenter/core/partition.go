package core

type Partition int

type Partitions []Partition

func (p Partitions) Contains(t Partition) bool {
	for _, el := range p {
		if el == t {
			return true
		}
	}
	return false
}
