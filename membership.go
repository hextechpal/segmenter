package segmenter

type reason string

const join reason = "join"
const leave reason = "leave"

type member struct {
	ID         string     `json:"consumerId"`
	JoinedAt   int64      `json:"joinedAt"`
	Partitions partitions `json:"partitions"`
	Group      string     `json:"group"`
}

type memberChangeInfo struct {
	Reason     reason `json:"reason"`
	ConsumerId string `json:"consumerId"`
	Group      string `json:"group"`
	Ts         int64  `json:"ts"`
}

type members []member

func (ms members) Contains(memberId string) bool {
	for _, m := range ms {
		if m.ID == memberId {
			return true
		}
	}
	return false
}

func (ms members) Add(member member) members {
	return append(ms, member)
}

func (ms members) Remove(mid string) members {
	idx := -1
	for i, m := range ms {
		if m.ID == mid {
			idx = i
		}
	}

	if idx < 0 {
		return ms
	}

	return append(ms[:idx], ms[idx+1:]...)
}

func (ms members) Len() int {
	return len(ms)
}

func (ms members) Less(i, j int) bool {
	return ms[i].JoinedAt < ms[j].JoinedAt
}

func (ms members) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms members) RemoveAll(members members) members {
	removed := make([]member, ms.Len()-members.Len())
	idx := 0
	for _, m := range ms {
		if !members.Contains(m.ID) {
			removed[idx] = m
			idx++
		}
	}
	return removed
}
