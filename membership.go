package segmenter

type Reason string

const join Reason = "join"
const leave Reason = "leave"

type member struct {
	ConsumerId string     `json:"consumerId"`
	JoinedAt   int64      `json:"joinedAt"`
	Partitions partitions `json:"partitions"`
	Group      string     `json:"group"`
}

type memberChangeInfo struct {
	Reason     Reason `json:"reason"`
	ConsumerId string `json:"consumerId"`
	Group      string `json:"group"`
	Ts         int64  `json:"ts"`
}

type Members []member

func (ms Members) Contains(memberId string) bool {
	for _, m := range ms {
		if m.ConsumerId == memberId {
			return true
		}
	}
	return false
}

func (ms Members) Add(member member) Members {
	return append(ms, member)
}

func (ms Members) Remove(mid string) Members {
	idx := -1
	for i, m := range ms {
		if m.ConsumerId == mid {
			idx = i
		}
	}

	if idx < 0 {
		return ms
	}

	return append(ms[:idx], ms[idx+1:]...)
}

func (ms Members) Len() int {
	return len(ms)
}

func (ms Members) Less(i, j int) bool {
	return ms[i].JoinedAt < ms[j].JoinedAt
}

func (ms Members) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms Members) RemoveAll(members Members) Members {
	removed := make([]member, ms.Len()-members.Len())
	idx := 0
	for _, m := range ms {
		if !members.Contains(m.ConsumerId) {
			removed[idx] = m
			idx++
		}
	}
	return removed
}

func (ms Members) FilterBy(group string) Members {
	nMembers := make([]member, 0)
	for _, m := range ms {
		if m.Group == group {
			nMembers = append(nMembers, m)
		}
	}
	return nMembers
}
