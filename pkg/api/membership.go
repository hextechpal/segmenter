package api

import "fmt"

type Reason string

const JOIN Reason = "JOIN"
const LEAVE Reason = "LEAVE"

type Member struct {
	ConsumerId string     `json:"consumerId"`
	JoinedAt   int64      `json:"joinedAt"`
	Partitions Partitions `json:"partitions"`
}

func (m Member) heartBeatKey(ns, stream string) string {
	return fmt.Sprintf("__%s:%s:__beat:%s", ns, stream, m.ConsumerId)
}

type MemberChangeInfo struct {
	Reason     Reason `json:"reason"`
	ConsumerId string `json:"consumerId"`
	Ts         int64  `json:"ts"`
}

type Members []Member

func (ms Members) Contains(memberId string) bool {
	for _, m := range ms {
		if m.ConsumerId == memberId {
			return true
		}
	}
	return false
}

func (ms Members) Add(member Member) Members {
	return append(ms, member)
}

func (ms Members) Remove(member Member) Members {
	idx := -1
	for i, m := range ms {
		if m.ConsumerId == member.ConsumerId {
			idx = i
		}
	}
	return append(ms[:idx], ms[idx+1:]...)
}

func (ms Members) RemoveById(mid string) Members {
	idx := -1
	for i, m := range ms {
		if m.ConsumerId == mid {
			idx = i
		}
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
	removed := make([]Member, ms.Len()-members.Len())
	idx := 0
	for _, m := range ms {
		if !members.Contains(m.ConsumerId) {
			removed[idx] = m
			idx++
		}
	}
	return removed
}
