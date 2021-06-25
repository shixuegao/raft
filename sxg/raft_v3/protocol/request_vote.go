package protocol

import (
	"encoding/binary"
)

type Vote struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

func (v *Vote) ToBytes() []byte {
	total := 4 * 8
	bs := make([]byte, total)
	size := 0
	binary.BigEndian.PutUint64(bs[size:size+8], v.Term)
	size += 8
	binary.BigEndian.PutUint64(bs[size:size+8], v.CandidateID)
	size += 8
	binary.BigEndian.PutUint64(bs[size:size+8], v.LastLogIndex)
	size += 8
	binary.BigEndian.PutUint64(bs[size:size+8], v.LastLogTerm)
	return bs
}

type VoteResp struct {
	Term        uint64
	VoteGranted uint8
}

func (vr *VoteResp) ToBytes() []byte {
	bs := make([]byte, 9)
	binary.BigEndian.PutUint64(bs[:8], uint64(vr.Term))
	bs[8] = vr.VoteGranted
	return bs
}

func NewVote(id, term, lastLogIndex, lastLogTerm uint64) *Vote {
	return &Vote{
		Term:         term,
		CandidateID:  id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func NewVoteResp(term uint64, votedGranted byte) *VoteResp {
	return &VoteResp{Term: term, VoteGranted: votedGranted}
}
