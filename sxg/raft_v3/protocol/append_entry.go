package protocol

import (
	"encoding/binary"
)

type Entry struct {
	Term         uint64
	LeaderID     uint64
	PrevLogIndex uint64
	PrevLogTerm  uint64
	LeaderCommit uint64
	EntriesLen   uint32
	Entries      []byte
}

func (e *Entry) ToBytes() []byte {
	fixed := 5*8 + 4
	bsFixed := make([]byte, fixed)
	size := 0
	binary.BigEndian.PutUint64(bsFixed[size:size+8], e.Term)
	size += 8
	binary.BigEndian.PutUint64(bsFixed[size:size+8], e.LeaderID)
	size += 8
	binary.BigEndian.PutUint64(bsFixed[size:size+8], e.PrevLogIndex)
	size += 8
	binary.BigEndian.PutUint64(bsFixed[size:size+8], e.PrevLogTerm)
	size += 8
	binary.BigEndian.PutUint64(bsFixed[size:size+8], e.LeaderCommit)
	size += 8
	if len(e.Entries) <= 0 {
		e.EntriesLen = 0
		binary.BigEndian.PutUint32(bsFixed[size:size+4], e.EntriesLen)
		return bsFixed
	} else {
		e.EntriesLen = uint32(len(e.Entries))
		binary.BigEndian.PutUint32(bsFixed[size:size+4], e.EntriesLen)
	}
	return append(bsFixed, e.Entries...)
}

type EntryResp struct {
	Term    uint64
	Index   uint64
	Success byte
}

func (ee *EntryResp) ToBytes() []byte {
	total := 2*8 + 1
	bs := make([]byte, total)
	size := 0
	binary.BigEndian.PutUint64(bs[size:size+8], ee.Term)
	size += 8
	binary.BigEndian.PutUint64(bs[size:size+8], ee.Index)
	size += 8
	bs[size] = ee.Success
	return bs
}

func NewEntryResp(term, index uint64, success byte) *EntryResp {
	return &EntryResp{Term: term, Index: index, Success: success}
}
