package raftv3

import (
	"encoding/binary"
	"fmt"
	"sxg/raft/v3/exception"
	"sxg/raft/v3/log"
)

type RaftLog struct {
	LogTerm  uint64
	LogIndex uint64
	DataLen  uint32
	Data     []byte
}

func (rl *RaftLog) ToBytes() []byte {
	fixed := 2*8 + 4
	bs := make([]byte, fixed)
	size := 0
	binary.BigEndian.PutUint64(bs[size:size+8], rl.LogTerm)
	size += 8
	binary.BigEndian.PutUint64(bs[size:size+8], rl.LogIndex)
	size += 8
	rl.DataLen = uint32(len(rl.Data))
	if rl.DataLen == 0 {
		binary.BigEndian.PutUint32(bs[size:size+4], rl.DataLen)
	} else {
		binary.BigEndian.PutUint32(bs[size:size+4], rl.DataLen)
		size += 4
		bs = append(bs[size:], rl.Data...)
	}
	return bs
}

func (rl *RaftLog) Term() uint64 {
	return rl.LogTerm
}

func (rl *RaftLog) Index() uint64 {
	return rl.LogIndex
}

func (rl *RaftLog) TermAndIndex() (uint64, uint64) {
	return rl.LogTerm, rl.LogIndex
}

func (rl *RaftLog) String() string {
	return fmt.Sprintf("Index: %v, Term: %v", rl.LogIndex, rl.LogTerm)
}

func (rl *RaftLog) SetTerm(term uint64) {
	rl.LogTerm = term
}

func (rl *RaftLog) SetIndex(index uint64) {
	rl.LogIndex = index
}

func (rl *RaftLog) SetTermAndIndex(term, index uint64) {
	rl.LogTerm = term
	rl.LogIndex = index
}

func NewRaftLog(p []byte) log.LogEntry {
	rl := &RaftLog{
		Data: p,
	}
	return rl
}

func BytesToRaftLogs(p []byte) ([]log.LogEntry, error) {
	logEntries := make([]log.LogEntry, 0)
	size := len(p)
	for {
		if size <= 0 {
			break
		}
		rLog := RaftLog{}
		ok := false
		offset := 0
		//log term
		if ok, size = _assertLeftSize(size, 8); !ok {
			return nil, exception.ErrShortLogData
		} else {
			rLog.LogTerm = binary.BigEndian.Uint64(p[offset : offset+8])
			offset += 8
		}
		//log index
		if ok, size = _assertLeftSize(size, 8); !ok {
			return nil, exception.ErrShortLogData
		} else {
			rLog.LogIndex = binary.BigEndian.Uint64(p[offset : offset+8])
			offset += 8
		}
		//Data Len
		if ok, size = _assertLeftSize(size, 4); !ok {
			return nil, exception.ErrShortLogData
		} else {
			rLog.DataLen = binary.BigEndian.Uint32(p[offset : offset+4])
			offset += 4
		}
		//Data
		if ok, size = _assertLeftSize(size, int(rLog.DataLen)); !ok {
			return nil, exception.ErrShortLogData
		} else {
			data := make([]byte, rLog.DataLen)
			copy(data, p[offset:offset+int(rLog.DataLen)])
			rLog.Data = data
			offset += int(rLog.DataLen)
		}
		logEntries = append(logEntries, &rLog)
	}
	return logEntries, nil
}

func _assertLeftSize(size, need int) (bool, int) {
	if size < need {
		return false, 0
	}
	return true, size - need
}
