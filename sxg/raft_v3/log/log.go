package log

import (
	"fmt"
	"sync"
)

const (
	DefaultAppendSize = 1024
)

type LogEntry interface {
	ToBytes() []byte
	Term() uint64
	Index() uint64
	TermAndIndex() (uint64, uint64)
	String() string
	SetTerm(uint64)
	SetIndex(uint64)
	SetTermAndIndex(uint64, uint64)
}

//preIndex <= appliedIndex <= commitIndex <= lastIndex
type LogManager struct {
	size          int
	appendSize    int
	maxHandleSize int
	prevIndex     uint64
	appliedIndex  uint64
	commitIndex   uint64
	lastIndex     uint64
	prevTerm      uint64
	lock          *sync.RWMutex
	entries       []LogEntry
}

//grow the size of entries
func (lm *LogManager) grow(size int) {
	capacity := len(lm.entries)
	if lm.size+size >= capacity {
		offset := lm.size + size - capacity
		count := offset / lm.appendSize
		if offset%lm.appendSize > 0 {
			count++
		}
		entries := make([]LogEntry, capacity+lm.appendSize*count)
		copy(entries[:], lm.entries[:lm.size])
		lm.entries = entries
	}
}

func (lm *LogManager) AppendLogEntryByLeader(term uint64, entry LogEntry) (uint64, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	lm.grow(1)
	index := lm.lastIndex + 1
	entry.SetTermAndIndex(term, index)
	lm.entries[lm.size] = entry
	lm.size += 1
	lm.lastIndex = index
	return index, nil
}

//注: 一切以Index为基准, 保证Index是递增的
func (lm *LogManager) AppendLogEntriesByFollower(prevIndex, prevTerm uint64, entries []LogEntry) (uint64, error) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if prevIndex < lm.commitIndex {
		return lm.commitIndex, fmt.Errorf("无法从CommitIndex[%v]之前进行日志扩充", lm.commitIndex)
	}
	if prevIndex > lm.lastIndex {
		return lm.lastIndex, fmt.Errorf("不存在日志索引[%v]", prevIndex)
	}
	if prevIndex == lm.prevIndex {
		//本地数组无数据
		if prevTerm != lm.prevTerm {
			return prevIndex, fmt.Errorf("索引[%v]的任期不匹配[%v]", prevIndex, prevTerm)
		}
		if err := lm.validateEntries(prevIndex, prevTerm, entries); err != nil {
			return prevIndex, err
		}
	} else {
		target := lm.logEntryAt(prevIndex)
		if prevTerm != target.Term() {
			return prevIndex, fmt.Errorf("索引[%v]的任期不匹配[%v]", prevIndex, prevTerm)
		}
		if err := lm.validateEntries(target.Index(), target.Term(), entries); err != nil {
			return prevIndex, err
		}
	}
	//扩充日志
	removeSize := int(lm.lastIndex - prevIndex)
	lm.size -= removeSize
	appendSize := len(entries)
	lm.grow(appendSize)
	copy(lm.entries[lm.size:lm.size+appendSize], entries)
	lm.size += appendSize
	lm.lastIndex = entries[appendSize-1].Index()
	return lm.lastIndex, nil
}

//1.索引必须连续
//2.待添加日志任期必须保持一致, 且大于prevTerm
func (lm *LogManager) validateEntries(prevIndex, prevTerm uint64, entries []LogEntry) error {
	if len(entries) <= 0 {
		return nil
	}
	if entries[0].Term() < prevTerm {
		return fmt.Errorf("日志任期[%v]小于前面任期[%v]", entries[0].Term(), prevTerm)
	}
	for i := 0; i < len(entries); i++ {
		entry := entries[i]
		if entry.Index() != prevIndex+1 {
			return fmt.Errorf("第%v个的日志索引[%v]不是前一个索引[%v]+1", i+1, entry.Index(), prevIndex)
		}
		prevIndex = entry.Index()
	}
	return nil
}

func (lm *LogManager) logEntryAt(index uint64) LogEntry {
	if index <= lm.prevIndex || index > lm.lastIndex {
		return nil
	}
	aIndex := lm.size - (int(lm.lastIndex - index)) - 1
	return lm.entries[aIndex]
}

func (lm *LogManager) GetLogEntriesAfter(index uint64) (uint64, []LogEntry) {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	if index < lm.prevIndex ||
		index >= lm.lastIndex {
		return 0, nil
	}
	size := int(lm.lastIndex - index)
	if size > lm.maxHandleSize {
		size = lm.maxHandleSize
	}
	aIndex := int(index - lm.prevIndex)
	entry := lm.entries[aIndex]
	return entry.Term(), deepCopyEntries(lm.entries[aIndex : aIndex+size])
}

func (lm *LogManager) LastInfo() (uint64, uint64) {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	if lm.size <= 0 {
		return lm.prevIndex, lm.prevTerm
	}
	entry := lm.entries[lm.size-1]
	return entry.Index(), entry.Term()
}

func (lm *LogManager) CommitIndex() uint64 {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	return lm.commitIndex
}

func (lm *LogManager) AppliedIndex() uint64 {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	return lm.appliedIndex
}
func (lm *LogManager) UpdateCommitIndex(index uint64) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if index <= lm.commitIndex {
		return
	}
	if index > lm.lastIndex {
		lm.commitIndex = lm.lastIndex
	} else {
		lm.commitIndex = index
	}
}

func (lm *LogManager) InitPrevTerm(term uint64) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if lm.prevIndex == 0 {
		lm.prevTerm = term
	}
}

func (lm *LogManager) LastIndexOfTerm(term uint64) (uint64, bool) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	minTerm := lm.prevTerm
	maxTerm := lm.prevTerm
	if lm.size > 0 {
		lEntry := lm.entries[lm.size-1]
		maxTerm = lEntry.Term()
	}
	if term < minTerm ||
		term > maxTerm {
		return 0, false
	}
	if lm.size > 0 {
		for i := lm.size - 1; i >= 0; i-- {
			entry := lm.entries[i]
			if entry.Term() == term {
				return entry.Index(), true
			}
		}
	}
	return lm.prevIndex, true
}

func (lm *LogManager) IsLocalUpToDate(index, term uint64) bool {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	lastIndex := lm.prevIndex
	lastTerm := lm.prevTerm
	if lm.size > 0 {
		lEntry := lm.entries[lm.size-1]
		lastIndex = lEntry.Index()
		lastTerm = lEntry.Term()
	}
	if lastTerm > term {
		return true
	} else if lastTerm < term {
		return false
	} else {
		return lastIndex > index
	}
}

//implementation of statemachine.Provider
func (lm *LogManager) Init(index uint64, term uint64) {
	lm.prevTerm = term
	lm.prevIndex = index
	lm.appliedIndex = index
	lm.commitIndex = index
	lm.lastIndex = index
}

//implementation of statemachine.Provider
func (lm *LogManager) GetUnAppliedLogEntriesAfter(index uint64) []LogEntry {
	lm.lock.RLock()
	defer lm.lock.RUnlock()
	if index < lm.appliedIndex ||
		index >= lm.commitIndex {
		return nil
	}
	size := int(lm.commitIndex - index)
	if size > lm.maxHandleSize {
		size = lm.maxHandleSize
	}
	aIndex := int(index - lm.prevIndex)
	return deepCopyEntries(lm.entries[aIndex : aIndex+size])
}

//implementation of statemachine.Provider
func (lm *LogManager) EchoAppliedLogEntries(appliedIndex uint64) {
	lm.lock.Lock()
	defer lm.lock.Unlock()
	if appliedIndex <= lm.appliedIndex ||
		lm.appliedIndex > lm.commitIndex {
		return
	}
	lm.appliedIndex = appliedIndex
}

func NewLogManager(appendSize, maxHandleSize int) *LogManager {
	return &LogManager{
		appendSize:    appendSize,
		maxHandleSize: maxHandleSize,
		lock:          new(sync.RWMutex),
		entries:       make([]LogEntry, appendSize),
	}
}

func deepCopyEntries(src []LogEntry) []LogEntry {
	dst := make([]LogEntry, len(src))
	copy(dst[:], src[:])
	return dst
}
