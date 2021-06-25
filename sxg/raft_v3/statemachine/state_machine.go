package statemachine

import "sxg/raft/v3/log"

type Provider interface {
	Init(index uint64, term uint64)
	GetUnAppliedLogEntriesAfter(index uint64) []log.LogEntry
	EchoAppliedLogEntries(appliedIndex uint64)
}

type FuncInit func(*StateMachine) error
type FuncApply func(*StateMachine, []log.LogEntry) (index uint64, term uint64, err error)

type StateMachine struct {
	prevLogIndex uint64
	prevLogTerm  uint64
	provider     Provider
	funcInit     FuncInit
	funcApply    FuncApply
}

func (sm *StateMachine) Init(provider Provider) error {
	if err := sm.funcInit(sm); err != nil {
		return err
	}
	provider.Init(sm.prevLogIndex, sm.prevLogTerm)
	return nil
}

func (sm *StateMachine) Applying() (int, error) {
	logEntries := sm.provider.GetUnAppliedLogEntriesAfter(sm.prevLogIndex)
	if len(logEntries) <= 0 {
		return 0, nil
	}
	index, term, err := sm.funcApply(sm, logEntries)
	if err != nil {
		return 0, err
	}
	prevLogIndex := sm.prevLogIndex
	sm.prevLogIndex = index
	sm.prevLogTerm = term
	sm.provider.EchoAppliedLogEntries(sm.prevLogIndex)
	return int(index - prevLogIndex), nil
}

func NewStateMachine(f1 FuncInit, f2 FuncApply) *StateMachine {
	machine := StateMachine{
		prevLogIndex: 0,
		prevLogTerm:  0,
		funcInit:     f1,
		funcApply:    f2,
	}
	return &machine
}
