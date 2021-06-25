package raftv3_test

import (
	raftv3 "sxg/raft/v3"
	"sxg/raft/v3/config"
	"sxg/raft/v3/log"
	record "sxg/raft/v3/logger"
	"sxg/raft/v3/server"
	"sxg/raft/v3/statemachine"
	"testing"
	"time"
)

func Test0(t *testing.T) {
	//logger
	logger := record.Logger
	defer record.Clear()
	//config
	filename := "config.yaml"
	conf, err := config.NewConfig(filename)
	if err != nil {
		logger.Errorf("读取Config文件[%v]失败-->%v", filename, err.Error())
		return
	}
	if err := conf.Validate(); err != nil {
		logger.Errorf("Config文件[%v]校验失败-->%v", filename, err.Error())
		return
	}
	//raft 创建流程
	//1 state machine
	stateMachine := statemachine.NewStateMachine(stateMachineInit, stateMachineApply)
	//2 log manager
	logManager := log.NewLogManager(conf.LogAppendSize, conf.MaxHandleSize)
	//3 init state machine
	if err := stateMachine.Init(logManager); err != nil {
		logger.Errorf("状态机初始化失败-->%v", err.Error())
		return
	}
	//4 server
	server, err := server.NewServer(conf, logManager, stateMachine, logger, raftv3.BytesToRaftLogs, raftv3.NewRaftLog)
	if err != nil {
		logger.Errorf("新建Server失败-->%v", err.Error())
	}
	server.Open()
	defer server.Close()
	time.Sleep(10 * time.Minute)
}

func stateMachineInit(sm *statemachine.StateMachine) error {
	return nil
}

func stateMachineApply(sm *statemachine.StateMachine, logEntries []log.LogEntry) (uint64, uint64, error) {
	return 0, 0, nil
}
