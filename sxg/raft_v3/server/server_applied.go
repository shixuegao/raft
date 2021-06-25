package server

import (
	"sxg/raft/v3/event"
	"time"
)

func (s *Server) apply() {
	c := make(chan byte)
	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		c <- 0

		for {
			select {
			case <-s.context.Done():
				return
			default:
			}
			n, err := s.stateMachine.Applying()
			if err != nil {
				s.logger.Errorf("Apply数据到状态机异常-->%s", err.Error())
				continue
			}
			if n <= 0 {
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}
	}()
	<-c
}

func (s *Server) notify() {
	c := make(chan byte)
	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		c <- 0

		var lastAppliedIndex uint64 = 0
		for {
			select {
			case <-s.context.Done():
				return
			default:
			}

			currentAppliedIndex := s.logManager.AppliedIndex()
			if currentAppliedIndex > lastAppliedIndex {
				processed := false
				s.notifyClient.Range(func(key, value interface{}) bool {
					index := key.(uint64)
					handler := value.(event.ClientHandler)
					if index <= currentAppliedIndex {
						handler(true, nil)
					}
					s.notifyClient.Delete(key)
					processed = true
					return true
				})
				if !processed {
					time.Sleep(10 * time.Millisecond)
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()
	<-c
}
