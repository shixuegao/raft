package server

import (
	"errors"
	"sort"
	"sxg/raft/v3/connector"
	"sxg/raft/v3/event"
	"sxg/raft/v3/protocol"
	"sxg/raft/v3/util"
	"sync"
	"time"
)

type voteBox struct {
	lock   *sync.Mutex
	box    map[uint64]byte
	quorum int
}

func (vb *voteBox) record(id uint64, ok bool) {
	if ok {
		vb.lock.Lock()
		defer vb.lock.Unlock()
		vb.box[id] = 1
	}
}

func (vb *voteBox) isWin() bool {
	vb.lock.Lock()
	defer vb.lock.Unlock()
	count := len(vb.box)
	return count >= vb.quorum
}

func newVoteBox(candidateID uint64, quorum int) *voteBox {
	return &voteBox{
		lock:   new(sync.Mutex),
		box:    map[uint64]byte{candidateID: 1},
		quorum: quorum,
	}
}

func (s *Server) quorum() int {
	return (1+len(s.config.Cluster))/2 + 1
}

func (s *Server) loop() {
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
			switch s.Role() {
			case Follower:
				s.loopFollower()
			case Candidate:
				s.loopCandidate()
			case Leader:
				s.loopLeader()
			}
		}
	}()
	<-c
}

func (s *Server) loopFollower() {
	randTimer := util.NewRandTimer(150, 150, time.Millisecond) //150ms ~ 300ms
	defer randTimer.Stop()

	for {
		select {
		case <-s.context.Done():
			return
		case <-randTimer.RawC():
			s.SetRole(Candidate)
			return
		case e, ok := <-s.eventQueue.RawChan():
			if !ok {
				return
			}
			if e.Type == event.TypeRemote {
				wrapper := e.Data.(*protocol.Wrapper)
				switch wrapper.RequestType {
				case protocol.TypeEntry:
					if s.handleEntryOnFollower(wrapper, e.Handler) {
						randTimer.Reset()
					}
				case protocol.TypeVote:
					if s.handleVoteOnFollower(wrapper, e.Handler) {
						randTimer.Reset()
					}
				default:
				}
			} else if e.Type == event.TypeClient {
				s.handleClientReq(e)
			}
		}
	}
}

func (s *Server) handleEntryOnFollower(wrapper *protocol.Wrapper, handler interface{}) bool {
	term := s.Term()
	entry := wrapper.GetEntry()
	conn := handler.(*connector.Connector)
	if term > entry.Term {
		resp := protocol.NewEntryResp(term, 0, protocol.Failure)
		s.sendResp(conn, resp)
		return false
	} else if term == entry.Term {
		logEntries, err := s.funcBytesToLogEntries(entry.Entries)
		if err != nil {
			s.logger.Warnf("接收来自Leader[%v]的Entry解析日志失败-->%v", conn.Remote(), err.Error())
			resp := protocol.NewEntryResp(term, entry.PrevLogIndex, protocol.Failure)
			s.sendResp(conn, resp)
		}
		index, err := s.logManager.AppendLogEntriesByFollower(entry.PrevLogIndex, entry.Term, logEntries)
		if err != nil {
			s.logger.Warnf("扩充来自Leader[%v]的日志失败-->%v", conn.Remote(), err.Error())
			resp := protocol.NewEntryResp(term, index, protocol.Failure)
			s.sendResp(conn, resp)
		}
		//update commit index
		s.logManager.UpdateCommitIndex(entry.LeaderCommit)
		return true
	} else {
		s.ConvertToFollower(entry.Term, entry.LeaderID)
		//与Leader的Term不比配, 返回Failure通知Leader
		resp := protocol.NewEntryResp(term, 0, protocol.Failure)
		s.sendResp(conn, resp)
		return true
	}
}

func (s *Server) handleVoteOnFollower(wrapper *protocol.Wrapper, handler interface{}) bool {
	term := s.Term()
	vote := wrapper.GetVote()
	conn := handler.(*connector.Connector)
	if term > vote.Term {
		resp := protocol.NewVoteResp(term, protocol.Disagree)
		s.sendResp(conn, resp)
	} else if term == vote.Term {
		if s.VoteFor() != 0 &&
			s.VoteFor() != vote.CandidateID {
			resp := protocol.NewVoteResp(term, protocol.Disagree)
			s.sendResp(conn, resp)
			return false
		}
		if s.logManager.IsLocalUpToDate(vote.LastLogIndex, vote.LastLogTerm) {
			resp := protocol.NewVoteResp(term, protocol.Disagree)
			s.sendResp(conn, resp)
			return false
		}
		s.SetVoteFor(vote.CandidateID)
		resp := protocol.NewVoteResp(term, protocol.Granted)
		s.sendResp(conn, resp)
	} else {
		if s.logManager.IsLocalUpToDate(vote.LastLogIndex, vote.LastLogTerm) {
			resp := protocol.NewVoteResp(term, protocol.Disagree)
			s.sendResp(conn, resp)
			return false
		}
		s.ConvertToFollower(vote.Term, vote.CandidateID)
		resp := protocol.NewVoteResp(vote.Term, protocol.Granted)
		s.sendResp(conn, resp)
	}
	return true
}

func (s *Server) loopCandidate() {
	randTimer := util.NewRandTimer(150, 150, time.Millisecond) //150ms ~ 300ms
	defer randTimer.Stop()

launchVote:
	for {
		randTimer.Reset()
		term := s.LaunchVote(s.config.Local.ID)
		voteBox := newVoteBox(s.config.Local.ID, s.quorum())

		s.peers.foreach(func(peer *Peer) bool {
			peer.vote(s, term, func(id uint64, resp *protocol.VoteResp) {
				voteBox.record(id, resp.VoteGranted == protocol.Granted)
			})
			return true
		})

		for {
			if voteBox.isWin() {
				s.SetRole(Leader)
				return
			}
			select {
			case <-s.context.Done():
				return
			case <-randTimer.RawC():
				goto launchVote
			case e, ok := <-s.eventQueue.RawChan():
				if !ok {
					return
				}
				if e.Type == event.TypeRemote {
					wrapper := e.Data.(*protocol.Wrapper)
					switch wrapper.RequestType {
					case protocol.TypeEntry:
						if s.handleEntryOnCandidate(wrapper, e.Handler) {
							return
						}
					case protocol.TypeVote:
						if s.handleVoteOnCandidate(wrapper, e.Handler) {
							return
						}
					default:
					}
				} else if e.Type == event.TypeClient {
					s.handleClientReq(e)
				}
			default:
			}
		}
	}
}

func (s *Server) handleEntryOnCandidate(wrapper *protocol.Wrapper, handler interface{}) bool {
	term := s.Term()
	entry := wrapper.GetEntry()
	conn := handler.(*connector.Connector)
	if term > entry.Term {
		resp := protocol.NewEntryResp(term, 0, protocol.Failure)
		s.sendResp(conn, resp)
	} else if term == entry.Term {
		s.ConvertToFollower(term, entry.LeaderID)
		resp := protocol.NewEntryResp(term, entry.PrevLogIndex, protocol.Failure)
		s.sendResp(conn, resp)
		return true
	} else {
		s.ConvertToFollower(entry.Term, entry.LeaderID)
		resp := protocol.NewEntryResp(term, 0, protocol.Failure)
		s.sendResp(conn, resp)
		return true
	}
	return false
}

func (s *Server) handleVoteOnCandidate(wrapper *protocol.Wrapper, handler interface{}) bool {
	term := s.Term()
	vote := wrapper.GetVote()
	conn := handler.(*connector.Connector)
	if term >= vote.Term {
		resp := protocol.NewVoteResp(term, protocol.Disagree)
		s.sendResp(conn, resp)
	} else {
		if s.logManager.IsLocalUpToDate(vote.LastLogIndex, vote.LastLogTerm) {
			resp := protocol.NewVoteResp(term, protocol.Disagree)
			s.sendResp(conn, resp)
		} else {
			s.ConvertToFollower(vote.Term, vote.CandidateID)
			resp := protocol.NewVoteResp(term, protocol.Granted)
			s.sendResp(conn, resp)
			return true
		}
	}
	return false
}

func (s *Server) loopLeader() {
	s.peerConvert(stateSend)
	defer s.peerConvert(stateWait)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.context.Done():
			return
		case e, ok := <-s.eventQueue.RawChan():
			if !ok {
				return
			}
			if e.Type == event.TypeRemote {
				wrapper := e.Data.(*protocol.Wrapper)
				switch wrapper.RequestType {
				case protocol.TypeEntry:
					if s.handleEntryOnLeader(wrapper, e.Handler) {
						return
					}
				case protocol.TypeVote:
					if s.handleVoteOnLeader(wrapper, e.Handler) {
						return
					}
				default:
				}
			} else if e.Type == event.TypeClient {
				s.handleClientReq(e)
			}
		default:
		}
		//refresh commitIndex
		select {
		case <-ticker.C:
			s.updateCommitIndex()
		default:
		}
	}
}

func (s *Server) handleEntryOnLeader(wrapper *protocol.Wrapper, handler interface{}) bool {
	term := s.Term()
	entry := wrapper.GetEntry()
	conn := handler.(*connector.Connector)
	if term > entry.Term {
		resp := protocol.NewEntryResp(term, 0, protocol.Failure)
		s.sendResp(conn, resp)
	} else if term == entry.Term {
		resp := protocol.NewEntryResp(term, entry.PrevLogIndex, protocol.Failure)
		s.logger.Warnf("收到来自远端[%v]的Entry信息, 可能已发生脑裂", conn.Remote())
		s.sendResp(conn, resp)
	} else {
		s.ConvertToFollower(entry.Term, entry.LeaderID)
		resp := protocol.NewEntryResp(term, 0, protocol.Failure)
		s.sendResp(conn, resp)
		return true
	}
	return false
}

func (s *Server) handleVoteOnLeader(wrapper *protocol.Wrapper, handler interface{}) bool {
	term := s.Term()
	vote := wrapper.GetVote()
	conn := handler.(*connector.Connector)
	if term >= vote.Term {
		resp := protocol.NewVoteResp(term, protocol.Disagree)
		s.sendResp(conn, resp)
	} else {
		if s.logManager.IsLocalUpToDate(vote.LastLogIndex, vote.LastLogTerm) {
			resp := protocol.NewVoteResp(term, protocol.Disagree)
			s.sendResp(conn, resp)
		} else {
			s.ConvertToFollower(vote.Term, vote.CandidateID)
			resp := protocol.NewVoteResp(term, protocol.Granted)
			s.sendResp(conn, resp)
			return true
		}
	}
	return false
}

func (s *Server) handleClientReq(e *event.Event) {
	handler := e.Handler.(event.ClientHandler)
	if s.Role() != Leader {
		err := errors.New("当前节点角色不是Leader, 无法处理Client请求")
		handler(false, err)
	} else {
		//添加日志
		data := e.Data.(event.ClientData)
		logEntry := s.funcNewLogEntry(data.ToBytes())
		if index, err := s.logManager.AppendLogEntryByLeader(s.Term(), logEntry); err != nil {
			handler(false, err)
		} else {
			s.notifyClient.Store(index, handler)
		}
	}
}

func (s *Server) updateCommitIndex() {
	count := 0
	slice := make([]uint64, len(s.config.Cluster))

	s.peers.foreach(func(peer *Peer) bool {
		slice[count] = peer.currentMatchedIndex()
		count++
		return true
	})
	//sort
	sort.Sort(sort.Reverse(util.UintSlice(slice)))
	s.logManager.UpdateCommitIndex(slice[len(slice)/2])
}

func (s *Server) peerConvert(state byte) {
	s.peers.foreach(func(peer *Peer) bool {
		peer.convert(state)
		return true
	})
}

func (s *Server) sendResp(conn *connector.Connector, resp protocol.WrapperData) {
	respType := ""
	switch resp.(type) {
	case *protocol.EntryResp:
		respType = "EntryResp"
	case *protocol.VoteResp:
		respType = "VoteResp"
	}
	if _, _, err := conn.Send(resp); err != nil {
		s.logger.Errorf("处于角色%v下, 发送%v到远端[%v]失败-->%v", roleDescribe(int(s.Role())), respType, conn.Remote(), err.Error())
	}
}
