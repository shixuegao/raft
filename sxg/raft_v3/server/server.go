package server

import (
	"context"
	"io"
	"net"
	logI "sxg/logger"
	"sxg/raft/v3/config"
	"sxg/raft/v3/connector"
	"sxg/raft/v3/event"
	"sxg/raft/v3/log"
	"sxg/raft/v3/protocol"
	"sxg/raft/v3/statemachine"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Follower = iota
	Candidate
	Leader
)

func roleDescribe(role int) string {
	switch role {
	case Leader:
		return "Leader"
	case Candidate:
		return "Candidate"
	default:
		return "Follower"
	}
}

type FuncBytesToLogEntries func(p []byte) ([]log.LogEntry, error)
type FuncNewLogEntry func(data []byte) log.LogEntry

type VoteBox struct {
	role    int32
	term    uint64
	voteFor uint64
}

func (vb *VoteBox) Term() uint64 {
	return atomic.LoadUint64(&vb.term)
}

func (vb *VoteBox) SetTerm(term uint64) {
	if term >= vb.term {
		atomic.StoreUint64(&vb.term, term)
	}
}

func (vb *VoteBox) Role() int32 {
	return atomic.LoadInt32(&vb.role)
}

func (vb *VoteBox) SetRole(role int32) {
	atomic.StoreInt32(&vb.role, role)
}

func (vb *VoteBox) VoteFor() uint64 {
	return atomic.LoadUint64(&vb.voteFor)
}

func (vb *VoteBox) SetVoteFor(id uint64) {
	atomic.StoreUint64(&vb.voteFor, id)
}

func (vb *VoteBox) ConvertToFollower(term, id uint64) {
	atomic.StoreInt32(&vb.role, Follower)
	atomic.StoreUint64(&vb.term, term)
	atomic.StoreUint64(&vb.voteFor, id)
}

func (vb *VoteBox) LaunchVote(id uint64) uint64 {
	atomic.StoreUint64(&vb.voteFor, id)
	return atomic.AddUint64(&vb.term, 1)
}

func NewVoteBox() *VoteBox {
	return &VoteBox{role: Follower}
}

type Server struct {
	*VoteBox
	config                *config.Config
	context               context.Context
	cancel                context.CancelFunc
	listener              *net.TCPListener
	peers                 *Peers
	logManager            *log.LogManager
	stateMachine          *statemachine.StateMachine
	wait                  *sync.WaitGroup
	logger                logI.Logger
	eventQueue            *event.EventQueue
	funcBytesToLogEntries FuncBytesToLogEntries
	funcNewLogEntry       FuncNewLogEntry
	notifyClient          *sync.Map
}

func (s *Server) Open() {
	s.listen()
	s.peers.start()
	s.loop()
	s.apply()
	s.notify()
}

func (s *Server) Close() {
	s.cancel()
	s.listener.Close()
	s.eventQueue.Close()
	s.peers.stop()
	s.wait.Wait()
}

func (s *Server) EventQueue() *event.EventQueue {
	return s.eventQueue
}

func (s *Server) listen() {
	c := make(chan byte)
	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		c <- 0

		for {
			conn, err := s.listener.AcceptTCP()
			if err != nil {
				s.logger.Errorf("TCP监听异常-->%s", err.Error())
				break
			}
			s.handleConn(conn)
		}
	}()
	<-c
}

func (s *Server) handleConn(conn *net.TCPConn) {
	s.wait.Add(1)
	go func() {
		defer s.wait.Done()
		remoteAddr := conn.RemoteAddr().String()
		connector := connector.NewConnector1(0, conn, s.config.MaxEntrySize, s.readTimeout())
		defer connector.CloseConn()
		for {
			select {
			case <-s.context.Done():
				return
			default:
			}
			wrapper, err := connector.Read()
			if err != nil {
				if err == io.EOF {
					s.logger.Infof("远端[Addr: %v, ID: %v]连接已关闭", remoteAddr, connector.RemoteID())
				} else {
					s.logger.Warnf("从远端[Addr: %v, ID: %v]读取信息异常-->%v", remoteAddr, connector.RemoteID(), err.Error())
				}
				return
			}
			if connector.RemoteID() == 0 {
				var id uint64 = 0
				switch wrapper.RequestType {
				case protocol.TypeEntry:
					id = wrapper.GetEntry().LeaderID
				case protocol.TypeVote:
					id = wrapper.GetVote().CandidateID
				}
				if id != 0 && !s.peers.isPeerMember(id) {
					s.logger.Warnf("收到远端[Addr: %v, ID: %v]的请求, 但该远端不在列表内", remoteAddr, id)
					return
				}
				connector.SetRemoteID(id)
			}
			event := event.NewRemoteEvent(wrapper, connector)
			s.eventQueue.PutSync(event)
		}
	}()
}

func (s *Server) readTimeout() time.Duration {
	return time.Duration(s.config.ReadTimeout) * time.Millisecond
}

func (s *Server) heartbeatInterval() time.Duration {
	return time.Duration(s.config.HeartbeatInterval) * time.Millisecond
}

func (s *Server) voteInterval() time.Duration {
	return time.Duration(s.config.VoteInterval) * time.Millisecond
}

func NewServer(config *config.Config, logManager *log.LogManager,
	stateMachine *statemachine.StateMachine, logger logI.Logger,
	f1 FuncBytesToLogEntries, f2 FuncNewLogEntry) (*Server, error) {
	server := Server{
		VoteBox:               NewVoteBox(),
		config:                config,
		logManager:            logManager,
		stateMachine:          stateMachine,
		wait:                  new(sync.WaitGroup),
		logger:                logger,
		eventQueue:            event.NewEventQueue(config.MaxEventSize),
		funcBytesToLogEntries: f1,
		funcNewLogEntry:       f2,
		notifyClient:          new(sync.Map),
	}
	//context
	server.context, server.cancel = context.WithCancel(context.Background())
	//listener
	if listener, err := newTcpListener(config.Local.Addr); err != nil {
		return nil, err
	} else {
		server.listener = listener
	}
	//Term
	_, term := logManager.LastInfo()
	server.SetTerm(term)
	//peers
	server.peers = NewPeers(config, &server)
	return &server, nil
}

func newTcpListener(addr string) (*net.TCPListener, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return nil, err
	}
	return tcpListener, nil
}
