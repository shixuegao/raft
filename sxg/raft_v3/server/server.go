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

type Server struct {
	config                *config.Config
	context               context.Context
	cancel                context.CancelFunc
	role                  int32
	term                  uint64
	votedFor              uint64
	listener              *net.TCPListener
	peers                 *sync.Map
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
	s.loop()
	s.apply()
	s.notify()
}

func (s *Server) Close() {
	s.cancel()
	s.listener.Close()
	s.eventQueue.Close()
	s.peers.Range(func(key, value interface{}) bool {
		(value.(*Peer)).stop()
		return true
	})
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
		ok, id := s.preHandleConnector(remoteAddr, connector)
		if !ok {
			return
		}
		connector.SetRemoteID(id)
		for {
			select {
			case <-s.context.Done():
				return
			default:
			}
			wrapper, err := connector.Read()
			if err != nil {
				if err == io.EOF {
					s.logger.Infof("远端[Addr: %v, ID: %v]连接已关闭", remoteAddr, id)
				} else {
					s.logger.Warnf("从远端[Addr: %v, ID: %v]读取信息异常-->%v", remoteAddr, id, err.Error())
				}
				return
			}
			event := event.NewRemoteEvent(wrapper, connector)
			s.eventQueue.PutSync(event)
		}
	}()
}

func (s *Server) preHandleConnector(remoteAddr string, connector *connector.Connector) (bool, uint64) {
	//recv basic
	wrapper, err := connector.Read()
	if err != nil {
		s.logger.Warnf("从远端[Addr: %v]读取Basic信息异常-->%v", remoteAddr, err.Error())
		return false, 0
	}
	if wrapper.RequestType != protocol.TypeBasic {
		s.logger.Warnf("从远端[Addr: %v]TCP第一次读取的数据不为Basic信息", remoteAddr)
		return false, 0
	}
	basic := wrapper.GetBasic()
	_, ok := s.peers.Load(basic.ID)
	if !ok {
		s.logger.Warnf("远端连接[Addr: %v]所代表的[ID: %v]不存在", remoteAddr, basic.ID)
		err = sendBasicResp(connector, protocol.Failure)
		if err != nil {
			s.logger.Warnf("回送Basic应答信息到远端[Addr: %v]异常-->%v", remoteAddr, err.Error())
		}
		return false, 0
	}
	//send basic resp
	err = sendBasicResp(connector, protocol.Success)
	if err != nil {
		s.logger.Warnf("回送Basic应答信息到远端[Addr: %v]异常-->%v", remoteAddr, err.Error())
		return false, 0
	}
	return true, basic.ID
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

func (s *Server) currentTerm() uint64 {
	return atomic.LoadUint64(&s.term)
}

func (s *Server) increaseTerm() uint64 {
	return atomic.AddUint64(&s.term, 1)
}

func (s *Server) setTerm(term uint64) {
	atomic.StoreUint64(&s.term, term)
}

func (s *Server) currentRole() int {
	return int(atomic.LoadInt32(&s.role))
}

func (s *Server) convertToRole(role int32) {
	atomic.StoreInt32(&s.role, role)
}

func (s *Server) voteFor(id uint64) {
	atomic.StoreUint64(&s.votedFor, id)
}

func NewServer(config *config.Config, logManager *log.LogManager,
	stateMachine *statemachine.StateMachine, logger logI.Logger,
	f1 FuncBytesToLogEntries, f2 FuncNewLogEntry) (*Server, error) {
	server := Server{
		config:                config,
		logManager:            logManager,
		stateMachine:          stateMachine,
		wait:                  new(sync.WaitGroup),
		votedFor:              0,
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
	_, server.term = logManager.LastInfo()
	//peers
	server.peers = new(sync.Map)
	for _, s := range config.Cluster {
		peer := NewPeer(config.Local.ID, s.ID, s.Addr, &server)
		server.peers.Store(s.ID, peer)
		peer.start()
	}
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

func sendBasicResp(connector *connector.Connector, success byte) error {
	resp := protocol.NewBasicResp(success)
	if _, _, err := connector.Send(resp); err != nil {
		return err
	}
	return nil
}
