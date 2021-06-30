package server

import (
	"context"
	"fmt"
	"io"
	"sxg/raft/v3/config"
	"sxg/raft/v3/connector"
	"sxg/raft/v3/log"
	"sxg/raft/v3/protocol"
	"sxg/raft/v3/util"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateWait = iota
	stateSend
)

type Peers struct {
	lock  *sync.Mutex
	peers map[uint64]*Peer
}

func (p *Peers) start() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, v := range p.peers {
		v.start()
	}
}

func (p *Peers) stop() {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, v := range p.peers {
		v.stop()
	}
}

func (p *Peers) foreach(f func(peer *Peer) bool) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, v := range p.peers {
		if !f(v) {
			break
		}
	}
}

func (p *Peers) isPeerMember(id uint64) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, v := range p.peers {
		if v.peerID == id {
			return true
		}
	}
	return false
}

func NewPeers(config *config.Config, server *Server) *Peers {
	p := Peers{lock: new(sync.Mutex), peers: map[uint64]*Peer{}}
	for _, v := range config.Cluster {
		p.peers[v.ID] = NewPeer(config.Local.ID, v.ID, v.Addr, server)
	}
	return &p
}

//Peer works after leader on
type Peer struct {
	leaderID   uint64
	peerID     uint64
	remoteAddr string

	prevLogIndex uint64
	matchedIndex uint64

	state     byte
	stateChan chan byte
	conn      *connector.Connector
	ticker    *time.Ticker
	context   context.Context
	cancel    context.CancelFunc
	wait      *sync.WaitGroup
	server    *Server
}

func (p *Peer) peer() string {
	return fmt.Sprintf("Addr: %v, ID: %v", p.remoteAddr, p.peerID)
}

func (p *Peer) currentMatchedIndex() uint64 {
	return atomic.LoadUint64(&p.matchedIndex)
}

func (p *Peer) setMatchedIndex(index uint64) {
	atomic.StoreUint64(&p.matchedIndex, index)
}

func (p *Peer) start() {
	p.wait.Add(1)
	c := make(chan byte, 1)
	go func() {
		defer p.wait.Done()
		c <- 0
		p.ticker = time.NewTicker(p.server.heartbeatInterval())
		defer p.ticker.Stop()

		for {
			select {
			case <-p.context.Done():
				return
			case state := <-p.stateChan:
				if p.state != state {
					switch state {
					case stateSend:
						if p.resetConn(true) {
							p.state = state
							lastIndex, _ := p.server.logManager.LastInfo()
							p.prevLogIndex = lastIndex + 1
							p.setMatchedIndex(0)
							//立刻递送一个包
							p.intervalInteraction()
							p.ticker.Reset(p.server.heartbeatInterval())
						}
					default:
						p.state = state
						p.resetConn(false)
					}
				}
			case <-p.ticker.C:
				p.intervalInteraction()
			}
		}
	}()
	<-c
}

func (p *Peer) stop() {
	p.cancel()
	p.wait.Wait()
}

func (p *Peer) resetConn(open bool) bool {
	if p.conn != nil {
		err := p.conn.CloseConn()
		if err != nil {
			p.server.logger.Warnf("关闭与Peer[%v]的TCP连接异常-->%v", p.peer(), err.Error())
		}
		p.conn = nil
	}
	if open {
		conn, err := p.initConn(p.server.config.MaxEntrySize, p.server.readTimeout())
		if err != nil {
			p.server.logger.Errorf("建立与Peer[%v]的TCP连接失败-->%v", p.peer(), err.Error())
			return false
		}
		p.conn = conn
	}
	return true
}

func (p *Peer) convert(state byte) {
	p.stateChan <- state
}

func (p *Peer) vote(server *Server, term uint64,
	f func(id uint64, resp *protocol.VoteResp)) error {
	conn, err := p.initConn(0, server.readTimeout())
	if err != nil {
		return err
	}
	c := make(chan byte)
	go func() {
		defer func() {
			util.AssertRecoverErr()
		}()
		defer conn.CloseConn()
		c <- 0
		for {
			//重新选举或者角色转变则退出
			if server.Term() != term ||
				server.Role() != Candidate {
				break
			}
			lastIndex, lastTerm := server.logManager.LastInfo()
			vote := protocol.NewVote(p.leaderID, term, lastIndex, lastTerm)
			wrapper, err := conn.Exchange(vote)
			if err != nil {
				server.logger.Warnf("向Peer[%v]发送投票请求失败-->%v", p.peer(), err.Error())
				if err == io.EOF {
					return
				}
				//sleep
				time.Sleep(server.voteInterval())
				continue
			}
			f(p.peerID, wrapper.GetVoteResp())
			break
		}
	}()
	<-c
	return nil
}

func (p *Peer) initConn(maxEntrySize int, readTimeout time.Duration) (*connector.Connector, error) {
	conn, err := connector.NewConnector2(p.peerID, p.remoteAddr, maxEntrySize, readTimeout)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (p *Peer) intervalInteraction() {
	if p.state == stateWait ||
		p.server.Role() != Leader ||
		p.conn == nil {
		return
	}
	//from local cache
	prevLogTerm, logEntries := p.server.logManager.GetLogEntriesAfter(p.prevLogIndex)
	if len(logEntries) > 0 {
		entry := p.newEntry(prevLogTerm, logEntries)
		if wrapper, err := p.conn.Exchange(entry); err != nil {
			p.server.logger.Errorf("与Peer[%v]交互Entry信息失败-->%v", p.peer(), err.Error())
		} else {
			lastIndex := logEntries[len(logEntries)-1].Index()
			p.handleEntryResp(wrapper.GetEntryResp(), lastIndex)
		}
		return
	}
	//from snapshot
}

func (p *Peer) handleEntryResp(resp *protocol.EntryResp, lastIndex uint64) {
	term := p.server.Term()
	if term < resp.Term {
		//忽略, 等其他Header发送Entry来转变当前Server的状态
	} else if term == resp.Term {
		switch resp.Success {
		case protocol.Success:
			if resp.Index < p.prevLogIndex ||
				resp.Index > lastIndex {
				p.server.logger.Warnf("Peer[%v]扩充日志成功, 但回送的索引[%v]异常, 不在正确的索引范围[%v - %v]", p.peer(), resp.Index, p.prevLogIndex, lastIndex)
				return
			}
			p.prevLogIndex = resp.Index
			p.setMatchedIndex(resp.Index)
		case protocol.Failure:
			if resp.Index > p.prevLogIndex {
				p.server.logger.Warnf("Peer[%v]扩充日志失败, 但回送的索引异常, 大于上一次日志索引[%v]", p.peer(), resp.Index, p.prevLogIndex)
				return
			}
			p.prevLogIndex = resp.Index
		default:
		}
	} else {
		//寻找指定Term的最后索引, 并将preLogIndex置为该索引值
		if index, ok := p.server.logManager.LastIndexOfTerm(resp.Term); ok {
			p.prevLogIndex = index
		}
		//from snapshort
	}
}

func (p *Peer) newEntry(prevLogTerm uint64, logEntries []log.LogEntry) *protocol.Entry {
	entry := protocol.Entry{
		Term:         p.server.Term(),
		LeaderID:     p.leaderID,
		PrevLogIndex: p.prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: p.server.logManager.CommitIndex(),
	}
	if len(logEntries) <= 0 {
		entry.Entries = []byte{}
	} else {
		bs := make([]byte, 0)
		for _, v := range logEntries {
			bs = append(bs, v.ToBytes()...)
		}
		entry.Entries = bs
	}
	return &entry
}

func NewPeer(leaderID, peerID uint64, remoteAddr string, server *Server) *Peer {
	context, cancel := context.WithCancel(context.Background())
	peer := &Peer{
		leaderID:   leaderID,
		peerID:     peerID,
		remoteAddr: remoteAddr,

		context:   context,
		cancel:    cancel,
		wait:      new(sync.WaitGroup),
		server:    server,
		state:     stateWait,
		stateChan: make(chan byte),
	}
	return peer
}
