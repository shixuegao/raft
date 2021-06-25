package raftv3

import (
	"context"
	"encoding/binary"
	"io"
	"net"
	logI "sxg/logger"
	"sxg/raft/v3/event"
	"sxg/raft/v3/server"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	AccessPermit = iota
	AccessDeny
)

const (
	Success = iota
	Failure
)

type Receiver struct {
	context        context.Context
	cancel         context.CancelFunc
	maxAccessCount int
	accessCount    int32
	listener       *net.TCPListener
	state          int32
	wait           *sync.WaitGroup
	logger         logI.Logger
	server         *server.Server
}

func (r *Receiver) Start() {
	go func() {
		for {
			conn, err := r.listener.AcceptTCP()
			if err != nil {
				r.logger.Errorf("监听Client TCP连接异常-->%v", err.Error())
				return
			}
			if atomic.LoadInt32(&r.state) == AccessDeny ||
				int(atomic.LoadInt32(&r.accessCount)) >= r.maxAccessCount {
				conn.Close()
				continue
			}
			atomic.AddInt32(&r.accessCount, 1)
			r.handleConn(conn)
		}
	}()
}

func (r *Receiver) isOver() bool {
	select {
	case <-r.context.Done():
		return true
	default:
		return false
	}
}

func (r *Receiver) handleConn(conn *net.TCPConn) {
	r.wait.Add(1)
	go func() {
		defer r.wait.Done()
		defer conn.Close()

		req, err := readSimpleReq(conn)
		if err != nil {
			return
		}
		var applied byte = Failure
		defer func() {
			resp := SimpleResp{req.ID, applied}
			if _, err := conn.Write(resp.ToBytes()); err != nil {
				r.logger.Errorf("写数据到远端[%v]失败-->%v", conn.RemoteAddr().String(), err.Error())
			}
		}()

		var ptr *appliedResult = nil
		event := event.NewClientEvent(req, func(applied bool, err error) {
			result := appliedResult{applied: applied, err: err}
			atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&ptr)), unsafe.Pointer(&result))
		})
		put := false
		for i := 0; i < 3; i++ {
			if r.isOver() {
				return
			}
			if put = r.server.EventQueue().TryPut(event); put {
				break
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
		if put {
			for {
				if r.isOver() {
					return
				}
				if atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&ptr))) == nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}
				if ptr.applied {
					applied = Success
				} else {
					applied = Failure
					r.logger.Warnf("处理Client的请求失败-->%v", ptr.err.Error())
				}
				break
			}
		} else {
			applied = Failure
		}
	}()
}

func NewReceiver(maxAccessCount int, addr string,
	server *server.Server, logger logI.Logger) (*Receiver, error) {
	lAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", lAddr)
	if err != nil {
		return nil, err
	}
	context, cancel := context.WithCancel(context.Background())
	receiver := Receiver{
		context:        context,
		cancel:         cancel,
		maxAccessCount: maxAccessCount,
		listener:       listener,
		state:          AccessPermit,
		wait:           new(sync.WaitGroup),
		logger:         logger,
		server:         server,
	}
	return &receiver, nil
}

type appliedResult struct {
	applied bool
	err     error
}

type SimpleReq struct {
	ID      uint64
	CLen    uint16
	Content []byte
}

type SimpleResp struct {
	ID      uint64
	Success byte
}

//implementation of event.ClientData
func (s *SimpleReq) ToBytes() []byte {
	total := 8 + 2 + s.CLen
	bs := make([]byte, total)
	binary.BigEndian.PutUint64(bs[:8], s.ID)
	binary.BigEndian.PutUint16(bs[8:10], s.CLen)
	copy(bs[10:], s.Content)
	return bs
}

func (s SimpleResp) ToBytes() []byte {
	total := 8 + 1
	bs := make([]byte, total)
	binary.BigEndian.PutUint64(bs[:8], s.ID)
	bs[8] = s.Success
	return bs
}

func readSimpleReq(conn *net.TCPConn) (*SimpleReq, error) {
	sr := SimpleReq{}
	temp := make([]byte, 8)
	//ID
	_, err := io.ReadFull(conn, temp)
	if err != nil {
		return nil, err
	}
	sr.ID = binary.BigEndian.Uint64(temp)
	//CLen
	_, err = io.ReadFull(conn, temp[:2])
	if err != nil {
		return nil, err
	}
	sr.CLen = binary.BigEndian.Uint16(temp[:2])
	//Content
	p := make([]byte, sr.CLen)
	_, err = io.ReadFull(conn, p)
	if err != nil {
		return nil, err
	}
	sr.Content = p
	return &sr, nil
}
