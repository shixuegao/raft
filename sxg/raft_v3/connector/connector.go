package connector

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sxg/raft/v3/exception"
	"sxg/raft/v3/protocol"
	"sxg/raft/v3/util"
	"sync/atomic"
	"time"
)

const (
	defaultReadTimeout = 5 * time.Second
)

type Connector struct {
	remoteID     uint64
	maxEntrySize int
	readTimeout  time.Duration
	number       uint32
	reader       *bufio.Reader
	conn         *net.TCPConn
}

func (c *Connector) numberIncrement() uint32 {
	return atomic.AddUint32(&c.number, 1)
}

func (c *Connector) RawConn() *net.TCPConn {
	return c.conn
}

func (c *Connector) CloseConn() error {
	return c.conn.Close()
}

func (c *Connector) RemoteID() uint64 {
	return c.remoteID
}

func (c *Connector) SetRemoteID(id uint64) {
	c.remoteID = id
}

func (c *Connector) Remote() string {
	return fmt.Sprintf("Addr: %v, ID: %v", c.conn.RemoteAddr(), c.remoteID)
}

//ErrReadTimeout, ErrUnMatchedNumber, ErrUnMatchedReqType and other
//notice: When wrapper is nil, the EOF happened
func (c *Connector) Exchange(data protocol.WrapperData) (*protocol.Wrapper, error) {
	reqType, number, err := c.Send(data)
	if err != nil {
		return nil, err
	}
	wrapper, err := c.Read()
	if err != nil {
		return nil, err
	}
	//the next progress may be useless...,
	//but it can actually guarantee correct response
	//so if you want to remove them, just do it
	if wrapper != nil && number != wrapper.Number {
		return nil, exception.ErrUnMatchedNumber
	}
	if !_matchType(reqType, wrapper.RequestType) {
		return nil, exception.ErrUnMatchedReqType
	}
	return wrapper, nil
}

func (c *Connector) Send(data protocol.WrapperData) (byte, uint32, error) {
	var reqType byte = 0xff
	switch data.(type) {
	case *protocol.Entry:
		reqType = protocol.TypeEntry
	case *protocol.EntryResp:
		reqType = protocol.TypeEntryResp
	case *protocol.Vote:
		reqType = protocol.TypeVote
	case *protocol.VoteResp:
		reqType = protocol.TypeVoteResp
	default:
	}
	if reqType == 0xff {
		return 0, 0, fmt.Errorf("未知的的请求类型: %v", reqType)
	}
	number := c.numberIncrement()
	wrapper := protocol.NewWrapper(reqType, number, data)
	bs := wrapper.ToBytes()
	size := 0
	total := len(bs)
	for {
		n, err := c.conn.Write(bs[size:])
		if err != nil {
			return 0, 0, err
		}
		size += n
		if size < total {
			continue
		}
		break
	}
	return reqType, number, nil
}

//ErrReadTimeout, EOF and other
func (c *Connector) Read() (wrapper *protocol.Wrapper, err error) {
	defer func() {
		if e := util.AssertRecoverErr(); e != nil {
			err = e
		}
	}()
	buf := make([]byte, 16)
	c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	tempWrapper := protocol.NewWrapper(0, 0, nil)
	//request type
	_assertReader(c.reader, buf[:1])
	tempWrapper.RequestType = buf[0]
	//number
	_assertReader(c.reader, buf[:4])
	tempWrapper.Number = binary.BigEndian.Uint32(buf[:4])
	//data
	if tempWrapper.RequestType == protocol.TypeEntry {
		tempWrapper.Data = c.readEntry(buf)
	} else if tempWrapper.RequestType == protocol.TypeEntryResp {
		tempWrapper.Data = c.readEntryResp(buf)
	} else if tempWrapper.RequestType == protocol.TypeVote {
		tempWrapper.Data = c.readVote(buf)
	} else if tempWrapper.RequestType == protocol.TypeVoteResp {
		tempWrapper.Data = c.readVoteResp(buf)
	} else {
		err = fmt.Errorf("未知的请求类型: %v", tempWrapper.RequestType)
	}
	return
}

func (c *Connector) readEntry(buf []byte) *protocol.Entry {
	entry := &protocol.Entry{}
	//Term
	_assertReader(c.reader, buf[:8])
	entry.Term = binary.BigEndian.Uint64(buf[:8])
	//LeaderID
	_assertReader(c.reader, buf[:8])
	entry.LeaderID = binary.BigEndian.Uint64(buf[:8])
	//PrevLogIndex
	_assertReader(c.reader, buf[:8])
	entry.PrevLogIndex = binary.BigEndian.Uint64(buf[:8])
	//PrevLogTerm
	_assertReader(c.reader, buf[:8])
	entry.PrevLogTerm = binary.BigEndian.Uint64(buf[:8])
	//LeaderCommit
	_assertReader(c.reader, buf[:8])
	entry.LeaderCommit = binary.BigEndian.Uint64(buf[:8])
	//EntriesLen
	_assertReader(c.reader, buf[:4])
	entry.EntriesLen = binary.BigEndian.Uint32(buf[:4])
	if entry.EntriesLen > uint32(c.maxEntrySize) {
		panic(fmt.Errorf("Entry总长度[%v]超过上限[%v]", entry.EntriesLen, c.maxEntrySize))
	}
	//Entries
	entry.Entries = make([]byte, entry.EntriesLen)
	_assertReader(c.reader, entry.Entries)
	return entry
}

func (c *Connector) readEntryResp(buf []byte) *protocol.EntryResp {
	resp := protocol.NewEntryResp(0, 0, 0)
	//Term
	_assertReader(c.reader, buf[:8])
	resp.Term = binary.BigEndian.Uint64(buf[:8])
	//Index
	_assertReader(c.reader, buf[:8])
	resp.Index = binary.BigEndian.Uint64(buf[:8])
	//Success
	_assertReader(c.reader, buf[:1])
	resp.Success = buf[0]
	return resp
}

func (c *Connector) readVote(buf []byte) *protocol.Vote {
	vote := protocol.NewVote(0, 0, 0, 0)
	//Term
	_assertReader(c.reader, buf[:8])
	vote.Term = binary.BigEndian.Uint64(buf[:8])
	//CandidateID
	_assertReader(c.reader, buf[:8])
	vote.CandidateID = binary.BigEndian.Uint64(buf[:8])
	//LastLogIndex
	_assertReader(c.reader, buf[:8])
	vote.LastLogIndex = binary.BigEndian.Uint64(buf[:8])
	//LastLogTerm
	_assertReader(c.reader, buf[:8])
	vote.LastLogTerm = binary.BigEndian.Uint64(buf[:8])
	return vote
}

func (c *Connector) readVoteResp(buf []byte) *protocol.VoteResp {
	resp := protocol.NewVoteResp(0, 0)
	//Term
	_assertReader(c.reader, buf[:8])
	resp.Term = binary.BigEndian.Uint64(buf[:8])
	//Success
	_assertReader(c.reader, buf[:1])
	resp.VoteGranted = buf[0]
	return resp
}

func NewConnector1(remoteID uint64, conn *net.TCPConn, maxEntrySize int, readTimeout time.Duration) *Connector {
	reader := bufio.NewReader(conn)
	if readTimeout <= 0 {
		readTimeout = defaultReadTimeout
	}
	return &Connector{
		remoteID:     remoteID,
		maxEntrySize: maxEntrySize,
		readTimeout:  readTimeout,
		reader:       reader,
		conn:         conn,
	}
}

func NewConnector2(remoteID uint64, addr string, maxEntrySize int, readTimeout time.Duration) (*Connector, error) {
	rAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, rAddr)
	if err != nil {
		return nil, err
	}
	if readTimeout <= 0 {
		readTimeout = defaultReadTimeout
	}
	reader := bufio.NewReader(conn)
	return &Connector{
		remoteID:     remoteID,
		maxEntrySize: maxEntrySize,
		readTimeout:  readTimeout,
		reader:       reader,
		conn:         conn,
	}, nil
}

func _assertReader(reader *bufio.Reader, p []byte) {
	_, err := io.ReadFull(reader, p)
	if err != nil {
		panic(err)
	}
}

func _matchType(sendType, readType byte) bool {
	switch sendType {
	case protocol.TypeEntry:
		return readType == protocol.TypeEntryResp
	case protocol.TypeVote:
		return readType == protocol.TypeVoteResp
	}
	return false
}
