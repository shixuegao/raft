package protocol

import "encoding/binary"

const (
	TypeBasic = iota
	TypeBasicResp
	TypeVote
	TypeVoteResp
	TypeEntry
	TypeEntryResp
)

const (
	Success = iota
	Failure
)

const (
	Granted = iota
	Disagree
)

type WrapperData interface {
	ToBytes() []byte
}

type Wrapper struct {
	RequestType byte
	Number      uint32
	Data        WrapperData
}

func (w *Wrapper) ToBytes() []byte {
	bsData := w.Data.ToBytes()
	total := 1 + len(bsData)
	bs := make([]byte, total)
	size := 0
	bs[size] = w.RequestType
	size += 1
	binary.BigEndian.PutUint32(bs[size:size+4], w.Number)
	size += 4
	copy(bs[size:], bsData)
	return bs
}

func (w *Wrapper) GetBasic() *Basic {
	return w.Data.(*Basic)
}

func (w *Wrapper) GetVote() *Vote {
	return w.Data.(*Vote)
}

func (w *Wrapper) GetVoteResp() *VoteResp {
	return w.Data.(*VoteResp)
}

func (w *Wrapper) GetEntry() *Entry {
	return w.Data.(*Entry)
}

func (w *Wrapper) GetEntryResp() *EntryResp {
	return w.Data.(*EntryResp)
}

func NewWrapper(requestType byte, number uint32, data WrapperData) *Wrapper {
	return &Wrapper{
		RequestType: requestType,
		Number:      number,
		Data:        data}
}
