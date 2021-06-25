package protocol

import "encoding/binary"

type Basic struct {
	ID uint64
}

func (b *Basic) ToBytes() []byte {
	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, b.ID)
	return bs
}

type BasicResp struct {
	Success byte
}

func (br *BasicResp) ToBytes() []byte {
	return []byte{br.Success}
}

func NewBasic(id uint64) *Basic {
	return &Basic{ID: id}
}

func NewBasicResp(success byte) *BasicResp {
	return &BasicResp{Success: success}
}
