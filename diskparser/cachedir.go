package diskparser

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

const (
	DIR_DEPTH = 4
)

type OpenDirEntry struct {
	NumWrites uint16
	MaxWrites uint16
}

type OpenDir struct {
	SingleDocDir Dir // Directory for the resident alternate
	FirstDir     Dir // Dir for the vector. If empty, a new dir is inserted, otherwise this dir is overwritten
}

// INTERNAL: do not access these members directly, use the
// accessors below (e.g. dir_offset, dir_set_offset).
// These structures are stored in memory 2 byte aligned.
// The accessors prevent unaligned memory access which
// is often either less efficient or unsupported depending
// on the processor.
type Dir struct {
	// THE BIT-FIELD INTERPRETATION OF THIS STRUCT WHICH HAS TO
	// USE MACROS TO PREVENT UNALIGNED LOADS
	// bits are numbered from lowest in u16 to highest
	// always index as u16 to avoid byte order issues
	Offset     uint64 `json:"offset"`      //bits 24 // (0,1:0-7) 16M * 512 = 8GB
	Big        uint8  `json:"big"`         //bits 2
	Size       uint8  `json:"size"`        //bits 6
	Tag        uint16 `json:"tag"`         //bits 12
	Phase      uint8  `json:"phase"`       //bits 1
	Head       uint8  `json:"head"`        //bits 1
	Pinned     uint8  `json:"pinned"`      //bits 1
	Token      uint8  `json:"token"`       //bits 1
	Next       uint16 `json:"next"`        //bits 16
	OffsetHigh uint16 `json:"offset_high"` //bits

	IdxSegment int
	IdxBucket  int
	IdxDepth   int
	RawByte    []byte
	RawByteHex string
	IdxOffset  int64
}

// INTERNAL: do not access these members directly, use the
// accessors below (e.g. dir_offset, dir_set_offset)
type FreeDir struct {
	Offset     [3]byte
	Reserved   uint8
	Prev       uint16
	Next       uint16
	OffsetHigh uint16
}

func NewDir(buffer []byte) (*Dir, error) {
	if len(buffer) != 10 {
		return nil, fmt.Errorf("buffer size not match sizeof Dir")
	}
	d := &Dir{}
	return d.LoadFromBuffer(buffer)
}

func (d *Dir) LoadFromBuffer(buffer []byte) (*Dir, error) {
	curPos := 0
	data := binary.LittleEndian.Uint32(buffer[:4])

	dataHigh := binary.LittleEndian.Uint16(buffer[8:10])
	d.Offset = uint64(data)&0x00ffffff | (uint64(dataHigh) << 24)

	d.Big = uint8((data >> 24) & 0x03)
	d.Size = uint8((data >> 26) & 0x3f)
	curPos += 4

	data = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	d.Tag = uint16(data & 0x00000fff)
	d.Phase = uint8((data >> 12) & 0x01)
	d.Head = uint8((data >> 13) & 0x01)
	d.Pinned = uint8((data >> 14) & 0x01)
	d.Token = uint8((data >> 15) & 0x01)
	d.Next = uint16((data >> 16) & 0xffff)

	d.RawByte = make([]byte, len(buffer))
	copy(d.RawByte, buffer)
	d.RawByteHex = hex.Dump(d.RawByte)
	return d, nil
}
