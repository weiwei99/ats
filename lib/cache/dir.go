package cache

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

type DirPos struct {
	Vol     *Vol
	Segment int
	Bucket  int
	Depth   int
	Offset  int64
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
	// PS：此处的Offset是结合了OffsetHigh的和
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
	Index      *DirPos
	RawByte    []byte
	RawByteHex string
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

// 创建一个Dir示例
func NewDir() *Dir {
	d := &Dir{
		Index: &DirPos{
			Vol: nil,
		},
	}
	return d
}
