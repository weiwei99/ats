/*

 */
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

var CacheKey_next_table = [256]uint8{
	21, 53, 167, 51, 255, 126, 241, 151, 115, 66, 155, 174, 226, 215, 80, 188, 12, 95, 8, 24, 162, 201, 46, 104, 79, 172,
	39, 68, 56, 144, 142, 217, 101, 62, 14, 108, 120, 90, 61, 47, 132, 199, 110, 166, 83, 125, 57, 65, 19, 130, 148, 116,
	228, 189, 170, 1, 71, 0, 252, 184, 168, 177, 88, 229, 242, 237, 183, 55, 13, 212, 240, 81, 211, 74, 195, 205, 147, 93,
	30, 87, 86, 63, 135, 102, 233, 106, 118, 163, 107, 10, 243, 136, 160, 119, 43, 161, 206, 141, 203, 78, 175, 36, 37, 140,
	224, 197, 185, 196, 248, 84, 122, 73, 152, 157, 18, 225, 219, 145, 45, 2, 171, 249, 173, 32, 143, 137, 69, 41, 35, 89,
	33, 98, 179, 214, 114, 231, 251, 123, 180, 194, 29, 3, 178, 31, 192, 164, 15, 234, 26, 230, 91, 156, 5, 16, 23, 244,
	58, 50, 4, 67, 134, 165, 60, 235, 250, 7, 138, 216, 49, 139, 191, 154, 11, 52, 239, 59, 111, 245, 9, 64, 25, 129,
	247, 232, 190, 246, 109, 22, 112, 210, 221, 181, 92, 169, 48, 100, 193, 77, 103, 133, 70, 220, 207, 223, 176, 204, 76, 186,
	200, 208, 158, 182, 227, 222, 131, 38, 187, 238, 6, 34, 253, 128, 146, 44, 94, 127, 105, 153, 113, 20, 27, 124, 159, 17,
	72, 218, 96, 149, 213, 42, 28, 254, 202, 40, 117, 82, 97, 209, 54, 236, 121, 75, 85, 150, 99, 198}

var CacheKey_prev_table = [256]uint8{
	57, 55, 119, 141, 158, 152, 218, 165, 18, 178, 89, 172, 16, 68, 34, 146, 153, 233, 114, 48, 229, 0, 187, 154, 19, 180,
	148, 230, 240, 140, 78, 143, 123, 130, 219, 128, 101, 102, 215, 26, 243, 127, 239, 94, 223, 118, 22, 39, 194, 168, 157, 3,
	173, 1, 248, 67, 28, 46, 156, 175, 162, 38, 33, 81, 179, 47, 9, 159, 27, 126, 200, 56, 234, 111, 73, 251, 206, 197,
	99, 24, 14, 71, 245, 44, 109, 252, 80, 79, 62, 129, 37, 150, 192, 77, 224, 17, 236, 246, 131, 254, 195, 32, 83, 198,
	23, 226, 85, 88, 35, 186, 42, 176, 188, 228, 134, 8, 51, 244, 86, 93, 36, 250, 110, 137, 231, 45, 5, 225, 221, 181,
	49, 214, 40, 199, 160, 82, 91, 125, 166, 169, 103, 97, 30, 124, 29, 117, 222, 76, 50, 237, 253, 7, 112, 227, 171, 10,
	151, 113, 210, 232, 92, 95, 20, 87, 145, 161, 43, 2, 60, 193, 54, 120, 25, 122, 11, 100, 204, 61, 142, 132, 138, 191,
	211, 66, 59, 106, 207, 216, 15, 53, 184, 170, 144, 196, 139, 74, 107, 105, 255, 41, 208, 21, 242, 98, 205, 75, 96, 202,
	209, 247, 189, 72, 69, 238, 133, 13, 167, 31, 235, 116, 201, 190, 213, 203, 104, 115, 12, 212, 52, 63, 149, 135, 183, 84,
	147, 163, 249, 65, 217, 174, 70, 6, 64, 90, 155, 177, 185, 182, 108, 121, 164, 136, 58, 220, 241, 4,
}

func NextCacheKey(key []byte) []byte {

	ret := make([]byte, 16)
	ret[0] = CacheKey_next_table[key[0]]
	for i := 1; i < 16; i++ {
		ret[i] = CacheKey_next_table[(ret[i-1]+key[i])&0xFF]
	}

	return ret
}

func PrevCacheKey(key []byte) []byte {
	ret := make([]byte, 16)
	ret[0] = CacheKey_prev_table[key[0]]
	for i := 1; i < 16; i++ {
		ret[i] = CacheKey_prev_table[(ret[i-1]+key[i])&0xFF]
	}

	return ret
}
