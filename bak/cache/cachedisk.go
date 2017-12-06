package cache

import (
	"io"
	"os"
	"unsafe"
)

const (
	VolBlockSize = (1024 * 1024 * 128)
	MinVolSize   = VolBlockSize
)

var cacheDisk []*CacheDisk

type off_t int64

type DiskHeader struct{}
type DiskVol struct{}

type DiskVolBlock struct {
	offset uint64
	len    uint64
	number int
	typ    int // :3 type,  :1 free
}

type CacheDisk struct {
	header          *DiskHeader
	path            string
	headerLen       int
	len             off_t
	start           off_t
	skip            off_t
	numUsableBlocks off_t
	hwSectorSize    int
	fd              int
	freeSpace       off_t
	wastedSpace     off_t
	diskVols        []*DiskVol
	freeBlocks      []*DiskVol
	numErrors       int
	cleared         int

	hashBaseString string
}

func (d *CacheDisk) open(path string, blocks, askip int64, ahwSectorSize int32, file *os.File, clear bool) {
	skip := askip
	start := skip

	len := blocks
	headerLen := uint64(0)

	l := uint64(0)
	for i := 0; i < 3; i++ {
		l = uint64((len * StoreBlockSize) - int64(start-skip))
		if l >= MinVolSize {
			headerLen = uint64(unsafe.Sizeof(DiskHeader{})) + uint64((l/MinVolSize-1)*uint64(unsafe.Sizeof(DiskVolBlock{})))
		} else {
			headerLen = uint64(unsafe.Sizeof(DiskHeader{}))
		}

		start = int64(skip) + int64(headerLen)
	}

	diskVols := make([]*DiskVol, (l/MinVolSize + 1))
	headerLen = uint64(roundToStoreBlock(headerLen))
	start = skip + int64(headerLen)
	numUsableBlocks := ((len * StoreBlockSize) - (start - askip)) >> StoreBlockShift

	// TODO: align
	header := new(DiskHeader)

	// reader header
	go d.openStart(file, skip, header, headerLen)
}

func (d *CacheDisk) openStart(file *os.File, skip int64, header *DiskHeader, headerLen int64) {
	// TODO
	b := make([]byte, headerLen)

	file.Seek(skip, 0)
	io.ReadFull(file, b)

	// deserialize to DiskHeader

	// initialize
}
