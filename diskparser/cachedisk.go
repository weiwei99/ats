package diskparser

import (
	"encoding/binary"
	//"encoding/hex"
	"fmt"
	"io"
	"os"
	"unsafe"

	"bytes"

	"errors"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/golang/glog"
	"github.com/zhuangsirui/binpacker"
)

/*
#include <string.h>
#include <stdint.h>
struct DiskVolBlock {
  	uint64_t offset; // offset in bytes from the start of the disk
  	uint64_t len;    // length in in store blocks
  	int number;
  	unsigned int type : 3;
  	unsigned int free : 1;
};
struct DiskHeader {
	unsigned int magic;
  	unsigned int num_volumes;      // number of discrete volumes (DiskVol)
	unsigned int num_free;         // number of disk volume blocks free
	unsigned int num_used;         // number of disk volume blocks in use
	unsigned int num_diskvol_blks; // number of disk volume blocks
	uint64_t num_blocks;
	struct DiskVolBlock vol_info[1];
};
*/
import "C"

type DiskVol struct {
	NumVolBlocks int32
	VolNumber    int32
	Size         uint64

	disk     *CacheDisk
	dpbQueue *queue.Queue
}

type DiskHeader struct {
	Magic          uint32 `json:"magic"`
	NumVolumes     uint32 `json:"num_volumes"`
	NumFree        uint32 `json:"num_free"`
	NumUsed        uint32 `json:"num_used"`
	NumDiskvolBlks uint32 `json:"num_diskvol_blks"`
	NumBlocks      uint64 `json:"num_blocks"`
	VolInfo        []DiskVolBlock
}

type DiskVolBlock struct {
	Offset uint64 `json:"offset"` // offset in bytes from the start of the disk
	Len    uint64 `json:"len"`    // length in in store blocks
	Number int32  `json:"number"`
	Type   uint8  `json:"type"` //0-3
	Free   bool   `json:"free"` // TODO: 1
}

const (
	ZYDiskHeaderOffset = 0x2000 // 8192
	ZYVolOffset        = 0xfe00 // 65024
	//ZYDocOffset = 0x

	DISK_HEADER_MAGIC uint32 = 0xABCD1237 // 出现在 0002000， 0009

	STORE_BLOCK_SIZE       = 8192
	STORE_BLOCK_SHIFT      = 13
	DEFAULT_HW_SECTOR_SIZE = 512

	CACHE_BLOCK_SHIFT = 9
	CACHE_BLOCK_SIZE  = 1 << CACHE_BLOCK_SHIFT // 512, smallest sector size
	START_BLOCKS      = 16
	START_POS         = START_BLOCKS * CACHE_BLOCK_SIZE

	VOL_BLOCK_SIZE = 1024 * 1024 * 128
	MIN_VOL_SIZE   = VOL_BLOCK_SIZE

	PAGE_SIZE = 8192

	header_len = 56 // 56个字节

	LEN_DiskVolBlock = 21
	LEN_DiskHeader   = 6 + LEN_DiskVolBlock

	VolBlockSize = (1024 * 1024 * 128)
	MinVolSize   = VolBlockSize

	CacheNoneType = 0
)

//func NewCacheDisk(path string) (*CacheDisk, error) {
//
//	fd, err := syscall.Open(path, syscall.O_RDONLY, 0777)
//
//	if err != nil {
//		fmt.Errorf("open path failed: %s", err.Error())
//		return nil, err
//	}
//
//	ret := &CacheDisk{
//		Fd:   fd,
//		Path: path,
//	}
//
//	geo := &Geometry{
//		TotalSZ: 6001175126016,
//		BlockSZ: 11721045168,
//		AlignSZ: 0,
//	}
//
//	ret.Geometry = geo
//
//	ret.Len = geo.BlockSZ - STORE_BLOCK_SIZE>>STORE_BLOCK_SHIFT
//	ret.Skip = STORE_BLOCK_SIZE
//
//	header, err := ret.LoadDiskHeader()
//	if err != nil {
//		return nil, err
//	}
//	ret.Header = header
//	return ret, nil
//}

const CacheFileSize = 268435456

func GetGeometry() *Geometry {

	geos := make([]*Geometry, 0)

	BigGeo := &Geometry{
		TotalSZ: 6001175126016,
		BlockSZ: 11721045168,
		AlignSZ: 0,
	}
	geos = append(geos, BigGeo)

	SmallGeo := &Geometry{
		TotalSZ: 2147483648,
		BlockSZ: 4194304,
		AlignSZ: 0,
	}
	geos = append(geos, SmallGeo)
	G5Geo := &Geometry{
		TotalSZ: 5368709120,
		BlockSZ: 10485760,
		AlignSZ: 0,
	}
	geos = append(geos, G5Geo)

	return geos[1]
}
func NewCacheDiskFromBuffer(buffer []byte) (*CacheDisk, error) {
	if len(buffer) != 56 {

	}
	ret := &CacheDisk{}

	// TODO:
	// ret.len = GetGeometry().BlockSZ - STORE_BLOCK_SIZE>>STORE_BLOCK_SHIFT
	ret.skip = STORE_BLOCK_SIZE

	header, err := ret.LoadDiskHeader(buffer)

	if err != nil {
		return nil, err
	}
	ret.header = header
	return ret, nil
}

func (cd *CacheDisk) LoadDiskHeader(buffer []byte) (*DiskHeader, error) {
	header := DiskHeader{
		VolInfo: make([]DiskVolBlock, 1),
	}
	curPos := 0
	header.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	if header.Magic != DISK_HEADER_MAGIC {
		return nil, fmt.Errorf("disk header magic not match")
	}
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumVolumes = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumFree = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumUsed = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumDiskvolBlks = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	// 因为C语言对齐
	curPos += 4
	header.NumBlocks = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	//uint64_t delta_3_2 = skip - (skip >> STORE_BLOCK_SHIFT);

	// 对齐
	//curPos += 2
	header.VolInfo[0].Offset = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8

	header.VolInfo[0].Len = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8

	header.VolInfo[0].Number = int32(binary.LittleEndian.Uint32(buffer[curPos : curPos+4]))
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+8]))
	curPos += 4

	bytesValue := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+2]))
	header.VolInfo[0].Type = uint8(bytesValue & 0x0007)
	header.VolInfo[0].Free = bool(bytesValue&0x0008 == 1)

	//fmt.Printf("\t%s\n", hex.Dump(buffer[:56]))
	return &header, nil
}

var cacheDisk []*CacheDisk

type off_t = uint64

type CacheDisk struct {
	header          *DiskHeader
	path            string
	headerLen       int32
	len             off_t
	start           off_t
	skip            off_t
	numUsableBlocks off_t
	hwSectorSize    int
	fd              int
	freeSpace       off_t
	wastedSpace     off_t
	diskVols        []*DiskVol
	freeBlocks      *DiskVol
	numErrors       int
	cleared         int

	hashBaseString string
	file           *os.File
}

func (d *CacheDisk) open(path string, blocks, askip off_t, ahwSectorSize int32, file *os.File, clear bool) error {
	skip := askip
	start := skip

	len := blocks
	headerLen := int32(0)

	l := uint64(0)
	for i := 0; i < 3; i++ {
		l = uint64((len * StoreBlockSize) - (start - skip))
		if l >= MinVolSize {
			headerLen = int32(unsafe.Sizeof(DiskHeader{})) + int32((l/MinVolSize-1)*uint64(unsafe.Sizeof(DiskVolBlock{})))
		} else {
			headerLen = int32(unsafe.Sizeof(DiskHeader{}))
		}

		start = skip + off_t(headerLen)
	}

	headerLen = int32(roundToStoreBlock(off_t(headerLen)))
	start = skip + off_t(headerLen)
	d.numUsableBlocks = ((len * StoreBlockSize) - (start - askip)) >> StoreBlockShift
	d.diskVols = make([]*DiskVol, l/MinVolSize)

	// TODO: align

	d.file = file
	d.headerLen = headerLen
	d.skip = skip
	// reader header
	d.openStart()

	if d.header.Magic != DISK_HEADER_MAGIC || d.header.NumBlocks != uint64(headerLen) {
		d.clearDisk()
		return nil
	}

	d.updateHeader()
	return nil
}

type DiskVolBlockQueue struct {
	b        *DiskVolBlock
	newBlock int
	link     *DiskVolBlockQueue
}

func (d *CacheDisk) updateHeader() {
	d.freeBlocks = new(DiskVol)
	d.freeBlocks.VolNumber = -1
	d.freeBlocks.disk = d
	d.freeBlocks.NumVolBlocks = 0
	d.freeBlocks.Size = 0

	freeSpace := uint64(0)
	n := 0
	j := 0

	for i := uint32(0); i < d.header.NumDiskvolBlks; i++ {
		dpbq := new(DiskVolBlockQueue)
		dpbq.b = &d.header.VolInfo[i]

		if d.header.VolInfo[i].Free {
			d.freeBlocks.NumVolBlocks++
			d.freeBlocks.Size += dpbq.b.Len
			d.diskVols[i].dpbQueue.Put(dpbq)
			freeSpace += dpbq.b.Len
			continue
		}

		volNumber := d.header.VolInfo[i].Number
		for j = 0; j < n; j++ {
			if d.diskVols[j].VolNumber == volNumber {
				d.diskVols[j].dpbQueue.Put(dpbq)

				d.diskVols[j].NumVolBlocks++
				d.diskVols[j].Size += dpbq.b.Len
				break
			}
		}

		if j == n {
			q := new(queue.Queue)
			q.Put(dpbq)
			d.diskVols = append(d.diskVols, &DiskVol{
				VolNumber:    volNumber,
				disk:         d,
				NumVolBlocks: 1,
				Size:         dpbq.b.Len,
				dpbQueue:     q,
			})
			n++
		}
	}

	// TODO: assert
}
func (d *CacheDisk) clearDisk() {
	d.deleteAllVolumes()
}

func (d *CacheDisk) deleteAllVolumes() {
	h := d.header

	h.VolInfo[0].Offset = d.start
	h.VolInfo[0].Len = d.numUsableBlocks
	h.VolInfo[0].Type = CacheNoneType
	h.VolInfo[0].Free = true

	h.Magic = DISK_HEADER_MAGIC
	h.NumUsed = 0
	h.NumVolumes = 0
	h.NumFree = 1
	h.NumDiskvolBlks = 1
	h.NumBlocks = d.len

	d.cleared = 1

	d.updateHeader()
	d.SaveDiskHeader()
}

func (d *CacheDisk) SaveDiskHeader() error {
	h := d.header
	buffer := new(bytes.Buffer)
	packer := binpacker.NewPacker(binary.LittleEndian, buffer)

	packer = packer.PushUint32(h.Magic).PushUint32(h.NumVolumes).PushUint32(h.NumFree).
		PushUint32(h.NumUsed).PushUint32(h.NumDiskvolBlks).PushUint64(h.NumBlocks)

	for i := 0; i < int(h.NumDiskvolBlks); i++ {
		packer = packer.PushUint64(h.VolInfo[i].Offset).PushUint64(h.VolInfo[i].Len).PushInt32(h.VolInfo[i].Number)

		v := h.VolInfo[i].Type & 0x07
		if h.VolInfo[i].Free {
			v += 0x08
		}
		packer.PushUint8(v)
	}

	b := buffer.Bytes()
	len := buffer.Len()
	if len != int(d.headerLen) {
		return errors.New("length error")
	}

	d.file.Seek(int64(d.skip), 0)
	n, err := d.file.Write(b)
	if err != nil || n != len {
		return errors.New("write err")
	}

	return packer.Error()
}

func (d *CacheDisk) openStart() {
	// TODO
	b := make([]byte, d.headerLen)

	d.file.Seek(int64(d.skip), 0)
	io.ReadFull(d.file, b)

	header, err := d.LoadDiskHeader(b)
	if err != nil {
		glog.Error("load disk header failed %s", err)
		return
	}

	d.header = header
	return
}
