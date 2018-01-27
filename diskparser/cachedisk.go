package diskparser

import (
	"encoding/binary"
	//"encoding/hex"
	"encoding/hex"
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
	"sync"
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
	NumVolBlocks int
	VolNumber    int
	Size         uint64
}

type DiskVolBlock struct {
	Offset uint64 `json:"offset"` // offset in bytes from the start of the disk
	Len    uint64 `json:"len"`    // length in in store blocks
	Number int    `json:"number"`
	Type   uint8  `json:"type"` //0-3
	Free   uint8  `json:"free"` //1
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

	DiskHeaderLen = 56 // 56个字节

	LEN_DiskVolBlock = 21
	LEN_DiskHeader   = 6 + LEN_DiskVolBlock
)

// 磁盘头，一个磁盘可以有多个Volume
type DiskHeader struct {
	Magic          uint32        `json:"magic"`
	NumVolumes     uint32        `json:"num_volumes"`
	NumFree        uint32        `json:"num_free"`
	NumUsed        uint32        `json:"num_used"`
	NumDiskvolBlks uint32        `json:"num_diskvol_blks"`
	NumBlocks      uint64        `json:"num_blocks"`
	VolInfo        *DiskVolBlock `json:"-"`
}

// 磁盘信息
type CacheDisk struct {
	Header   *DiskHeader
	Geometry *Geometry

	Path            string `json:"path"`
	Len             int64  `json:"len"`
	Start           int64  `json:"start"`
	Skip            int64  `json:"skip"`
	NumUsableBlocks int64  `json:"num_usable_blocks"`
	HWSectorSize    int    `json:"hw_sector_size"`
	Fd              int
	DiskVols        []DiskVol
	FreeBlocks      []DiskVol
	Cleared         int  `json:"cleared"`
	NumErrors       int  `json:"num_errors"`
	DebugLoad       bool // debug加载模式，解析保存数据（消耗内存）

	YYVol               *Vol   `json:"-"`
	PsRawDiskHeaderData []byte `json:"-"`
	PsDiskOffsetStart   int64  // 磁盘上相对起始位置
	PsDiskOffsetEnd     int64  // 磁盘上相对结束位置
	Dio                 *DiskReader
	AtsConf             *conf.ATSConfig
	DocLoadMutex        *sync.RWMutex
}

func NewCacheDisk(path string, atsconf *conf.ATSConfig) (*CacheDisk, error) {
	/// 初始化reader
	dr := &DiskReader{}
	err := dr.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parse disk %s failed: %s", path, err.Error())
	}

	cd := &CacheDisk{
		Start:        START,
		Dio:          dr,
		Path:         path,
		AtsConf:      atsconf,
		DocLoadMutex: new(sync.RWMutex),
	}

	// 初始化变量
	cd.Header = &DiskHeader{
		VolInfo: &DiskVolBlock{},
	}
	// 初始化必要的磁盘信息
	err = cd.initGeometryInfo()
	if err != nil {
		return nil, fmt.Errorf("init disk geometry info failed: %s", err.Error())
	}

	return cd, nil
}

// 所需数据大小
func (cd *CacheDisk) CacheDiskHeaderLen() int64 {
	return DiskHeaderLen
}

//// 从buffer中加载CacheDisk结构信息
//func (cd *CacheDisk) load(buffer []byte) error {
//	if len(buffer) < DiskHeaderLen {
//		return fmt.Errorf("need %d raw data for parse disk info", DiskHeaderLen)
//	}
//
//	// 预存数据
//	if cd.DebugLoad {
//		cd.PsRawDiskHeaderData = make([]byte, DiskHeaderLen)
//		copy(cd.PsRawDiskHeaderData, buffer)
//	}
//
//	// 分析磁盘头
//	header, err := cd.loadDiskHeader(buffer[:DiskHeaderLen])
//	if err != nil {
//		return err
//	}
//	cd.Header = header
//
//	return nil
//}

//
func (cd *CacheDisk) initGeometryInfo() error {
	cd.Geometry = getGeometry()
	cd.Len = getGeometry().BlockSZ - STORE_BLOCK_SIZE>>STORE_BLOCK_SHIFT
	cd.Skip = STORE_BLOCK_SIZE

	return nil
}

const CacheFileSize = 268435456

func (cd *CacheDisk) loadDiskHeader(buffer []byte) (*DiskHeader, error) {
	header := DiskHeader{
		VolInfo: &DiskVolBlock{},
	}
	curPos := 0
	header.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	if header.Magic != DISK_HEADER_MAGIC {
		return nil, fmt.Errorf("disk header magic not match")
	}
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - Magic <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumVolumes = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumVolume <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumFree = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumFree <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumUsed = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumUsed <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	header.NumDiskvolBlks = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumDiskVolBlocks <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	// 因为C语言对齐
	curPos += 4
	header.NumBlocks = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - NumBlocks <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8
	//uint64_t delta_3_2 = skip - (skip >> STORE_BLOCK_SHIFT);

	// 对齐
	//curPos += 2
	header.VolInfo.Offset = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - VolInfo - Offset <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8

	header.VolInfo.Len = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - VolInfo - Len <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8

	header.VolInfo.Number = int(binary.LittleEndian.Uint32(buffer[curPos : curPos+4]))
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - VolInfo - Number <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+4]))
	curPos += 4

	// binary.LittleEndian.Uint16

	bytesValue := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	glog.V(10).Infof(
		"CacheDisk - DiskHeader - VolInfo - Type[0-3] & Free[4-4] <Offset %d>: \n %s\n",
		curPos, hex.Dump(buffer[curPos:curPos+2]))
	header.VolInfo.Type = uint8(bytesValue & 0x0007)
	header.VolInfo.Free = uint8(bytesValue & 0x0008)

	return &header, nil
}
