package diskparser

import (
	"encoding/binary"
	//"encoding/hex"
	"fmt"
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

type DiskHeader struct {
	Magic          uint32 `json:"magic"`
	NumVolumes     uint32 `json:"num_volumes"`
	NumFree        uint32 `json:"num_free"`
	NumUsed        uint32 `json:"num_used"`
	NumDiskvolBlks uint32 `json:"num_diskvol_blks"`
	NumBlocks      uint64 `json:"num_blocks"`
	VolInfo        *DiskVolBlock
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

	header_len = 56 // 56个字节

	LEN_DiskVolBlock = 21
	LEN_DiskHeader   = 6 + LEN_DiskVolBlock
)

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
	Cleared         int `json:"cleared"`
	NumErrors       int `json:"num_errors"`
}

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

	ret.Len = GetGeometry().BlockSZ - STORE_BLOCK_SIZE>>STORE_BLOCK_SHIFT
	ret.Skip = STORE_BLOCK_SIZE

	header, err := ret.LoadDiskHeader(buffer)

	if err != nil {
		return nil, err
	}
	ret.Header = header
	return ret, nil
}

func (cd *CacheDisk) LoadDiskHeader(buffer []byte) (*DiskHeader, error) {
	header := DiskHeader{
		VolInfo: &DiskVolBlock{},
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
	header.VolInfo.Offset = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8

	header.VolInfo.Len = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+8]))
	curPos += 8

	header.VolInfo.Number = int(binary.LittleEndian.Uint32(buffer[curPos : curPos+4]))
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+8]))
	curPos += 4

	bytesValue := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	//fmt.Printf("\t%s\n", hex.Dump(buffer[curPos:curPos+2]))
	header.VolInfo.Type = uint8(bytesValue & 0x0007)
	header.VolInfo.Free = uint8(bytesValue & 0x0008)

	//fmt.Printf("\t%s\n", hex.Dump(buffer[:56]))
	return &header, nil
}
