/*
Initialization starts with an instance of Store reading the storage configuration file, by default storage.config.
For each valid element in the file an instance of Span is created. These are of basically four types:

* File
* Directory
* Disk
* Raw device

After creating all the Span instances, they are grouped by device ID to internal linked lists attached to
the Store::disk array[#store-disk-array]_. Spans that refer to the same directory, disk, or raw device are coalesced
in to a single span. Spans that refer to the same file with overlapping offsets are also coalesced [5]. This is all
done in ink_cache_init() called during startup.

*/
package cache

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
	"github.com/weiwei99/ats/lib/disk"
	"os"
	"syscall"
	"unsafe"
)

const (
	BLKBSZGET    = 0x80081270
	BLKBSZSET    = 0x40081271
	BLKFLSBUF    = 0x1261
	BLKFRAGET    = 0x1265
	BLKFRASET    = 0x1264
	BLKGETSIZE   = 0x1260
	BLKGETSIZE64 = 0x80081272
	BLKALIGNOFF  = 0x127a
	BLKPBSZGET   = 0x127b
	BLKRAGET     = 0x1263
	BLKRASET     = 0x1262
	BLKROGET     = 0x125e
	BLKROSET     = 0x125d
	BLKRRPART    = 0x125f
	BLKSECTGET   = 0x1267
	BLKSECTSET   = 0x1266
	BLKSSZGET    = 0x1268
)

//
type Span struct {
	Blocks          int64              `json:"blocks"`
	Offset          int64              `json:"offset"`
	HWSectorSize    uint32             `json:"hw_sector_size"`
	Alignment       uint32             `json:"alignment"`
	Path            string             `json:"path"`
	FilePathName    bool               `json:"file_path_name"`
	DiskId          [16]byte           //
	ForcedVolumeNum int                `json:"forced_volume_num"`
	StorageConf     conf.StorageConfig `json:"-"`
}

func NewSpan(spanConf conf.StorageConfig) (*Span, error) {
	sp := &Span{
		ForcedVolumeNum: -1,
		HWSectorSize:    256,
		Path:            spanConf.Path,
		StorageConf:     spanConf,
	}

	// 路径是否有效
	fd, err := syscall.Open(sp.Path, syscall.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("unable to open '%s' %s", sp.StorageConf.Path, err.Error())
	}
	syscall.Close(fd)

	// 文件属性
	fi, err := os.Stat(sp.Path)
	if err != nil {
		return nil, fmt.Errorf("unable to stat: '%s' %s", sp.Path, err.Error())
	}
	stat := fi.Sys().(*syscall.Stat_t)

	// Directories require an explicit size parameter. For device nodes and files, we use
	// the existing size.
	if fi.IsDir() && sp.StorageConf.Size <= 0 {
		return nil, fmt.Errorf("cache %s requires a size > 0", sp.Path)
	}

	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		fallthrough
	case syscall.S_IFCHR:
		geo, err := GetGeometry(sp.StorageConf.Path)
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint32(sp.DiskId[:8], 0)
		binary.LittleEndian.PutUint32(sp.DiskId[8:16], uint32(stat.Rdev))
		sp.Blocks = geo.TotalSZ / STORE_BLOCK_SIZE
		sp.Alignment = geo.AlignSZ
		sp.HWSectorSize = uint32(geo.BlockSZ)
		sp.FilePathName = true

	case syscall.S_IFDIR:
		binary.LittleEndian.PutUint32(sp.DiskId[:8], uint32(stat.Dev))
		binary.LittleEndian.PutUint32(sp.DiskId[8:16], uint32(stat.Ino))
		sp.Alignment = 0
		sp.Blocks = int64(sp.StorageConf.Size) / STORE_BLOCK_SIZE
		sp.FilePathName = false

	case syscall.S_IFREG:
		if sp.StorageConf.Size > 0 && fi.Size() < int64(sp.StorageConf.Size) {
			// TODO: 查看文件系统剩余大小
			glog.Warning("not enough free space for cache")
		}
		binary.LittleEndian.PutUint32(sp.DiskId[:8], uint32(stat.Dev))
		binary.LittleEndian.PutUint32(sp.DiskId[8:16], uint32(stat.Ino))
		sp.Alignment = 0
		sp.FilePathName = true
	default:
		return nil, fmt.Errorf("SPAN_ERROR_UNSUPPORTED_DEVTYPE")

	}
	return sp, nil
}

func (span *Span) TotalBlocks() int64 {
	return span.Blocks
}

func (span *Span) Size() int64 {
	return span.Blocks * STORE_BLOCK_SIZE
}

//
func GetGeometry(path string) (*disk.Geometry, error) {

	dk, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer dk.Close()

	ret := disk.Geometry{}

	// BLKGETSIZE64 gets the block device size in bytes.
	var blksize64 uint64
	if err := ioctl(dk.Fd(), BLKGETSIZE64, uintptr(unsafe.Pointer(&blksize64))); err != nil {
		return nil, err
	}
	ret.TotalSZ = int64(blksize64)

	// BLKSSZGET gets the logical block size in bytes.
	var blksize uint64
	if err := ioctl(dk.Fd(), BLKSSZGET, uintptr(unsafe.Pointer(&blksize))); err != nil {
		return nil, err
	}
	ret.BlockSZ = int64(blksize)

	// BLKSSZGET gets the logical block size in bytes.
	var alignsz uint32
	if err := ioctl(dk.Fd(), BLKALIGNOFF, uintptr(unsafe.Pointer(&alignsz))); err != nil {
		return nil, err
	}
	ret.AlignSZ = alignsz

	if ret.TotalSZ == 0 || ret.BlockSZ == 0 {
		return nil, fmt.Errorf("can not get total size or block size")
	}

	//geos := make([]*disk.Geometry, 0)
	//BigGeo := &disk.Geometry{
	//	TotalSZ: 6001175126016,
	//	BlockSZ: 11721045168,
	//	AlignSZ: 0,
	//}
	//geos = append(geos, BigGeo)
	//
	//SmallGeo := &disk.Geometry{
	//	TotalSZ: 2147483648,
	//	BlockSZ: 4194304,
	//	AlignSZ: 0,
	//}
	//geos = append(geos, SmallGeo)
	//G5Geo := &disk.Geometry{
	//	TotalSZ: 5368709120,
	//	BlockSZ: 10485760,
	//	AlignSZ: 0,
	//}
	//geos = append(geos, G5Geo)

	return &ret, nil
}

func ioctl(fd uintptr, request, argp uintptr) (err error) {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, request, argp)
	if errno != 0 {
		err = errno
	}
	return os.NewSyscallError("ioctl", err)
}
