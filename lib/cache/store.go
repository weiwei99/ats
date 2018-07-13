/*
Initialization starts with an instance of Store reading the storage configuration file, by default storage.config.

store 之后，就是 span
*/
package cache

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
	"github.com/weiwei99/ats/lib/disk"
	"os"
	"syscall"
	"unsafe"
)

type Store struct {
	NDisk  uint32          `json:"n_disk"`
	Spans  []*Span         `json:"disk"`
	Config *conf.ATSConfig `json:"-"`
}

const (
	BLKBSZGET    = 0x80081270
	BLKBSZSET    = 0x40081271
	BLKFLSBUF    = 0x1261
	BLKFRAGET    = 0x1265
	BLKFRASET    = 0x1264
	BLKGETSIZE   = 0x1260
	BLKGETSIZE64 = 0x80081272
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

//
func NewStore(config *conf.ATSConfig) (*Store, error) {

	store := &Store{
		Config: config,
	}

	return store, nil
}

//
func (store *Store) LoadConfig() error {
	for _, v := range store.Config.Storages {
		sp, err := NewSpan(v)
		if err != nil {
			glog.Errorf("load disk %s failed", v)
			continue
		}
		store.Spans = append(store.Spans, sp)
	}
	if len(store.Spans) == 0 {
		return fmt.Errorf("%s", "can not found any span")
	}
	return nil
}

func (store *Store) TotalBlocks() int {
	t := 0
	for _, s := range store.Spans {
		t += s.TotalBlocks()
	}
	return t
}

func ioctl(fd uintptr, request, argp uintptr) (err error) {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, request, argp)
	if errno != 0 {
		err = errno
	}
	return os.NewSyscallError("ioctl", err)
}
