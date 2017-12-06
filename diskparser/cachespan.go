package diskparser

import (
	"errors"
	"syscall"

	"github.com/golang/glog"
)

type spanDiskId [2]int32

type Span struct {
	blocks       int64
	offset       int64
	hwSectorSize uint32
	alignment    uint32
	diskId       spanDiskId

	forcedVolumeNum int

	filePathname   bool
	pathname       string
	hashBaseString string

	next *Span
}

func (s *Span) init(path string, size int) error {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return nil
	}

	// TODO: deal with dir
	// TODO: deal with regular file
	//

	switch stat.Mode & syscall.S_IFMT {
	case syscall.S_IFBLK:
		s.diskId[0] = 0
		s.diskId[1] = int32(stat.Rdev)
		s.filePathname = true

		s.hwSectorSize = uint32(stat.Blksize)
		s.alignment = 0
		s.blocks = stat.Size / StoreBlockSize
		break
	default:
		return errors.New("not supported type")
	}

	s.pathname = path
	glog.V(6).Infof("cache_init: initialized span '%s'\n"+
		"hw_sector_size = %d, size = %d, blocks = %d, disk_id = %d file_pathname = %s",
		s.pathname, s.hwSectorSize, s.size(), s.blocks, s.diskId[0], s.diskId[1], s.filePathname)

	return nil
}

func (s *Span) size() int64 {
	return s.blocks * StoreBlockSize
}
