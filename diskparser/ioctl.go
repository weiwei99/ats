package diskparser

import (
	"fmt"
	"os/exec"
	"strconv"
	"syscall"
	"unsafe"
)

const (
	BlkGetSize64 = 1
)

func ioctlBlkGetSize64(fd uintptr) (int64, error) {
	var size int64
	if _, _, err := syscall.Syscall(syscall.SYS_IOCTL, fd, BlkGetSize64, uintptr(unsafe.Pointer(&size))); err != 0 {
		return 0, err
	}
	return size, nil
}

// blockdev --getsize64 /dev/sda
func GetBlkGetSize64(path string) int {
	cmd := exec.Command("/sbin/blockdev", "--getsize64", path)
	blksize, err := cmd.Output()
	if err != nil {
		fmt.Println(err.Error())
		return -1
	}

	bs, err := strconv.Atoi(string(blksize))
	return bs
}

// blockdev --getsize /dev/sda
func GetBlkGetSize(path string) int {
	cmd := exec.Command("/sbin/blockdev", "--getsize", path)
	blksize, err := cmd.Output()
	if err != nil {
		fmt.Println(err.Error())
		return -1
	}

	bs, err := strconv.Atoi(string(blksize))
	return bs
}
