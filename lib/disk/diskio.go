package disk

import (
	"fmt"
	"github.com/golang/glog"
	"os"
	"syscall"
	"time"
)

type Reader struct {
	Path             string   `json:"path"`
	File             *os.File `json:"-"`
	Fd               *int     `json:"-"`
	StatReadBytes    uint64   `json:"stat_read_bytes"`
	StatReadCount    uint64   `json:"stat_read_count"`
	StatCostTimeNano int64    `json:"stat_cost_time_nano"`
	StatIoSpeed      float64  `json:"stat_io_speed"`
}

// todo: support o_direct
func (dio *Reader) Open(path string) error {
	//_, err := os.Stat(path)
	//if os.IsNotExist(err) {
	//	fd, err := syscall.Open(path, syscall.O_RDONLY, 0644)
	//	if err != nil {
	//		fmt.Errorf("open raw disk [%s] failed: %s", path, err.Error())
	//		return err
	//	}
	//	dio.Fd = &fd
	//	dio.File = nil
	//	fmt.Printf("dada\n")
	//} else {
	//	file, err := os.Open(path)
	//	if err != nil {
	//		fmt.Errorf("open file :%s failed: %s", path, err.Error())
	//		return err
	//	}
	//	dio.File = file
	//	dio.Fd = nil
	//	fmt.Printf("file dada\n")
	//}
	// no need o_create
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0644)
	if err != nil {
		fmt.Errorf("open raw disk [%s] failed: %s", path, err.Error())
		return err
	}
	dio.Fd = &fd
	dio.File = nil
	dio.Path = path

	// 为了防止除以0
	if dio.StatCostTimeNano == 0 {
		dio.StatCostTimeNano = 1
	}
	dio.StatReadCount = 0
	return nil
}

func (dio *Reader) Read(offset, size int64) ([]byte, error) {
	glog.V(10).Infof("------io try read: %d, %d\n", offset, size)
	ret := make([]byte, size)
	start := time.Now().UnixNano()
	if dio.Fd != nil {
		pos, err := syscall.Seek(*dio.Fd, offset, 0)
		if err != nil {
			return ret, fmt.Errorf("seek to cache dis header failed: %s", err.Error())
		}
		if pos != offset {
			return ret, fmt.Errorf("pos not much: %d", offset)
		}
		numRead, err := syscall.Read(*dio.Fd, ret)
		if err != nil {
			err = fmt.Errorf("read buffer failed: %s", err.Error())
			return ret, err
		}
		if int64(numRead) != size {
			err = fmt.Errorf("read buffer length not match: %d, %d", numRead, size)
			return ret, err
		}
	} else {

	}
	dio.StatCostTimeNano += time.Now().UnixNano() - start
	dio.StatReadBytes += uint64(size)
	dio.StatReadCount += 1
	//NanoSpeed := float64(dio.StatReadBytes) / float64(dio.StatCostTimeNano)
	//dio.StatIoSpeed = NanoSpeed * 1000 * 1000 * 1000
	return ret, nil
}

func (dio *Reader) DumpStat() string {

	dio.StatIoSpeed = float64(dio.StatReadBytes) / 1024 / 1024 / (float64(dio.StatCostTimeNano) / 1000 / 1000 / 1000)

	dumpStr := fmt.Sprintf("DIO info: \n"+
		"        Path:            %s\n"+
		" 		 Read Count:      %d\n"+
		"        Read Bytes:      %d byte\n"+
		"        CostTime:        %.6f sec\n"+
		"        IoSpeed:         %f MB/s\n",
		dio.Path,
		dio.StatReadCount,
		dio.StatReadBytes,
		float64(dio.StatCostTimeNano)/1000/1000/1000,
		dio.StatIoSpeed)

	return dumpStr
}
