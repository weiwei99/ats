package diskparser

import (
	"fmt"
	"os"
	"syscall"
	"time"
)

type DiskReader struct {
	Path             string   `json:"path"`
	File             *os.File `json:"-"`
	Fd               *int     `json:"-"`
	StatReadBytes    uint64   `json:"stat_read_bytes"`
	StatCostTimeNano int64    `json:"stat_cost_time_nano"`
	StatIoSpeed      float64  `json:"stat_io_speed"`
}

func (dio *DiskReader) open(path string) error {
	_, err := os.Stat("path")
	if os.IsNotExist(err) {
		fd, err := syscall.Open(path, syscall.O_RDONLY, 0777)
		if err != nil {
			fmt.Errorf("open raw disk [%s] failed: %s", path, err.Error())
			return err
		}
		dio.Fd = &fd
		dio.File = nil
	} else {
		file, err := os.Open(path)
		if err != nil {
			fmt.Errorf("open file :%s failed: %s", path, err.Error())
			return err
		}
		dio.File = file
		dio.Fd = nil
	}
	dio.Path = path

	// 为了防止除以0
	if dio.StatCostTimeNano == 0 {
		dio.StatCostTimeNano = 1
	}
	return nil
}

func (dio *DiskReader) read(offset, size int64) ([]byte, error) {

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
	//NanoSpeed := float64(dio.StatReadBytes) / float64(dio.StatCostTimeNano)
	//dio.StatIoSpeed = NanoSpeed * 1000 * 1000 * 1000
	return ret, nil
}

func (dio *DiskReader) DumpStat() string {

	dio.StatIoSpeed = float64(dio.StatReadBytes) / 1024 / 1024 / (float64(dio.StatCostTimeNano) / 1000 / 1000 / 1000)

	dumpStr := fmt.Sprintf("DIO info: \n"+
		"        Read Bytes:      %d byte\n"+
		"        CostTime:        %.6f sec\n"+
		"        IoSpeed:         %f MB/s\n",
		dio.StatReadBytes,
		float64(dio.StatCostTimeNano)/1000/1000/1000,
		dio.StatIoSpeed)

	return dumpStr
}
