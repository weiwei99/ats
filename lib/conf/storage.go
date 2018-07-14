package conf

import (
	"bufio"
	"fmt"
	"github.com/golang/glog"
	"io"
	"os"
	"strconv"
	"strings"
)

type StorageType int

const (
	StorageFile StorageType = iota
	StorageDisk
	StorageNetDisk
	StorageUnknown
)

type StorageConfig struct {
	Path    string      `json:"path"`
	Size    uint64      `json:"size"`
	SizeStr string      `json:"size_str"`
	Type    StorageType `json:"storage_type"`
}

func (ac *ATSConfig) loadStorage() error {
	filename := ac.Path + "storage.config"
	fi, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return err
	}
	defer fi.Close()

	br := bufio.NewReader(fi)
	for {
		a, _, c := br.ReadLine()
		if c == io.EOF {
			break
		}
		b := strings.TrimSpace(string(a))
		if len(b) < 1 || string(b[0]) == "#" {
			continue
		}
		words := strings.Fields(b)

		ins := StorageConfig{
			Path: words[0],
			Type: StorageUnknown,
		}

		// 分析大小
		if len(words) == 2 && len(words[1]) > 2 {
			// 配置了大小的情况
			ins.SizeStr = words[1]
			fmt.Println(ins.SizeStr)
			s, err := strconv.Atoi(ins.SizeStr)
			if err == nil {
				ins.Size = uint64(s)
			} else if ins.SizeStr[len(ins.SizeStr)-1] == 'M' {
				s, err := strconv.Atoi(ins.SizeStr[:len(ins.SizeStr)-1])
				if err != nil {
					glog.Warning("----example: 125M")
					continue
				}
				ins.Size = uint64(s) * (1 << 20)
			} else if ins.SizeStr[len(ins.SizeStr)-1] == 'G' {
				s, err := strconv.Atoi(ins.SizeStr[:len(ins.SizeStr)-1])
				if err != nil {
					glog.Warning("----example: 125G")
					continue
				}
				ins.Size = uint64(s) * (1 << 30)
			} else {

				continue
			}
		}

		ac.Storages = append(ac.Storages, ins)
	}

	return nil
}
