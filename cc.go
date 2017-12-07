package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/weiwei99/ats/diskparser"
	"syscall"
)

func ParseDoc(path string) {
	fd, err := syscall.Open(path, syscall.O_RDONLY, 0777)

	if err != nil {
		fmt.Errorf("open path failed: %s", err.Error())
		return
	}

	syscall.Seek(fd, 0xe0a0000, 0)

	buffer := make([]byte, 40960)
	_, err = syscall.Read(fd, buffer)
	if err != nil {
		fmt.Errorf("read disk header failed: %s", err.Error())
		return
	}

	doc, err := diskparser.NewDoc(buffer)
	if err != nil {
		fmt.Errorf("create doc failed: %s", err.Error())
		return
	}
	docstr, err := json.Marshal(doc)
	fmt.Println(string(docstr))
}

func main() {

	conf := diskparser.Config{}
	flag.StringVar(&conf.Path, "path", "/dev/sdb", "-path=/dev/sdb")
	flag.IntVar(&conf.MinAverageObjectSize, "mos", 8000, "-min_average_object_size")
	flag.Parse()

	// 分析
	cp := diskparser.CacheParser{}
	err := cp.ParseCacheDisk(conf)
	if err != nil {
		fmt.Println(err)
		return
	}
}
