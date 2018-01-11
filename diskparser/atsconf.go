package diskparser

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type ATSConfig struct {
	Path                 string
	MinAverageObjectSize int
	Storages             []string
}

func NewAtsConfig(path string) (*ATSConfig, error) {

	ac := &ATSConfig{
		Storages: []string{},
		Path:     path,
	}
	ac.Path = strings.TrimSuffix(ac.Path, "/")
	ac.Path += "/"

	err := ac.loadRecords()
	if err != nil {
		return nil, err
	}
	err = ac.loadStorage()
	if err != nil {
		return nil, err
	}
	return ac, nil
}

func (ac *ATSConfig) loadRecords() error {

	filename := ac.Path + "records.config"
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
		ba := strings.Fields(b)
		if ba[1] == "proxy.config.cache.min_average_object_size" {
			v, err := strconv.Atoi(ba[3])
			if err != nil {
				return err
			}
			ac.MinAverageObjectSize = v
			break
		}
	}
	return nil
}

func (ac *ATSConfig) loadVolume() error {
	filename := ac.Path + "volume.config "
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
		fmt.Println(string(a))
	}
	return nil
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
		ac.Storages = append(ac.Storages, b)
	}

	return nil
}

func (ac *ATSConfig) isComment(a []byte) bool {
	b := strings.TrimSpace(string(a))
	if len(b) < 1 || string(b[0]) == "#" {
		return true
	}
	return false
}

func (ac *ATSConfig) Dump() string {
	var ret string
	ret = fmt.Sprintf("%s", "----Current ATS config----\n")
	ret += "Disk Info: \n"
	for i, s := range ac.Storages {
		ret += fmt.Sprintf("\tID:%d:\t%s\n", i, s)
	}
	ret += fmt.Sprintf("MinAverageObjectSize: %d\n", ac.MinAverageObjectSize)
	return ret
}
