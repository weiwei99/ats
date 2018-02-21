package conf

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

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
