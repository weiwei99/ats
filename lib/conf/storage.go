package conf

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

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
