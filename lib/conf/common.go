package conf

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type ConfigReader struct {
	FileName string
	file     *os.File
	reader   *bufio.Reader
}

func NewConfigReader(filename string) (*ConfigReader, error) {

	cr := &ConfigReader{}
	fi, err := os.Open(filename)
	if err != nil {
		fmt.Printf("Error: %s\n", err)
		return nil, err
	}
	br := bufio.NewReader(fi)
	cr.reader = br
	return cr, nil
}

func (cr *ConfigReader) ReadLine() (string, error) {
	a, _, c := cr.reader.ReadLine()
	if c == io.EOF {
		return "", c
	}
	b := strings.TrimSpace(string(a))
	if len(b) < 1 || string(b[0]) == "#" {
		return "", nil
	}

	return b, nil
}

func (cr *ConfigReader) Close() {
	cr.file.Close()
}
