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
		fmt.Printf("Open: %s, Error: %s\n", filename, err)
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

func KVParse(value string) (string, string, error) {
	objs := strings.Split(value, "=")
	if len(objs) != 2 {
		return "", "", fmt.Errorf("%s", "split with = failed")
	}
	k := strings.TrimSpace(objs[0])
	v := strings.TrimSpace(objs[1])
	return k, v, nil
}

func (cr *ConfigReader) Close() {
	cr.file.Close()
}
