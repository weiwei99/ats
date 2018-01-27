package proxy

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type RemapService struct {
	rules map[string]string
}

//
func NewRemapService() *RemapService {
	rs := &RemapService{
		rules: make(map[string]string),
	}
	return rs
}

// 加载规则
func (rs *RemapService) LoadRule(path string) error {
	fi, err := os.Open(path)
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
		bs := strings.Fields(b)
		if bs[0] == "map" {
			rs.rules[bs[1]] = bs[2]
		}
		break
	}
	return nil
}

// 追加规则
func (rs *RemapService) AppendRule(rule string) error {
	b := strings.TrimSpace(string(rule))
	if len(b) < 1 || string(b[0]) == "#" {
		return fmt.Errorf("rule is empty")
	}
	bs := strings.Fields(b)
	if bs[0] == "map" {
		rs.rules[bs[1]] = bs[2]
	}
	return nil
}

// Remap URL
func (rs *RemapService) Remap(urlStr string) string {
	for from, to := range rs.rules {
		if strings.Contains(urlStr, from) {
			return strings.Replace(urlStr, from, to, -1)
		}
	}
	return urlStr
}

func (rs *RemapService) Dump() string {
	var retval string
	for from, to := range rs.rules {
		retval += fmt.Sprintf("[%s]-> %s", from, to)
	}
	return retval
}
