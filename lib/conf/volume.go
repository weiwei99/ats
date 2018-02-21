package conf

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

type VolConfig struct {
	Number    int   `json:"number"`
	Scheme    int   `json:"scheme"`
	Size      int64 `json:"size"`
	InPercent bool  `json:"in_percent"`
	Percent   int   `json:"percent"`
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
