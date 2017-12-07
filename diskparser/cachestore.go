package diskparser

import (
	"bufio"
	"log"
	"os"
)

var store Store

type Store struct {
	nDisks         int
	nDisksInConfig int
	disk           []*Span
}

func (s *Store) readConfig() error {
	nDSStore := 0
	file, err := os.Open("/export/servers/trafficserver/etc/storage.conf")
	if err != nil {
		return err
	}
	defer file.Close()

	size := -1
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		path := scanner.Text()

		if len(path) == 0 || path[0] == '#' {
			continue
		}

		s.nDisksInConfig++
		// TODO: set size
		// TODO: addtitional condition value

		ns := new(Span)
		if err := ns.init(path, size); err != nil {
			continue
		}

		nDSStore++
		s.nDisks = nDSStore
		s.disk = append(s.disk, ns)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	s.sort()
}

func (s *Store) sort() {
	// TODO: sort
}
