package cache

import (
	"fmt"
	"os"

	"git.jd.com/pid-cdn/ats/lib/ts"
	"github.com/golang/glog"
)

const (
	StoreBlockSize  = 8192
	CacheBlockShift = 9
	CacheBlockSize  = (1 << CacheBlockShift)
	StartBlocks     = 16
	StartPos        = StartBlocks * CacheBlockSize
	StoreBlockShift = 13
)

var (
	gndisk    int
	gdisks    []*CacheDisk
	startDone bool
)

type Processor struct {
}

func roundToStoreBlock(r uint64) uint64 {
	return ts.InkAlign(r, StoreBlockSize)
}

func (*Processor) Start() error {
	ndisks := store.nDisks
	gdisks = make([]*CacheDisk, ndisks)
	gndisk = 0

	// TODO: configure volumn
	for i := 0; i < ndisks; i++ {
		sd := store.disk[i]
		blocks := sd.blocks

		if f, err := os.OpenFile(sd.pathname, os.O_SYNC, 0644); err == nil {
			// TODO: cache config_force_sector_size
			sectorSize := sd.hwSectorSize
			if sectorSize > StoreBlockSize {
				sectorSize = StoreBlockSize
			}

			// TODO: forced_volumn num, hash_base_string

			var r = sd.offset * StoreBlockSize
			if sd.offset*StoreBlockSize < StartPos {
				r = int64(StartPos + sd.alignment)
			}
			skip := roundToStoreBlock(r)
			blocks = blocks - skip>>StoreBlockShift

			gdisks[gndisk] = new(CacheDisk)
			gdisks[gndisk].hashBaseString = sd.hashBaseString

			gdisks[gndisk].open(sd.pathname, blocks, skip, sd.hwSectorSize, f, false)

			gndisk++

			f.Close()
		} else {
			glog.Warning("cache unable to open %s %s", sd.pathname, err)
		}

	}

	if gndisk == 0 {
		return fmt.Errorf("unable to open cache disks: Cache disabled")
	}

	startDone = 1

	return nil
}
