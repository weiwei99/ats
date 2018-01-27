package diskparser

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
)

type Store struct {
	NDisk  uint32          `json:"n_disk"`
	Spans  []*Span         `json:"disk"`
	Config *conf.ATSConfig `json:"-"`
}

type Span struct {
	Blocks          int64    `json:"blocks"`
	Offset          int64    `json:"offset"`
	HWSectorSize    uint32   `json:"hw_sector_size"`
	Alignment       uint32   `json:"alignment"`
	Path            string   `json:"path"`
	DiskId          [16]byte //
	ForcedVolumeNum int      `json:"forced_volume_num"`
}

func NewSpan(path string) (*Span, error) {
	sp := &Span{
		ForcedVolumeNum: -1,
		HWSectorSize:    256,
		Path:            path,
	}
	// 填充BLOCKS等信息

	return sp, nil
}

func (span *Span) TotalBlocks() int {

	return 0
}

//
func getGeometry() *Geometry {

	geos := make([]*Geometry, 0)

	BigGeo := &Geometry{
		TotalSZ: 6001175126016,
		BlockSZ: 11721045168,
		AlignSZ: 0,
	}
	geos = append(geos, BigGeo)

	SmallGeo := &Geometry{
		TotalSZ: 2147483648,
		BlockSZ: 4194304,
		AlignSZ: 0,
	}
	geos = append(geos, SmallGeo)
	G5Geo := &Geometry{
		TotalSZ: 5368709120,
		BlockSZ: 10485760,
		AlignSZ: 0,
	}
	geos = append(geos, G5Geo)

	return geos[1]
}

//
func NewStore(config *conf.ATSConfig) (*Store, error) {

	store := &Store{
		Config: config,
	}

	return store, nil
}

//
func (store *Store) LoadConfig() error {
	for _, v := range store.Config.Storages {
		sp, err := NewSpan(v)
		if err != nil {
			glog.Errorf("load disk %s failed", v)
			continue
		}
		store.Spans = append(store.Spans, sp)
	}
	if len(store.Spans) == 0 {
		return fmt.Errorf("%s", "can not found any span")
	}
	return nil
}

func (store *Store) TotalBlocks() int {
	t := 0
	for _, s := range store.Spans {
		t += s.TotalBlocks()
	}
	return t
}
