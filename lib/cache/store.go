/*
Initialization starts with an instance of Store reading the storage configuration file, by default storage.config.

store 之后，就是 span
*/
package cache

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
	"github.com/weiwei99/ats/lib/disk"
)

type Store struct {
	NDisk  uint32          `json:"n_disk"`
	Spans  []*Span         `json:"disk"`
	Config *conf.ATSConfig `json:"-"`
}

//
func GetGeometry() *disk.Geometry {

	geos := make([]*disk.Geometry, 0)

	BigGeo := &disk.Geometry{
		TotalSZ: 6001175126016,
		BlockSZ: 11721045168,
		AlignSZ: 0,
	}
	geos = append(geos, BigGeo)

	SmallGeo := &disk.Geometry{
		TotalSZ: 2147483648,
		BlockSZ: 4194304,
		AlignSZ: 0,
	}
	geos = append(geos, SmallGeo)
	G5Geo := &disk.Geometry{
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
