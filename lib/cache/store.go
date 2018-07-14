/*
Initialization starts with an instance of Store reading the storage configuration file, by default storage.config.

store 之后，就是 span
*/
package cache

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

//
func NewStore(config *conf.ATSConfig) (*Store, error) {

	store := &Store{
		Config: config,
	}
	err := store.loadConfig()
	if err != nil {
		return nil, err
	}
	return store, nil
}

//
func (store *Store) loadConfig() error {
	for _, v := range store.Config.Storages {
		sp, err := NewSpan(v)
		if err != nil {
			glog.Errorf("load disk [%s] %s failed", v.Path, err.Error())
			continue
		}
		store.Spans = append(store.Spans, sp)
	}
	if len(store.Spans) == 0 {
		return fmt.Errorf("%s", "can not found any span")
	}
	return nil
}

func (store *Store) TotalBlocks() int64 {
	var t int64 = 0
	for _, s := range store.Spans {
		t += s.TotalBlocks()
	}
	return t
}
