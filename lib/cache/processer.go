package cache

import (
	"fmt"
	"github.com/weiwei99/ats/lib/conf"
)

type CacheProcesser struct {
	GVol       []*Vol
	Store      *Store
	CacheDisks []*CacheDisk
}

func (cp *CacheProcesser) DirCheck(afix bool) int {
	for _, v := range cp.GVol {
		v.DirCheck(afix)
	}
	return 0
}

func NewCacheProcesser(config *conf.ATSConfig) (*CacheProcesser, error) {
	cp := &CacheProcesser{
		CacheDisks: make([]*CacheDisk, 0),
	}

	store, err := NewStore(config) //
	if err != nil {
		return nil, err
	}
	cp.Store = store
	return cp, nil
}

func (cp *CacheProcesser) Start() error {
	return cp.StartInternal(0)
}

//
func (cp *CacheProcesser) StartInternal(flag int) error {

	// 对应CacheInit
	err := cp.Store.LoadConfig() // 只是为了根据storage的配置，设置路径
	if err != nil {
		return err
	}

	//
	for _, v := range cp.Store.Spans {
		// 根据span配置生成cachedisk对象
		cd, err := NewCacheDisk(v.Path, cp.Store.Config) // 需要路径和ats的配置
		if err != nil {
			return err
		}
		// 利用layout分析器，完善cachedisk数据
		//config := disklayout.Config{}
		//lo := disklayout.NewLayout(cd, &config)
		//err = lo.ParseLevel1()
		//if err != nil {
		//		return err
		//	}
		cp.CacheDisks = append(cp.CacheDisks, cd)
	}
	if len(cp.CacheDisks) == 0 {
		return fmt.Errorf("%s", "construct no cache disk")
	}
	return nil
}
