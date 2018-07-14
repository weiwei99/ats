package cache

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
	"path/filepath"
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
	//
	for _, span := range cp.Store.Spans {
		path := span.Path
		// 处理目录情况
		if !span.FilePathName {
			path = filepath.Join(path, "cache.db")
		}

		// 生成CacheDisk
		sectorSize := span.HWSectorSize

		if span.HWSectorSize <= 0 || sectorSize > STORE_BLOCK_SIZE {
			glog.Infof("resetting hardware sector size from %d to %d", sectorSize, STORE_BLOCK_SIZE)
			sectorSize = STORE_BLOCK_SIZE
		}

		skip := START_POS + span.Alignment
		blocks := span.Blocks - (int64(skip) >> STORE_BLOCK_SHIFT)

		// 根据span配置生成cachedisk对象
		cd, err := NewCacheDisk(span, cp.Store.Config) // 需要路径和ats的配置
		if err != nil {
			return err
		}
		err = cd.Open(path, blocks, int64(skip), int(sectorSize), 0, false)
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

func (cp *CacheProcesser) FindURL(url string) *Doc {

	// TODO: 根据其他信息，应该一次定位到url位于哪个cachedisk(其实准确来说，应该是vol)
	doc, err := cp.CacheDisks[0].FindURL(url)
	if err != nil {
		fmt.Printf("find failed: %s\n", err)
		return nil
	}
	return doc
}
