package diskparser

import (
	"fmt"
	"github.com/weiwei99/ats/lib/cache"
	"github.com/weiwei99/ats/lib/conf"
)

type CacheParser struct {
	Paths []string
	//CacheDisks []*cache.CacheDisk
	Conf      *conf.ATSConfig
	Store     *cache.Store
	Processor *cache.CacheProcesser
}

//
func NewCacheParser(atsconf *conf.ATSConfig) (*CacheParser, error) {
	cp := &CacheParser{}

	processor, err := cache.NewCacheProcesser(atsconf)
	if err != nil {
		return nil, err
	}
	err = processor.Start()
	if err != nil {
		return nil, err
	}
	cp.Processor = processor
	return cp, nil
}

//
func (cparser *CacheParser) ParseMain(path string) error {
	return nil
}

//
func (cparser *CacheParser) MainParse() error {
	// 分析Cache结构
	for _, cd := range cparser.Processor.CacheDisks {
		err := cd.OpenStart()
		if err != nil {
			return err
		}
		for i := 0; i < int(cd.Header.NumVolumes); i++ {
			vol, err := cache.NewVolFromDisk(cd, cd.Header.VolInfos[i])
			if err != nil {
				fmt.Println(err)
				return err
			}
			// 一个cd里面，应该有多个vol吧？？？
			cd.YYVol = vol
		}
	}
	// 创建CacheVol,等价于cplist_xxx
	// create the cachevol list only if num volumes are greater
	// than 0.
	//cvm := cache.NewCacheVolumes(cparser.CacheDisks, cparser.Conf.ConfigVolumes)
	//if cparser.Conf.ConfigVolumes.NumVolumes == 0 {
	//	cvm.Reconfigure()
	//	/* if no volumes, default to just an http cache */
	//} else {
	//	// else
	//	// create the cachevol list.
	//	err := cvm.Init()
	//	if err != nil {
	//		return err
	//	}
	//	/* now change the cachevol list based on the config file */
	//	cvm.Reconfigure()
	//}

	// 建立Cache对象HASH表

	//cache.NewHostRecord()
	return nil
}

func (cparser *CacheParser) DumpParser() string {
	return ""
}
