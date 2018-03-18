package diskparser

import (
	"github.com/weiwei99/ats/lib/cache"
	"github.com/weiwei99/ats/lib/conf"
	"github.com/weiwei99/ats/lib/disklayout"
)

type CacheParser struct {
	Paths      []string
	CacheDisks []*cache.CacheDisk
	Conf       *conf.ATSConfig
}

func NewCacheParser(atsconf *conf.ATSConfig) (*CacheParser, error) {
	cp := &CacheParser{
		CacheDisks: make([]*cache.CacheDisk, 0),
	}

	for _, v := range atsconf.Storages {
		cdisk, err := cache.NewCacheDisk(v, atsconf)
		if err != nil {
			return nil, err
		}
		cp.CacheDisks = append(cp.CacheDisks, cdisk)
	}

	return cp, nil
}

func (cparser *CacheParser) ParseMain(path string) error {
	return nil
}

func (cparser *CacheParser) MainParse() error {
	// 分析Cache结构
	for _, v := range cparser.CacheDisks {
		config := disklayout.Config{}
		lo := disklayout.NewLayout(v, &config)
		err := lo.ParseLevel1()
		if err != nil {
			return err
		}
	}
	// 创建CacheVol,等价于cplist_xxx
	// create the cachevol list only if num volumes are greater
	// than 0.
	cvm := cache.NewCacheVolumes(cparser.CacheDisks, cparser.Conf.ConfigVolumes)
	if cparser.Conf.ConfigVolumes.NumVolumes == 0 {
		cvm.Reconfigure()
		/* if no volumes, default to just an http cache */
	} else {
		// else
		// create the cachevol list.
		err := cvm.Init()
		if err != nil {
			return err
		}
		/* now change the cachevol list based on the config file */
		cvm.Reconfigure()
	}

	// 建立Cache对象HASH表

	//cache.NewHostRecord()
	return nil
}

func (cparser *CacheParser) DumpParser() string {
	return ""
}
