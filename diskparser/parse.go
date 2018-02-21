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
	for _, v := range cparser.CacheDisks {
		config := disklayout.Config{}
		lo := disklayout.NewLayout(v, &config)
		err := lo.ParseLevel1()
		if err != nil {
			return err
		}
	}
	return nil
}

func (cparser *CacheParser) DumpParser() string {
	return ""
}
