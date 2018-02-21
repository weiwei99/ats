package disklayout

import (
	"fmt"
	"github.com/weiwei99/ats/lib/cache"
)

// 影响布局的配置信息
type Config struct {
}

// 磁盘布局信息
type Layout struct {
	CacheDisk *cache.CacheDisk //
	Config    *Config
	StartPos  int               `json:"start_pos"` // 数据起始位置
	Header    *cache.DiskHeader // 磁盘头
	Vols      *cache.Vol        // vol信息
	Dir       *cache.Dir        // Dir信息
}

func NewLayout(cd *cache.CacheDisk, config *Config) *Layout {

	lo := &Layout{
		CacheDisk: cd,
		Config:    config,
	}

	return lo
}

// 分析主要结构，包括： DiskHeader, VolInfo, Dir
func (lo *Layout) ParseLevel1() error {
	cd := lo.CacheDisk

	// 分析磁盘描述头
	err := lo.parseCacheDiskHeader()
	if err != nil {
		return err
	}

	// 分析Vol,一定要用NumVolumes，不能枚举VolInfos，因为VolInfos是全部空间
	for i := 0; i < int(cd.Header.NumVolumes); i++ {
		_, err := lo.parseVol(cd.Header.VolInfos[i])
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	//cd.YYVol = vol

	return nil
}
