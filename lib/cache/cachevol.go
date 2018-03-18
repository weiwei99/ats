/*
the resulting storage is distributed across the volumes in cplist_reconfigure().
The CacheVol instances are created at this time.
*/
package cache

import (
	"fmt"
	"github.com/weiwei99/ats/lib/conf"
)

// 逻辑上的Vol
type CacheVol struct {
	VolNumber int             `json:"vol_number"` // vol编号
	Scheme    uint8           `json:"scheme"`     // 支持scheme类型
	Size      uint64          `json:"size"`       // 大小
	NumVols   int             `json:"num_vols"`   // 有多少vols
	Vols      []*Vol          `json:"-"`          // vol结构
	DiskVols  []*DiskVol      `json:"-"`          // 磁盘的stripe
	ConfigVol *conf.ConfigVol `json:"-"`          //
}

// cp_list
type CacheVolumeManager struct {
	Config     *conf.ConfigVolumes
	CacheVol   map[int]*CacheVol //
	CacheDisks []*CacheDisk      //
}

func NewCacheVolumes(gndisk []*CacheDisk, config *conf.ConfigVolumes) *CacheVolumeManager {
	cvm := &CacheVolumeManager{
		CacheDisks: gndisk,
		CacheVol:   make(map[int]*CacheVol, 0),
	}

	return cvm
}

// 初始化
// 将多块磁盘上，多个diskvol，组织到多个cacheVol中去
//
func (cv *CacheVolumeManager) Init() error {
	for diskIdx, cd := range cv.CacheDisks {
		for stripIdx, dv := range cd.DiskVols {
			//
			if _, found := cv.CacheVol[dv.VolNumber]; found {
				// 找到VolNumber一致的cachevol，将diskvol追加到cachevol里
				if cv.CacheVol[dv.VolNumber].Scheme != dv.Disk.Header.VolInfos[stripIdx].Type {
					return fmt.Errorf("scheme not match: %d vs %d",
						cv.CacheVol[dv.VolNumber].Scheme,
						dv.Disk.Header.VolInfos[stripIdx].Type)
				}
				cv.CacheVol[dv.VolNumber].Size += dv.Size
				cv.CacheVol[dv.VolNumber].DiskVols[diskIdx] = &dv
				cv.CacheVol[dv.VolNumber].NumVols += dv.NumVolBlocks
				continue
			}
			// 没有找到对应的CacheVol，则创建一个
			ncv, err := NewCacheVol(nil)
			if err != nil {
				return err
			}
			cv.CacheVol[dv.VolNumber] = ncv
			cv.CacheVol[dv.VolNumber].DiskVols = make([]*DiskVol, len(cv.CacheDisks))
			cv.CacheVol[dv.VolNumber].Scheme = dv.Disk.Header.VolInfos[stripIdx].Type
			cv.CacheVol[dv.VolNumber].Size = dv.Size
			cv.CacheVol[dv.VolNumber].DiskVols[diskIdx] = &dv
			cv.CacheVol[dv.VolNumber].NumVols = dv.NumVolBlocks

		}
	}

	return nil
}

func (cv *CacheVolumeManager) Update() {
	/* go through cplist and delete volumes that are not in the volume.config */
}

// 再分配
func (cv *CacheVolumeManager) Reconfigure() error {
	// 针对Volume的配置，有两种配置方式
	if cv.Config.NumVolumes == 0 {
		// 1. volume的配置文件为空，即采用http方式
		/* only the http cache */
		cp, _ := NewCacheVol(nil)
		cp.VolNumber = 0
		cp.Scheme = 1 // CACHE_HTTP_TYPE
		cp.DiskVols = make([]*DiskVol, len(cv.CacheDisks))

		for idx, dv := range cp.DiskVols {
			cp.Size += dv.Size
			cp.DiskVols[idx] = dv
			cp.NumVols += dv.NumVolBlocks
		}
	} else {
		// 2. 遵循volume配置方式

		/* change percentages in the config patitions to absolute value */
		var totalSpaceInBLKs int64
		var blocksPerVol int64 = VOL_BLOCK_SIZE / STORE_BLOCK_SIZE
		/* sum up the total space available on all the disks. round down the space to 128 megabytes */
		for _, cd := range cv.CacheDisks {
			totalSpaceInBLKs += (cd.NumUsableBlocks / blocksPerVol) * blocksPerVol
		}
		percentRemaining := 100.00
		for _, configVolume := range cv.Config.CPQueue {
			if configVolume.InPercent != true {
				continue
			}
			if configVolume.Percent > int(percentRemaining) {
				return fmt.Errorf("%s", "percent config error")
			}
			spaceInBlks := (int64)(float64(configVolume.Percent)/percentRemaining) * totalSpaceInBLKs
			spaceInBlks = spaceInBlks >> (20 - STORE_BLOCK_SHIFT)
			/* round down to 128 megabyte multiple */
			spaceInBlks = (spaceInBlks >> 7) << 7
			configVolume.Size = spaceInBlks
			totalSpaceInBLKs -= spaceInBlks << (20 - STORE_BLOCK_SHIFT)
			if configVolume.Size < 128 {
				percentRemaining -= 0
			} else {
				percentRemaining -= float64(configVolume.Percent)
			}
			if configVolume.Size < 128 {
				return fmt.Errorf("%s", "create volumes failed")
			}
			// 成功创建
		}

		// 2.1 处理配置中有百分比的情况

		cv.Update()

		// 2.2 未知

		// 2.3 处理配置中为SIZE的情况，这种情况相对复杂，因为要将CacheVol的大小按比例分摊到各个Disk中去

	}
}

//
func NewCacheVol(vc *conf.ConfigVol) (*CacheVol, error) {
	cv := &CacheVol{}
	return cv, nil
}
