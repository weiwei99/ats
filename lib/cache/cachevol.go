/*
the resulting storage is distributed across the volumes in cplist_reconfigure().
The CacheVol instances are created at this time.
*/
package cache

import "github.com/weiwei99/ats/lib/conf"

// 逻辑上的Vol
type CacheVol struct {
	VolNumber int               `json:"vol_number"`
	Scheme    int               `json:"scheme"`
	Size      int               `json:"size"`
	Vols      []*Vol            `json:"-"`
	DiskVols  []*DiskVol        `json:"-"`
	ConfigVol []*conf.VolConfig `json:"-"`
}

//
func NewCacheVol(vc *conf.VolConfig) (*CacheVol, error) {
	cv := &CacheVol{}
	return cv, nil
}
