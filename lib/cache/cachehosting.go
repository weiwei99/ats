package cache

import "github.com/golang/glog"

const (
	VOL_HASH_TABLE_SIZE = 32707
	VOL_HASH_ALLOC_SIZE = (8 * 1024 * 1024)
	VOL_HASH_EMPTY      = 0xFFFF
)

type HostRecord struct {
	GoodNumVols  int    `json:"good_num_vols"`
	NumVols      int    `json:"num_vols"`
	NumCacheVols int    `json:"num_cache_vols"`
	Vols         []*Vol `json:"-"`
}
type HostMatcher struct {
}
type HostTable struct {
}

func (ht *HostTable) BuildTable(config_file_path string) {

}
func (ht *HostTable) BuildTableFromString(config_file_path, str string) {

}

func NewHostRecord(cvls []*CacheVol, cacheType int) *HostRecord {

	hr := &HostRecord{
		Vols: make([]*Vol, 0),
	}

	// 从cacheVol中针对type重新组建
	var tmpCacheVol []*CacheVol
	var tmpVol []*Vol

	for _, cvl := range cvls {
		if cvl.Scheme == cacheType {
			hr.NumCacheVols += 1
			hr.NumVols += len(cvl.Vols)
			tmpCacheVol = append(tmpCacheVol, cvl)
		}
	}

	for i := 0; i < hr.NumCacheVols; i++ {
		for j := 0; j < hr.NumVols; j++ {
			tmpVol = append(tmpVol, tmpCacheVol[i].Vols[j])
		}
	}
	//
	hr.buildVolHashTable()

	return hr
}

//func (ht *HostTable) Match(rdata string, CacheHostResult *result) {
//
//}

// explicit pair for random table in build_vol_hash_table
type rtablePair struct {
	rval uint ///< relative value, used to sort.
	idx  uint ///< volume mapping table index.
}

func (hr *HostRecord) buildVolHashTable() {
	var total uint64
	mapping := make([]uint, hr.NumVols)

	// initialize number of elements per vol
	for i, vol := range hr.Vols {
		// todo: 此处应去掉磁盘bad的情况
		// if DISK_BAD
		//   continue
		mapping[i] = uint(i)
		total += uint64(vol.Len >> STORE_BLOCK_SHIFT)
	}

	var used uint
	p := hr.Vols
	forvol := make([]uint, hr.NumVols)
	gotvol := make([]uint, hr.NumVols)
	rnd := make([]uint, hr.NumVols)
	rtable_entries := make([]uint, hr.NumVols)
	var rtable_size uint

	// estimate allocation
	for i := 0; i < hr.NumVols; i++ {
		forvol[i] = VOL_HASH_TABLE_SIZE * (uint(p[i].Len) >> STORE_BLOCK_SHIFT) / uint(total)
		used += forvol[i]
		rtable_entries[i] = uint(p[i].Len) / VOL_HASH_ALLOC_SIZE
		rtable_size += rtable_entries[i]
		gotvol[i] = 0
	}
	// spread around the excess
	extra := VOL_HASH_TABLE_SIZE - used
	for i := 0; i < int(extra); i++ {
		forvol[i%hr.NumVols]++
	}
	// seed random number generator
	for i := 0; i < hr.NumVols; i++ {
		//uint64_t x = p[i]->hash_id.fold();
		//rnd[i] = (unsigned int)x;

		rnd[i] = 1
	}

	ttable := make([]uint16, VOL_HASH_TABLE_SIZE)

	for i, _ := range ttable {
		ttable[i] = VOL_HASH_EMPTY
	}

	var rtable []rtablePair
	width := (1 << 32) / VOL_HASH_TABLE_SIZE
	var pos int // target position to allocate
	var i int

	for j := 0; j < VOL_HASH_TABLE_SIZE; j++ {
		pos = width/2 + j*width
		for {
			if pos > int(rtable[i].rval) && i < int(rtable_size)-1 {
				i++
			} else {
				break
			}
		}
		ttable[j] = uint16(mapping[rtable[i].idx])
		gotvol[rtable[i].idx]++
	}
	for i := 0; i < hr.NumVols; i++ {
		glog.Errorf("cache_init", "build_vol_hash_table index %d mapped to %d requested %d got %d", i, mapping[i], forvol[i], gotvol[i])
	}

}

func next_rand(p uint) uint {
	seed := 1103515145*p + 12345
	return seed
}
