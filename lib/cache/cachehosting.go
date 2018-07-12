package cache

import "github.com/golang/glog"

const (
	VOL_HASH_TABLE_SIZE = 32707
	VOL_HASH_ALLOC_SIZE = (8 * 1024 * 1024)
	VOL_HASH_EMPTY      = 0xFFFF
)

type CacheHostTable struct {
	CacheType  int // 缓存类型
	NumEntries int
	GenHostRec CacheHostRecord
}

//
type CacheHostRecord struct {
	CacheType      int
	Vols           []*Vol      `json:"-"`
	GoodNumVols    int         `json:"good_num_vols"`
	NumVols        int         `json:"num_vols"`
	NumInitialized int         `json:"num_initialized"`
	NumCacheVols   int         `json:"num_cache_vols"`
	CacheVol       []*CacheVol `json:"-"`
	VolHashTable   []uint16    `json:"-"` // url计算hash, 截取hash，在VolHashTable，找到vol编码，根据VOL编号，找到VOL
}

type HostMatcher struct {
}
type HostTable struct {
}

func (ht *HostTable) BuildTable(config_file_path string) {

}
func (ht *HostTable) BuildTableFromString(config_file_path, str string) {

}

//func NewHostRecord(cvls []*CacheVol, cacheType int) *HostRecord {
//
//	hr := &HostRecord{
//		Vols: make([]*Vol, 0),
//	}
//
//	// 从cacheVol中针对type重新组建
//	var tmpCacheVol []*CacheVol
//	var tmpVol []*Vol
//
//	for _, cvl := range cvls {
//		if cvl.Scheme == cacheType {
//			hr.NumCacheVols += 1
//			hr.NumVols += len(cvl.Vols)
//			tmpCacheVol = append(tmpCacheVol, cvl)
//		}
//	}
//
//	for i := 0; i < hr.NumCacheVols; i++ {
//		for j := 0; j < hr.NumVols; j++ {
//			tmpVol = append(tmpVol, tmpCacheVol[i].Vols[j])
//		}
//	}
//	//
//	hr.buildVolHashTable()
//
//	return hr
//}

//func (ht *HostTable) Match(rdata string, CacheHostResult *result) {
//
//}

// explicit pair for random table in build_vol_hash_table
type rtablePair struct {
	rval uint ///< relative value, used to sort.
	idx  uint ///< volume mapping table index.
}

// 一致性hash
// c: build_vol_hash_table
func (hr *CacheHostRecord) buildVolHashTable() {
	var total uint64
	remainVol := hr.NumVols

	mapping := make([]uint, remainVol)
	p := make([]*Vol, remainVol)

	badVols := 0
	realMap := 0
	// initialize number of elements per vol
	for i, vol := range hr.Vols {
		// todo: 此处应去掉磁盘bad的情况
		// if DISK_BAD
		//	 badVols++
		//   continue
		mapping[realMap] = uint(i)
		p[realMap] = hr.Vols[i]
		realMap++
		total += uint64(vol.Len >> STORE_BLOCK_SHIFT)
	}

	remainVol -= badVols
	// TODO: 处理对于磁盘均坏掉的情况下的逻辑
	if total == 0 || remainVol == 0 {
		return
	}

	var used uint
	forvol := make([]uint, remainVol)         //
	gotvol := make([]uint, remainVol)         // 用于计数，某个idx，分配了几个单元
	rnd := make([]uint, remainVol)            // 随机数
	rtable_entries := make([]uint, remainVol) // 记录一个vol可以被划分为几个hash单元
	var rtable_size uint

	// estimate allocation
	for i := 0; i < remainVol; i++ {
		forvol[i] = VOL_HASH_TABLE_SIZE * (uint(p[i].Len) >> STORE_BLOCK_SHIFT) / uint(total) // 某个vol应该占有多少vol_hash_table
		used += forvol[i]                                                                     // 统计被占有的和
		rtable_entries[i] = uint(p[i].Len) / VOL_HASH_ALLOC_SIZE                              // rtable_entries,该vol所容纳rtable_entries个数
		rtable_size += rtable_entries[i]
		gotvol[i] = 0
	}
	// spread around the excess
	extra := VOL_HASH_TABLE_SIZE - used // 由于estimate allocation阶段的除法，导致会有extra存在
	for i := 0; i < int(extra); i++ {
		forvol[i%remainVol]++ // 取余分配在各个forvol中，确保VOL_HASH_TABLE_SIZE上的每个值，都会被forvol统计到
	}

	// seed random number generator
	for i := 0; i < remainVol; i++ {
		// 每个vol有自己独有的hash_id，意味着，每个vol构建的rnd数字起源相同，由于后面用到的next_rand函数算法原因
		// 造成，对于每个vol，每次程序重启后运行，rand相应位置上的值，均能保持不变
		//uint64_t x = p[i]->hash_id.fold();
		//rnd[i] = (unsigned int)x;
		rnd[i] = 1
	}

	ttable := make([]uint16, VOL_HASH_TABLE_SIZE) // 最终的hash一致性table
	// initialize table to "empty"
	for i, _ := range ttable {
		ttable[i] = VOL_HASH_EMPTY
	}
	// generate random numbers proportaion to allocation
	rtable := make([]rtablePair, rtable_size)
	rindex := 0
	for i := 0; i < remainVol; i++ { // 针对所有的vol(完好的），所有的rtable_entries，构建rtablePair
		for j := 0; j < int(rtable_entries[i]); j++ {
			// 类似于c语言中的：rtable[rindex].rval = next_rand(&rnd[i]);
			rtable[rindex].rval = next_rand(rnd[i])
			rnd[i] = rtable[rindex].rval
			rtable[rindex].idx = uint(i)
			rindex++
		}
	}
	// sort (rand #, vol $ pairs)，对rtablePair进行排序

	//
	width := (1 << 32) / VOL_HASH_TABLE_SIZE
	var pos int // target position to allocate
	// select vol with closest random number for each bucket
	var i int = 0 // index moving through the random numbers
	for j := 0; j < VOL_HASH_TABLE_SIZE; j++ {
		pos = width/2 + j*width // position to select closest to
		for {
			if pos > int(rtable[i].rval) && i < int(rtable_size)-1 {
				i++
			} else {
				break
			}
		}
		ttable[j] = uint16(mapping[rtable[i].idx]) // 更新hash环，相应位置的vol索引值,索引值为:mapping[rtable[i].idx]
		gotvol[rtable[i].idx]++                    // 编号为rtable[i].idx的vol(准确说，不是vol)被使用+1，用于计数
	}
	for i := 0; i < remainVol; i++ {
		glog.Errorf("cache_init", "build_vol_hash_table index %d mapped to %d requested %d got %d", i, mapping[i], forvol[i], gotvol[i])
	}
	// install new table
}

func next_rand(p uint) uint {
	seed := 1103515145*p + 12345
	return seed
}
