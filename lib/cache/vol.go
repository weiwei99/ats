package cache

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
)

const (
	VOL_MAGIC uint32 = 0xF1D0F00D // 出现在 00fe00, 0010000，4754000， 4756000， 8e9a000
	DOC_MAGIC uint32 = 0x5F129B13

	SIZEOF_DIR             = 10
	SIZEOF_VolHeaderFooter = 80
)

type VolHeaderFooter struct {
	Magic             uint32        `json:"magic"`
	Version           VersionNumber `json:"version"`
	CreateTime        uint64        `json:"create_time"`
	WritePos          int64         `json:"write_pos"`
	LastWritePos      int64         `json:"last_write_pos"`
	AggPos            int64         `json:"agg_pos"`
	Generation        uint32        `json:"generation"`
	Phase             uint32        `json:"phase"`
	Cycle             uint32        `json:"cycle"`
	SyncSerial        uint32        `json:"sync_serial"`
	WriteSerial       uint32        `json:"write_serial"`
	Dirty             uint32        `json:"dirty"`
	SectorSize        uint32        `json:"sector_size"`
	Unused            uint32        `json:"unused"`
	FreeList          []uint16      `json:"free_list"`
	AnalyseDiskOffset int64         `json:"analyse_disk_offset"`
}

// Vol的基本配置信息
type VolConfig struct {
	VolInfo              *DiskVolBlock
	MinAverageObjectSize int
}

// 物理上的vol，即span
type Vol struct {
	Path                   string           `json:"path"`
	Dir                    [][][]*Dir       `json:"-"`
	DirPos                 int64            `json:"dir_pos"`
	Header                 *VolHeaderFooter `json:"header"`
	Footer                 *VolHeaderFooter `json:"footer"`
	Segments               int              `json:"segments"`
	Buckets                int64            `json:"buckets"`
	RecoverPos             int64            `json:"recover_pos"`
	PrevRecoverPos         int64            `json:"prev_recover_pos"`
	ScanPos                int64            `json:"scan_pos"`
	Skip                   int64            `json:"skip"`  // start to headers
	Start                  int64            `json:"start"` // start of data
	Len                    int64            `json:"len"`
	DataBlocks             int64            `json:"data_blocks"`
	AggBufPos              int              `json:"agg_buf_pos"`
	YYMinAverageObjectSize int              `json:"yy_min_average_object_size"`
	HashText               string           `json:"hash_text"`
	HitEvacuateWindow      int
	//Disk                   *CacheDisk
	Config          *VolConfig
	Conf            *conf.ATSConfig `json:"-"`
	ContentStartPos int64           `json:"content_start_pos"`

	YYFullDir  []*Dir `json:"-"`
	YYStaleDir []*Dir `json:"-"`

	Content []*Doc `json:"-"`
}

//
func NewVol(config *VolConfig) (*Vol, error) {
	v := &Vol{
		Config: config,
	}
	v.Len = int64(config.VolInfo.Len * STORE_BLOCK_SIZE)
	v.Skip = int64(config.VolInfo.Offset)
	v.PrevRecoverPos = 0
	v.Start = int64(config.VolInfo.Offset)

	v.YYMinAverageObjectSize = config.MinAverageObjectSize
	// 分析大小
	v.initData()
	v.DataBlocks = v.Len - (v.Start-v.Skip)/STORE_BLOCK_SIZE
	cache_config_hit_evacuate_percent := 10
	v.HitEvacuateWindow = int(v.DataBlocks) * cache_config_hit_evacuate_percent / 100

	v.HashText = fmt.Sprintf("%s %d:%d", "/dev/sdb", v.Skip, v.Config.VolInfo.Len)
	glog.Errorf("%s", v.HashText)
	return v, nil
}

func (v *Vol) initData() {
	v.initDataInternal()
	v.initDataInternal()
	v.initDataInternal()
	v.allocDir()
}

func (v *Vol) initDataInternal() {
	//var cache_config_min_average_object_size int64 = 512000 //8000
	//var cache_config_min_average_object_size int64 = 8000
	v.Buckets = (v.Len - (v.Start - v.Skip)) / int64(v.YYMinAverageObjectSize) / DIR_DEPTH
	v.Segments = int((v.Buckets + (((1 << 16) - 1) / DIR_DEPTH)) / ((1 << 16) / DIR_DEPTH))
	v.Buckets = (v.Buckets + int64(v.Segments) - 1) / int64(v.Segments)
	v.Start = v.Skip + 2*int64(v.DirLen())
}

func (v *Vol) HeaderLen() int {
	return RoundToStoreBlock(SIZEOF_VolHeaderFooter + 2*(v.Segments-1))
}

func (v *Vol) DirLen() int {
	// TODO: d.Buckets 这个要改
	return v.HeaderLen() +
		RoundToStoreBlock(int(v.Buckets)*DIR_DEPTH*v.Segments*SIZEOF_DIR) +
		RoundToStoreBlock(SIZEOF_VolHeaderFooter)
}

func (v *Vol) DirEntries() int {
	return int(v.Buckets) * DIR_DEPTH * v.Segments
}

// 申请Dir空间
func (v *Vol) allocDir() {
	// 预申请空间
	v.Dir = make([][][]*Dir, v.Segments)
	for idxs, _ := range v.Dir {
		v.Dir[idxs] = make([][]*Dir, v.Buckets)
		for idxb, _ := range v.Dir[idxs] {
			v.Dir[idxs][idxb] = make([]*Dir, DIR_DEPTH)
			for idxd, _ := range v.Dir[idxs][idxb] {
				v.Dir[idxs][idxb][idxd] = &Dir{
					Index: &DirPos{
						Segment: idxs,
						Bucket:  idxb,
						Depth:   idxd,
						Vol:     v,
					},

					Next: 0,
				}
			}
		}
	}
}

func (v *Vol) DirCheck(afix bool) int {
	HIST_DEPTH := 8
	hist := make([]int, HIST_DEPTH)
	shist := make([]int, v.Segments)

	v.YYStaleDir = make([]*Dir, 0)
	v.YYFullDir = make([]*Dir, 0)
	var empty, stale, full, last, free int
	for s := 0; s < v.Segments; s++ {
		seg := v.DirSegment(s)
		for b := 0; b < int(v.Buckets); b++ {
			ee := v.Dir[s][b][0]
			h := 0
			for {
				if ee.Offset == 0 {
					empty += 1
				} else {
					h += 1
					if !v.DirValid(ee) {
						stale += 1
						v.YYStaleDir = append(v.YYStaleDir, ee)
					} else {
						full += 1
						v.YYFullDir = append(v.YYFullDir, ee)
					}
				}
				ee = v.NextDir(ee, seg)
				if ee == nil {
					break
				}
			}
			if h > HIST_DEPTH {
				h = HIST_DEPTH
			}
			hist[h] += 1
		}
		t := stale + full
		shist[s] = t - last
		last = t
		free += v.DirFreelistLength(s)
	}
	//fmt.Printf(" Directory for [%s %d:%d]\n", v.Disk.Path, v.Disk.Header.VolInfo.Offset, v.Disk.Header.VolInfo.Len)
	fmt.Printf(" Bytes: [%d]\n", v.Buckets*int64(v.Segments)*DIR_DEPTH*SIZEOF_DIR)
	fmt.Printf(" Segments: %d\n", int64(v.Segments))
	fmt.Printf(" Buckets %d\n", v.Buckets)
	fmt.Printf(" Entries: %d\n", v.Buckets*int64(v.Segments)*DIR_DEPTH)
	fmt.Printf(" Full: %d\n", full)
	fmt.Printf(" Empty: %d\n", empty)
	fmt.Printf(" Stale: %d\n", stale)
	fmt.Printf(" Free %d\n", free)
	fmt.Printf("       Bucket Fullness:  ")
	for j := 0; j < HIST_DEPTH; j++ {
		fmt.Printf("%d ", hist[j])
		if j%4 == 3 {
			fmt.Printf("\n                         ")
		}
	}
	fmt.Printf("\n")
	fmt.Printf("        Segment Fullness:  ")
	for j := 0; j < v.Segments; j++ {
		fmt.Printf("%d ", shist[j])
		if j%5 == 4 {
			fmt.Printf("\n                         ")
		}
	}
	fmt.Printf("\n")
	fmt.Printf("        Freelist Fullness:  ")
	for j := 0; j < v.Segments; j++ {

		fmt.Printf("%d ", v.DirFreelistLength(j))
		if j%5 == 4 {
			fmt.Printf("\n")
		}
	}
	return 0
}

func (v *Vol) CheckDir() bool {
	for s := 0; s < v.Segments; s++ {
		sdir := v.DirSegment(s)
		for b := 0; b < int(v.Buckets); b++ {
			bdir := v.DirBucket(b, sdir)
			if v.DirBucketLen(bdir, s) < 0 {
				return false
			}
			if !(bdir.Next == 0 || bdir.Offset != 0) {
				return false
			}
		}
	}
	return true
}

func (v *Vol) DirBucketLen(b *Dir, s int) int {
	sdir := v.DirSegment(s)
	e := b

	for max := 100; max > 0; max-- {
		e = v.NextDir(e, sdir)
		if e == nil {
			return 100 - max + 1
		}
	}
	return -1
}
func (v *Vol) DirValid(e *Dir) bool {
	if v.Header.Phase == uint32(e.Phase) {
		// vol_in_phase_valid
		return int64(e.Offset-1) < (v.Header.WritePos+int64(v.AggBufPos)-v.Start)/CACHE_BLOCK_SIZE
	} else {
		// vol_out_of_phase_valid
		return int64(e.Offset-1) >= (v.Header.AggPos-v.Start)/CACHE_BLOCK_SIZE
	}
}

//func (v *Vol) DirFreelistLength(s int) int {
//	//seg := v.DirSegment(s)
//
//}

func (v *Vol) NextDir(dir *Dir, seg *Dir) *Dir {
	if dir.Next == 0 {
		return nil
	}

	if DIR_DEPTH < 5 {
		return v.DirInSeg(seg, int(dir.Next))
	} else {
		panic("zhuyu")
	}
}

func (v *Vol) DirNext(d *Dir) int {
	return int(d.Next)
}

func (v *Vol) DirSegment(s int) *Dir {
	return v.Dir[s][0][0]
}

func (v *Vol) DirBucketRow(b *Dir, i int) *Dir {
	return v.DirInSeg(b, i)
}

func (v *Vol) DirInSeg(dir *Dir, i int) *Dir {
	s, b, d := dir.Index.Segment, dir.Index.Bucket, dir.Index.Depth
	pos := s*int(v.Buckets)*DIR_DEPTH + b*DIR_DEPTH + d + i

	if pos > v.Segments*int(v.Buckets)*DIR_DEPTH {
		fmt.Printf("exception out of length: %d, max: %d\n", pos, v.DirEntries())
		return nil
	}
	s1 := pos / (int(v.Buckets) * DIR_DEPTH)
	b1 := (pos % (int(v.Buckets) * DIR_DEPTH)) / DIR_DEPTH
	d1 := (pos % (int(v.Buckets) * DIR_DEPTH)) % DIR_DEPTH

	return v.Dir[s1][b1][d1]
}

func (v *Vol) DirFreelistLength(s int) int {
	free := 0
	seg := v.DirSegment(s)
	e := v.DirInSeg(seg, int(v.Header.FreeList[s]))

	for {
		if e == nil {
			return free
		}
		e = v.NextDir(e, seg)
		free += 1
	}

}

//
func (v *Vol) DirProbe(key []byte) (*Dir, **Dir) {

	if len(key) != 16 {
		return nil, nil
	}
	s := binary.LittleEndian.Uint32(key[0:4]) % uint32(v.Segments)
	b := binary.LittleEndian.Uint32(key[4:8]) % uint32(v.Buckets)
	DIR_TAG_WIDTH := 12
	seg := v.DirSegment(int(s))
	e := v.DirBucket(int(b), seg)
	fmt.Printf("dirprobe....%d, %d\n", s, b)
	fmt.Println(key)
	estr, _ := json.Marshal(e)
	fmt.Printf("e: %s\n", estr)

	if e.Offset != 0 {
		for {
			b := binary.LittleEndian.Uint32(key[8:12]) & ((1 << uint(DIR_TAG_WIDTH)) - 1)
			fmt.Printf("%s vs %s\n", uint32(e.Tag), b)
			if uint32(e.Tag) == b {
				// 检测碰撞

				if v.DirValid(e) {
					return e, nil
				} else {
					// delete the invalid entry
					continue
				}
			} else {

			}
			fmt.Println("try next")
			e = v.NextDir(e, seg)
		}
	}
	return nil, nil
}

func (v *Vol) DirBucket(b int, s *Dir) *Dir {
	return v.Dir[s.Index.Segment][b][0]
}
