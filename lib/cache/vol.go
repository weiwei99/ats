package cache

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/weiwei99/ats/lib/conf"
	"time"
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

	Content   []*Doc     `json:"-"`
	CacheDisk *CacheDisk `json:"-"`
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

	v.HashText = fmt.Sprintf("%s %d:%d", "KKKKKKKKKKK", v.Skip, v.Config.VolInfo.Len)
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

func (v *Vol) DirCheck(afix bool) error {
	HIST_DEPTH := 8
	hist := make([]int, HIST_DEPTH+1)
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
	fmt.Printf(" Bytes of Dir: [%d]\n", v.Buckets*int64(v.Segments)*DIR_DEPTH*SIZEOF_DIR)
	fmt.Printf(" Segments: %d\n", int64(v.Segments))
	fmt.Printf(" Buckets %d\n", v.Buckets)
	fmt.Printf(" Total Entries: %d\n", v.Buckets*int64(v.Segments)*DIR_DEPTH)
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
	return nil
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
	fmt.Printf("dirprobe..key %s -> <segment %d, bucket: %d>\n", hex.EncodeToString(key), s, b)

	//estr, _ := json.Marshal(e)
	//fmt.Printf("dir e: %s\n", estr)

	if e.Offset != 0 {
		for {
			b := binary.LittleEndian.Uint32(key[8:12]) & ((1 << uint(DIR_TAG_WIDTH)) - 1)
			if uint32(e.Tag) == b {
				// 检测碰撞

				if v.DirValid(e) {
					return e, nil
				} else {
					// delete the invalid entry
					fmt.Println("delete the invalid entry")
					// todo: 生成新的e
					// e = delete(xx)
					continue
				}
			} else {
				fmt.Printf("dir_probe_tag: tag mismatch %X vs expected %X or %X\n",
					uint32(e.Tag), b, binary.LittleEndian.Uint32(key[8:12]))
			}
			e = v.NextDir(e, seg)
			if e == nil {
				break
			}
		}
	}
	fmt.Println("dir_probe_miss")
	return nil, nil
}

func (v *Vol) DirBucket(b int, s *Dir) *Dir {
	return v.Dir[s.Index.Segment][b][0]
}

func NewVolFromDisk(cd *CacheDisk, block *DiskVolBlock) (*Vol, error) {
	begin := time.Now()

	volConfig := &VolConfig{
		MinAverageObjectSize: cd.AtsConf.MinAverageObjectSize,
		VolInfo:              block,
	}
	vol, err := NewVol(volConfig)
	if err != nil {
		return nil, fmt.Errorf("create vol failed: %s", err.Error())
	}
	vol.CacheDisk = cd

	// 分析headers（包含header, footer)
	err = vol.loadVolHeaders()
	if err != nil {
		return nil, fmt.Errorf("vol header read failed: %s", err.Error())
	}

	// 分析freelist
	vol.Header.FreeList = make([]uint16, vol.Segments)
	// Freelist在 80-72的位置
	freelistBufPos := vol.Header.AnalyseDiskOffset + (SIZEOF_VolHeaderFooter - 8)
	freelistBuf, err := cd.Dio.Read(freelistBufPos, int64(vol.Segments)*2)
	for i := 0; i < vol.Segments; i++ {
		vol.Header.FreeList[i] = binary.LittleEndian.Uint16(freelistBuf[i*2 : i*2+2])
	}
	hstr, err := json.Marshal(vol.Header)
	if err != nil {
		return nil, err
	}
	fmt.Printf("VolHeaderFooter: \n %s\n", hstr)

	// 加载DIR结构
	err = vol.loadDirs()
	if err != nil {
		return nil, err
	}

	// 分析DIR使用情况
	err = vol.DirCheck(false)
	if err != nil {
		return nil, fmt.Errorf("check dir failed: %s", err.Error())
	}
	volStr, _ := json.Marshal(vol)
	fmt.Println(string(volStr))
	fmt.Printf("cost %f secs\n", time.Since(begin).Seconds())
	return vol, nil
}

// 分析vol的头信息，注意，一共存在4套
func (vol *Vol) loadVolHeaders() error {

	//
	footerLen := RoundToStoreBlock(SIZEOF_VolHeaderFooter)
	fmt.Printf("footerlen: %d, dir len: %d\n", footerLen, vol.DirLen())
	footerOffset := vol.DirLen() - footerLen

	hfBufferLen := int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	//hfBuffer := make([]byte, hfBufferLen)

	// VolHeaderFooter存储顺序是： AHeader, AFooter, BHeader, BFooter
	ret := make([]*VolHeaderFooter, 4)
	offsets := []int64{
		vol.Skip,                                             // aHeadPos
		vol.Skip + int64(footerOffset),                       // aFootPos
		vol.Skip + int64(vol.DirLen()),                       // bHeadPos
		vol.Skip + int64(vol.DirLen()) + int64(footerOffset), // bFootPos
	}

	for idx, offset := range offsets {
		hfBuffer, err := vol.CacheDisk.Dio.Read(offset, hfBufferLen)
		if err != nil {
			return fmt.Errorf("seek to cache dis header failed: %s", err.Error())
		}
		vhf, err := newVolHeaderFooterFromBytes(hfBuffer)
		if err != nil {
			return fmt.Errorf("head[%d]: %d, info: %s", idx, offset, err.Error())
		}
		vhf.AnalyseDiskOffset = offset
		ret[idx] = vhf
	}

	var isFirst = true
	if ret[0].SyncSerial == ret[1].SyncSerial &&
		(ret[0].SyncSerial >= ret[2].SyncSerial || ret[2].SyncSerial != ret[3].SyncSerial) {

		vol.Header = ret[0]
		vol.Footer = ret[1]
	} else if ret[2].SyncSerial == ret[3].SyncSerial {
		vol.Header = ret[2]
		vol.Footer = ret[3]
		isFirst = false
	}

	if vol.Header.Magic != VOL_MAGIC || vol.Footer.Magic != VOL_MAGIC {
		return fmt.Errorf(
			"head or footer magic not match %s, used first head: %s head pos: %d, foot pos: %d",
			VOL_MAGIC, isFirst, vol.Header.AnalyseDiskOffset, vol.Footer.AnalyseDiskOffset)
	}
	vol.ContentStartPos = ret[0].AnalyseDiskOffset + int64(2*vol.DirLen())
	return nil
}

// 根据buffer创建VolHeaderFooter结构体
func newVolHeaderFooterFromBytes(buffer []byte) (*VolHeaderFooter, error) {

	vf := VolHeaderFooter{}
	curPos := 0
	vf.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	//if vf.Magic != cache.VOL_MAGIC {
	//	return nil, fmt.Errorf("vol magic not match")
	//}

	vf.Version.InkMajor = int16(binary.LittleEndian.Uint16(buffer[curPos : curPos+2]))
	curPos += 2
	vf.Version.InkMinor = int16(binary.LittleEndian.Uint16(buffer[curPos : curPos+2]))
	curPos += 2

	vf.CreateTime = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8

	vf.WritePos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos += 8

	vf.LastWritePos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos += 8
	vf.AggPos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos += 8
	vf.Generation = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Phase = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Cycle = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.SyncSerial = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.WriteSerial = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Dirty = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.SectorSize = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Unused = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	//vf.FreeList = binary.LittleEndian.Uint16(buffer[curPos : curPos+2])

	return &vf, nil
}

func newVolHeaderFooterFromBuffer(buffer []byte) (*VolHeaderFooter, error) {
	vf := VolHeaderFooter{}
	curPos := 0
	vf.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	//if vf.Magic != cache.VOL_MAGIC {
	//	return nil, fmt.Errorf("vol magic not match")
	//}

	vf.Version.InkMajor = int16(binary.LittleEndian.Uint16(buffer[curPos : curPos+2]))
	curPos += 2
	vf.Version.InkMinor = int16(binary.LittleEndian.Uint16(buffer[curPos : curPos+2]))
	curPos += 2

	vf.CreateTime = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8

	vf.WritePos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos += 8

	vf.LastWritePos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos += 8
	vf.AggPos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos += 8
	vf.Generation = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Phase = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Cycle = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.SyncSerial = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.WriteSerial = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Dirty = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.SectorSize = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	vf.Unused = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	//vf.FreeList = binary.LittleEndian.Uint16(buffer[curPos : curPos+2])

	return &vf, nil
}

// 填充dir数据
func (v *Vol) loadDirs() error {
	// 读取dir的磁盘信息
	v.DirPos = v.Header.AnalyseDiskOffset + int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	buffer, err := v.CacheDisk.Dio.Read(v.DirPos, int64(v.DirEntries()*SIZEOF_DIR))
	if err != nil {
		return fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}

	// 结构化
	if len(buffer) != v.DirEntries()*SIZEOF_DIR {
		return fmt.Errorf("buffer len not much")
	}
	for s := 0; s < v.Segments; s++ {
		sOffset := s * int(v.Buckets) * DIR_DEPTH
		for b := 0; b < int(v.Buckets); b++ {
			bOffset := sOffset + b*DIR_DEPTH
			for d := 0; d < DIR_DEPTH; d++ {
				offset := (bOffset + d) * SIZEOF_DIR
				dir, err := NewDirFromBuffer(buffer[offset : offset+SIZEOF_DIR])
				if err != nil {
					return fmt.Errorf("wrong dir pos [%d, %d, %d], err: %s", s, b, d, err.Error())
				}
				dir.Index.Segment = s
				dir.Index.Bucket = b
				dir.Index.Depth = d
				dir.Index.Offset = v.DirPos + int64(offset)
				dir.Index.Vol = v
				v.Dir[s][b][d] = dir
			}
		}
	}
	return nil
}

func (v *Vol) Dump() string {

	return ""
}
