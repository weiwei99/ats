package diskparser

import (
	"encoding/binary"
	"fmt"
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

type CacheVol struct {
	VolNumber int `json:"vol_number"`
	Scheme    int `json:"scheme"`
	Size      int `json:"size"`
	Vols      []*Vol
	DiskVols  []*DiskVol
}

type Vol struct {
	Path              string           `json:"path"`
	Dir               [][][]*Dir       `json:"-"`
	DirPos            int64            `json:"dir_pos"`
	Header            *VolHeaderFooter `json:"header"`
	Footer            *VolHeaderFooter `json:"footer"`
	Segments          int              `json:"segments"`
	Buckets           int64            `json:"buckets"`
	RecoverPos        int64            `json:"recover_pos"`
	PrevRecoverPos    int64            `json:"prev_recover_pos"`
	ScanPos           int64            `json:"scan_pos"`
	Skip              int64            `json:"skip"`  // start to headers
	Start             int64            `json:"start"` // start of data
	Len               int64            `json:"len"`
	DataBlocks        int64            `json:"data_blocks"`
	AggBufPos         int              `json:"agg_buf_pos"`
	HitEvacuateWindow int
	Disk              *CacheDisk
	Conf              *Config `json:"-"`
	ContentStartPos   int64   `json:"content_start_pos"`

	YYFullDir  []*Dir `json:"-"`
	YYStaleDir []*Dir `json:"-"`

	Content []*Doc `json:"-"`
}

func NewVol() (*Vol, error) {
	v := &Vol{}
	return v, nil
}

func (d *Vol) initData() {
	d.initDataInternal()
	d.initDataInternal()
	d.initDataInternal()
	d.allocDir()
}

func (d *Vol) initDataInternal() {
	//var cache_config_min_average_object_size int64 = 512000 //8000
	//var cache_config_min_average_object_size int64 = 8000
	d.Buckets = (d.Len - (d.Start - d.Skip)) / int64(d.Conf.MinAverageObjectSize) / DIR_DEPTH
	d.Segments = int((d.Buckets + (((1 << 16) - 1) / DIR_DEPTH)) / ((1 << 16) / DIR_DEPTH))
	d.Buckets = (d.Buckets + int64(d.Segments) - 1) / int64(d.Segments)
	d.Start = d.Skip + 2*int64(d.DirLen())
}

func (d *Vol) HeaderLen() int {
	return RoundToStoreBlock(SIZEOF_VolHeaderFooter + 2*(d.Segments-1))
}

func (d *Vol) DirLen() int {
	// TODO: d.Buckets 这个要改
	return d.HeaderLen() +
		RoundToStoreBlock(int(d.Buckets)*DIR_DEPTH*d.Segments*SIZEOF_DIR) +
		RoundToStoreBlock(SIZEOF_VolHeaderFooter)
}

func (d *Vol) DirEntries() int {
	return int(d.Buckets) * DIR_DEPTH * d.Segments
}

// 申请Dir空间
func (d *Vol) allocDir() {
	// 预申请空间
	d.Dir = make([][][]*Dir, d.Segments)
	for idxs := range d.Dir {
		d.Dir[idxs] = make([][]*Dir, d.Buckets)
		for idxb := range d.Dir[idxs] {
			d.Dir[idxs][idxb] = make([]*Dir, DIR_DEPTH)
			for idxd := range d.Dir[idxs][idxb] {
				d.Dir[idxs][idxb][idxd] = &Dir{
					IdxSegment: idxs,
					IdxBucket:  idxb,
					IdxDepth:   idxd,
					Next:       0,
				}
			}
		}
	}
}

//
func (v *Vol) LoadDirs(start int64, buffer []byte) error {
	if len(buffer) != v.DirEntries()*SIZEOF_DIR {
		return fmt.Errorf("buffer len not much")
	}
	for s := 0; s < v.Segments; s++ {
		sOffset := s * int(v.Buckets) * DIR_DEPTH
		for b := 0; b < int(v.Buckets); b++ {
			bOffset := sOffset + b*DIR_DEPTH
			for d := 0; d < DIR_DEPTH; d++ {
				offset := (bOffset + d) * SIZEOF_DIR
				v.Dir[s][b][d].LoadFromBuffer(buffer[offset : offset+SIZEOF_DIR])
				v.Dir[s][b][d].IdxOffset = start + int64(offset)
				//fmt.Println(v.Dir[s][b][d])
			}
			//if b == 9908 {
			//	for tt := 0; tt < 4; tt++ {
			//		vstr, _ := json.Marshal(v.Dir[s][b][tt])
			//		fmt.Println(string(vstr))
			//	}
			//	//break
			//}
		}
		//if s == 0 {
		//	//break
		//}
	}
	return nil
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
	fmt.Printf(" Directory for [%s %d:%d]\n", v.Disk.Path, v.Disk.Header.VolInfo.Offset, v.Disk.Header.VolInfo.Len)
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
	s, b, d := dir.IdxSegment, dir.IdxBucket, dir.IdxDepth
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
func (v *Vol) DirBucket(b int, s *Dir) *Dir {
	return v.Dir[s.IdxSegment][b][0]
}

func NewVolHeaderFooter(buffer []byte) (*VolHeaderFooter, error) {

	vf := VolHeaderFooter{}

	curPos := 0
	vf.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	//if vf.Magic != VOL_MAGIC {
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
