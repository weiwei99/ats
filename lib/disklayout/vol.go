/*
vol结构
*/
package disklayout

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/weiwei99/ats/lib/cache"
	"time"
)

// 需要借助cache disk中的信息来parse vol
func (lo *Layout) parseVol(block *cache.DiskVolBlock) (*cache.Vol, error) {

	cd := lo.CacheDisk
	begin := time.Now()

	volConfig := &cache.VolConfig{
		MinAverageObjectSize: cd.AtsConf.MinAverageObjectSize,
		VolInfo:              block,
	}
	vol, err := cache.NewVol(volConfig)
	if err != nil {
		return nil, fmt.Errorf("create vol failed: %s", err.Error())
	}

	// 分析headers（包含header, footer)
	err = lo.parseVolHeaders(vol)
	if err != nil {
		return nil, fmt.Errorf("vol header read failed: %s", err.Error())
	}

	// 分析freelist
	vol.Header.FreeList = make([]uint16, vol.Segments)
	// Freelist在 80-72的位置
	freelistBufPos := vol.Header.AnalyseDiskOffset + (cache.SIZEOF_VolHeaderFooter - 8)
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
	err = lo.parseDir(vol)
	if err != nil {
		return nil, err
	}

	// 分析DIR使用情况
	vol.DirCheck(false)
	volStr, _ := json.Marshal(vol)
	fmt.Println(string(volStr))
	fmt.Printf("cost %f secs\n", time.Since(begin).Seconds())
	return vol, nil
}

// 分析vol的头信息，注意，一共存在4套
func (lo *Layout) parseVolHeaders(vol *cache.Vol) error {
	cd := lo.CacheDisk
	//
	footerLen := RoundToStoreBlock(cache.SIZEOF_VolHeaderFooter)
	fmt.Printf("footerlen: %d, dir len: %d\n", footerLen, vol.DirLen())
	footerOffset := vol.DirLen() - footerLen

	hfBufferLen := int64(RoundToStoreBlock(cache.SIZEOF_VolHeaderFooter))
	//hfBuffer := make([]byte, hfBufferLen)

	// VolHeaderFooter存储顺序是： AHeader, AFooter, BHeader, BFooter
	ret := make([]*cache.VolHeaderFooter, 4)
	offsets := []int64{
		vol.Skip,                                             // aHeadPos
		vol.Skip + int64(footerOffset),                       // aFootPos
		vol.Skip + int64(vol.DirLen()),                       // bHeadPos
		vol.Skip + int64(vol.DirLen()) + int64(footerOffset), // bFootPos
	}

	for idx, offset := range offsets {
		hfBuffer, err := cd.Dio.Read(offset, hfBufferLen)
		if err != nil {
			return fmt.Errorf("seek to cache dis header failed: %s", err.Error())
		}
		vhf, err := lo.loadVolHeaderFooterFromBytes(hfBuffer)
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

	if vol.Header.Magic != cache.VOL_MAGIC || vol.Footer.Magic != cache.VOL_MAGIC {
		return fmt.Errorf("head or footer magic not match %s, used first head: %s head pos: %d, foot pos: %d",
			cache.VOL_MAGIC, isFirst, vol.Header.AnalyseDiskOffset, vol.Footer.AnalyseDiskOffset)
	}
	vol.ContentStartPos = ret[0].AnalyseDiskOffset + int64(2*vol.DirLen())
	return nil
}

func (lo *Layout) parseDir(vol *cache.Vol) error {
	cd := lo.CacheDisk
	// Scan Dir
	vol.DirPos = vol.Header.AnalyseDiskOffset + int64(RoundToStoreBlock(cache.SIZEOF_VolHeaderFooter))
	abuf, err := cd.Dio.Read(vol.DirPos, int64(vol.DirEntries()*cache.SIZEOF_DIR))
	if err != nil {
		return fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	err = lo.loadDirs(vol, abuf)
	if err != nil {
		return fmt.Errorf("load dir failed: %s", err.Error())
	}
	return nil
}

func (lo *Layout) loadVolHeaderFooterFromBytes(buffer []byte) (*cache.VolHeaderFooter, error) {

	vf := cache.VolHeaderFooter{}
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
func (lo *Layout) loadDirs(v *cache.Vol, buffer []byte) error {
	if len(buffer) != v.DirEntries()*cache.SIZEOF_DIR {
		return fmt.Errorf("buffer len not much")
	}
	for s := 0; s < v.Segments; s++ {
		sOffset := s * int(v.Buckets) * cache.DIR_DEPTH
		for b := 0; b < int(v.Buckets); b++ {
			bOffset := sOffset + b*cache.DIR_DEPTH
			for d := 0; d < cache.DIR_DEPTH; d++ {
				offset := (bOffset + d) * cache.SIZEOF_DIR
				dir, err := loadDirFromBytes(v, buffer[offset:offset+cache.SIZEOF_DIR])
				if err != nil {
					return fmt.Errorf("wrong dir pos [%d, %d, %d], err: %s", s, b, d, err.Error())
				}
				dir.Index.Segment = s
				dir.Index.Bucket = b
				dir.Index.Depth = d
				dir.Index.Offset = v.DirPos + int64(offset)
				v.Dir[s][b][d] = dir
			}
		}
	}
	return nil
}

//
func loadDirFromBytes(v *cache.Vol, buffer []byte) (*cache.Dir, error) {
	d := &cache.Dir{
		Index: &cache.DirPos{
			Vol: v,
		},
	}
	curPos := 0
	data := binary.LittleEndian.Uint32(buffer[:4])

	//
	dataHigh := binary.LittleEndian.Uint16(buffer[8:10])
	d.Offset = uint64(data)&0x00ffffff | (uint64(dataHigh) << 24)

	d.Big = uint8((data >> 24) & 0x03)
	d.Size = uint8((data >> 26) & 0x3f)
	curPos += 4

	data = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	d.Tag = uint16(data & 0x00000fff)
	d.Phase = uint8((data >> 12) & 0x01)
	d.Head = uint8((data >> 13) & 0x01)
	d.Pinned = uint8((data >> 14) & 0x01)
	d.Token = uint8((data >> 15) & 0x01)
	d.Next = uint16((data >> 16) & 0xffff)

	d.RawByte = make([]byte, len(buffer))
	copy(d.RawByte, buffer)
	d.RawByteHex = hex.Dump(d.RawByte)
	return d, nil
}
