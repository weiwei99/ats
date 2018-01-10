package diskparser

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const (
	START = 8192
)

type CacheParser struct {
	Paths []string
	Start uint64
	Dio   *DiskReader

	cdisk *CacheDisk
	Conf  *Config

	DocLoadMutex *sync.RWMutex
}

func NewCacheParser() (*CacheParser, error) {
	cp := &CacheParser{
		DocLoadMutex: new(sync.RWMutex),
	}
	return cp, nil
}

func (cparser *CacheParser) ParseMain(path string) error {
	return nil
}

func (cparser *CacheParser) ParseCacheDiskHeader(buffer []byte) (*CacheDisk, error) {
	cdisk, err := NewCacheDisk()
	if err != nil {
		return nil, err
	}
	err = cdisk.Load(buffer)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	return cdisk, nil
}

func (cparser *CacheParser) ParseRawDisk(conf Config) error {
	// 打开磁盘
	err := cparser.Dio.open(conf.Path)
	if err != nil {
		fmt.Errorf("open path failed: %s", err.Error())
		return err
	}

	cparser.Start = 0
	// 跳过磁盘头
	cparser.Start += START
	// 分析CacheDisk的Header
	buffer, err := cparser.Dio.read(START, int64(DiskHeaderLen))
	if err != nil {
		fmt.Println(err)
		return err
	}
	cdisk, err := cparser.ParseCacheDiskHeader(buffer)
	if err != nil {
		return err
	}
	cdisk.PsDiskOffsetStart = int64(cparser.Start)
	cdisk.PsDiskOffsetEnd = int64(cparser.Start + DiskHeaderLen)
	cdisk.Path = conf.Path
	cparser.cdisk = cdisk
	cdiskInfo, err := json.Marshal(cdisk)
	fmt.Println(string(cdiskInfo))

	// 分析Vol
	cparser.Start = cparser.Start + uint64(DiskHeaderLen)
	vol, err := cparser.ParseVol(cdisk, &conf)
	if err != nil {
		fmt.Println(err)
		return err
	}
	cdisk.YYVol = vol

	return nil
}

// 检查DIR是否健康
func (cparser *CacheParser) CheckDir() {
	vol := cparser.cdisk.YYVol
	success := vol.CheckDir()
	fmt.Printf("check dir: %v\n", success)
}

// 分析content obj
func (cparser *CacheParser) ScanHttpObject() {
	err := cparser.ExtractDocs(0)
	if err != nil {
		fmt.Println(err)
	}
}

// 需要借助cache disk中的信息来parse vol
func (cparser *CacheParser) ParseVol(cacheDisk *CacheDisk, conf *Config) (*Vol, error) {
	begin := time.Now()
	if cacheDisk == nil {
		return nil, fmt.Errorf("parse vol failed, cache disk is nil")
	}

	vol, err := NewVol(cacheDisk, conf.MinAverageObjectSize)
	if err != nil {
		return nil, fmt.Errorf("create vol failed: %s", err.Error())
	}

	// 分析header
	_, err = cparser.loadVolHeader(vol)
	if err != nil {
		return nil, fmt.Errorf("vol header read failed: %s", err.Error())
	}

	// 分析freelist
	vol.Header.FreeList = make([]uint16, vol.Segments)
	// Freelist在 80-72的位置
	freelistBufPos := vol.Header.AnalyseDiskOffset + (SIZEOF_VolHeaderFooter - 8)
	freelistBuf, err := cparser.Dio.read(freelistBufPos, int64(vol.Segments)*2)
	for i := 0; i < vol.Segments; i++ {
		vol.Header.FreeList[i] = binary.LittleEndian.Uint16(freelistBuf[i*2 : i*2+2])
	}

	hstr, err := json.Marshal(vol.Header)
	if err != nil {
		return nil, err
	}
	fmt.Printf("VolHeaderFooter: \n %s\n", hstr)

	// 加载DIR结构
	err = cparser.parseDir(vol)
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

func (cparser *CacheParser) parseDir(vol *Vol) error {
	// Scan Dir
	vol.DirPos = vol.Header.AnalyseDiskOffset + int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	abuf, err := cparser.Dio.read(vol.DirPos, int64(vol.DirEntries()*SIZEOF_DIR))
	if err != nil {
		return fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	err = vol.LoadDirs(vol.DirPos, abuf)
	if err != nil {
		return fmt.Errorf("load dir failed: %s", err.Error())
	}
	return nil
}

func (cparser *CacheParser) loadVolHeader(vol *Vol) ([]*VolHeaderFooter, error) {

	ret := make([]*VolHeaderFooter, 4)
	//
	footerLen := RoundToStoreBlock(SIZEOF_VolHeaderFooter)
	fmt.Printf("footerlen: %d, dir len: %d\n", footerLen, vol.DirLen())
	footerOffset := vol.DirLen() - footerLen

	hfBufferLen := int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	hfBuffer := make([]byte, hfBufferLen)

	// A HEADER
	aHeadPos := vol.Skip
	hfBuffer, err := cparser.Dio.read(aHeadPos, hfBufferLen)
	if err != nil {
		return nil, fmt.Errorf("seek to cache dis header failed: %s", err.Error())
	}
	aHead, err := NewVolHeaderFooter(hfBuffer)
	if err != nil {
		return nil, fmt.Errorf("ahead: %d, info: %s", aHeadPos, err.Error())
	}
	aHead.AnalyseDiskOffset = aHeadPos
	ret[0] = aHead

	// A FOOTER
	aFootPos := aHeadPos + int64(footerOffset)
	hfBuffer, err = cparser.Dio.read(aFootPos, hfBufferLen)
	if err != nil {
		return nil, fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	aFoot, err := NewVolHeaderFooter(hfBuffer)
	if err != nil {
		return nil, fmt.Errorf("afoot: %d, info: %s", aFootPos, err.Error())
	}
	aFoot.AnalyseDiskOffset = aFootPos
	ret[1] = aFoot

	// B HEADER
	bHeadPos := vol.Skip + int64(vol.DirLen())
	hfBuffer, err = cparser.Dio.read(bHeadPos, hfBufferLen)
	if err != nil {
		return nil, fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	bHead, err := NewVolHeaderFooter(hfBuffer)
	if err != nil {
		return nil, fmt.Errorf("bhead: %d, info: %s", bHeadPos, err.Error())
	}
	bHead.AnalyseDiskOffset = bHeadPos
	ret[2] = bHead

	// B FOOTER
	bFootPos := bHeadPos + int64(footerOffset)
	hfBuffer, err = cparser.Dio.read(bFootPos, hfBufferLen)
	if err != nil {
		return nil, fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	bFoot, err := NewVolHeaderFooter(hfBuffer)
	if err != nil {
		return nil, fmt.Errorf("bfoot: %d, info: %s", bFootPos, err.Error())
	}
	bFoot.AnalyseDiskOffset = bFootPos
	ret[3] = bFoot

	for _, hh := range ret {
		hhstr, _ := json.Marshal(hh)
		fmt.Println(string(hhstr))
	}
	var isFirst = true
	if aHead.SyncSerial == aFoot.SyncSerial &&
		(aHead.SyncSerial >= bHead.SyncSerial || bHead.SyncSerial != bFoot.SyncSerial) {

		vol.Header = aHead
		vol.Footer = aFoot

	} else if bHead.SyncSerial == bFoot.SyncSerial {
		vol.Header = bHead
		vol.Footer = bFoot
		isFirst = false
	}

	if vol.Header.Magic != VOL_MAGIC || vol.Footer.Magic != VOL_MAGIC {
		return nil, fmt.Errorf("head or footer magic not match %s, used first head: %s"+
			" head pos: %d, foot pos: %d",
			VOL_MAGIC, isFirst, vol.Header.AnalyseDiskOffset, vol.Footer.AnalyseDiskOffset)
	}
	vol.ContentStartPos = aHead.AnalyseDiskOffset + int64(2*vol.DirLen())
	return ret, nil
}

func (cparser *CacheParser) LoadReadyDocCount() (int, int) {
	if cparser.cdisk == nil || cparser.cdisk.YYVol == nil {
		return 0, 0
	}
	v := cparser.cdisk.YYVol
	cparser.DocLoadMutex.RLock()
	defer cparser.DocLoadMutex.RUnlock()
	return len(v.Content), len(v.YYFullDir)
}

//
func (cparser *CacheParser) ExtractDocs(max int) error {
	if cparser.cdisk == nil || cparser.cdisk.YYVol == nil {
		return fmt.Errorf("%s", "need parse vol first")
	}
	v := cparser.cdisk.YYVol
	v.Content = make([]*Doc, 0)

	if max < 1 || max >= len(v.YYFullDir) {
		max = len(v.YYFullDir)
	}
	fmt.Printf("total FullDir : %d, need parse: %d\n", len(v.YYFullDir), max)
	for _, dir := range v.YYFullDir {
		docPos := int64(dir.Offset-1)*DEFAULT_HW_SECTOR_SIZE + v.ContentStartPos
		buff, err := cparser.Dio.read(docPos, 72)
		if err != nil {
			return err
		}
		newDoc, err := NewDoc(buff)
		if err != nil {
			return fmt.Errorf("parse doc failed: %s", err.Error())
		}
		newDoc.YYDiskOffset = docPos

		if newDoc.Magic != DOC_MAGIC {
			return fmt.Errorf("doc magic not match")
		}
		if newDoc.HLen == 0 {
			continue
		}

		cparser.DocLoadMutex.Lock()
		v.Content = append(v.Content, newDoc)
		cparser.DocLoadMutex.Unlock()
		max = max - 1
		if max < 1 {
			break
		}

	}
	fmt.Printf("total content: %d\n", len(v.Content))
	return nil
}

// 从doc中提出http信息
func (cparser *CacheParser) ExtractHttpInfoHeader(doc *Doc) (*HTTPCacheAlt, error) {
	if doc.Magic != DOC_MAGIC {
		return nil, fmt.Errorf("doc magic not match")
	}
	if doc.HLen == 0 {
		return nil, fmt.Errorf("doc is empty")
	}

	startPos := doc.YYDiskOffset + 72
	//fmt.Printf("dir h len: %d\n", d.HLen)
	buf, err := cparser.Dio.read(startPos, int64(doc.HLen))
	if err != nil {
		return nil, err
	}

	hi := &HTTPCacheAlt{}
	hi.YYDiskOffset = startPos
	hi.LoadFromBuffer(buf)

	if hi.Magic != CACHE_ALT_MAGIC_MARSHALED {
		return nil, fmt.Errorf("not http info block")
	}

	return hi, nil
}

func (cparser *CacheParser) DumpParser() string {
	return ""
}
