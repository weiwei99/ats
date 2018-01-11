package diskparser

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"time"
)

const (
	START = 8192
)

type CacheParser struct {
	Paths []string
	//Start        uint64
	//Dio    []*DiskReader
	CacheDisks []*CacheDisk
	Conf       *ATSConfig
}

func NewCacheParser(atsconf *ATSConfig) (*CacheParser, error) {
	cp := &CacheParser{
		CacheDisks: make([]*CacheDisk, 0),
	}

	for _, v := range atsconf.Storages {
		cdisk, err := NewCacheDisk(v, atsconf)
		if err != nil {
			return nil, err
		}
		cp.CacheDisks = append(cp.CacheDisks, cdisk)
	}

	return cp, nil
}

func (cparser *CacheParser) ParseMain(path string) error {
	return nil
}

// cache disk header parse
func (cd *CacheDisk) ParseCacheDiskHeader() error {
	buffer, err := cd.Dio.read(START, int64(DiskHeaderLen))
	if err != nil {
		fmt.Println(err)
		return err
	}
	err = cd.Load(buffer)
	if err != nil {
		fmt.Println(err)
		return err
	}
	cd.PsDiskOffsetStart = int64(START)
	cd.PsDiskOffsetEnd = int64(START + DiskHeaderLen)
	cdiskInfo, err := json.Marshal(cd)
	fmt.Println(string(cdiskInfo))

	return nil
}

func (cparser *CacheParser) MainParse() error {
	for _, v := range cparser.CacheDisks {
		err := v.ParseRawDisk()
		if err != nil {
			return err
		}
	}
	return nil
}

func (cd *CacheDisk) ParseRawDisk() error {
	// 打开磁盘
	//err := cparser.Dio.open(conf.Path)
	//if err != nil {
	//	fmt.Errorf("open path failed: %s", err.Error())
	//	return err
	//}
	//
	//cparser.Start = 0
	//// 跳过磁盘头
	//cparser.Start += START
	// 分析CacheDisk的Header

	err := cd.ParseCacheDiskHeader()
	if err != nil {
		return err
	}

	// 分析Vol
	vol, err := cd.ParseVol()
	if err != nil {
		fmt.Println(err)
		return err
	}
	cd.YYVol = vol

	return nil
}

// 检查DIR是否健康
func (cd *CacheDisk) CheckDir() {
	vol := cd.YYVol
	success := vol.CheckDir()
	fmt.Printf("check dir: %v\n", success)
}

// 分析content obj
func (cd *CacheDisk) ScanHttpObject() {
	err := cd.ExtractDocs(0)
	if err != nil {
		fmt.Println(err)
	}
}

// 需要借助cache disk中的信息来parse vol
func (cd *CacheDisk) ParseVol() (*Vol, error) {
	begin := time.Now()

	vol, err := NewVol(cd, cd.AtsConf.MinAverageObjectSize)
	if err != nil {
		return nil, fmt.Errorf("create vol failed: %s", err.Error())
	}

	// 分析header
	_, err = cd.loadVolHeader(vol)
	if err != nil {
		return nil, fmt.Errorf("vol header read failed: %s", err.Error())
	}

	// 分析freelist
	vol.Header.FreeList = make([]uint16, vol.Segments)
	// Freelist在 80-72的位置
	freelistBufPos := vol.Header.AnalyseDiskOffset + (SIZEOF_VolHeaderFooter - 8)
	freelistBuf, err := cd.Dio.read(freelistBufPos, int64(vol.Segments)*2)
	for i := 0; i < vol.Segments; i++ {
		vol.Header.FreeList[i] = binary.LittleEndian.Uint16(freelistBuf[i*2 : i*2+2])
	}

	hstr, err := json.Marshal(vol.Header)
	if err != nil {
		return nil, err
	}
	fmt.Printf("VolHeaderFooter: \n %s\n", hstr)

	// 加载DIR结构
	err = cd.parseDir(vol)
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

func (cd *CacheDisk) parseDir(vol *Vol) error {
	// Scan Dir
	vol.DirPos = vol.Header.AnalyseDiskOffset + int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	abuf, err := cd.Dio.read(vol.DirPos, int64(vol.DirEntries()*SIZEOF_DIR))
	if err != nil {
		return fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	err = vol.LoadDirs(vol.DirPos, abuf)
	if err != nil {
		return fmt.Errorf("load dir failed: %s", err.Error())
	}
	return nil
}

func (cd *CacheDisk) loadVolHeader(vol *Vol) ([]*VolHeaderFooter, error) {

	ret := make([]*VolHeaderFooter, 4)
	//
	footerLen := RoundToStoreBlock(SIZEOF_VolHeaderFooter)
	fmt.Printf("footerlen: %d, dir len: %d\n", footerLen, vol.DirLen())
	footerOffset := vol.DirLen() - footerLen

	hfBufferLen := int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	hfBuffer := make([]byte, hfBufferLen)

	// A HEADER
	aHeadPos := vol.Skip
	hfBuffer, err := cd.Dio.read(aHeadPos, hfBufferLen)
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
	hfBuffer, err = cd.Dio.read(aFootPos, hfBufferLen)
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
	hfBuffer, err = cd.Dio.read(bHeadPos, hfBufferLen)
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
	hfBuffer, err = cd.Dio.read(bFootPos, hfBufferLen)
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

func (cd *CacheDisk) LoadReadyDocCount() (int, int) {
	if cd.YYVol == nil {
		return 0, 0
	}
	v := cd.YYVol
	cd.DocLoadMutex.RLock()
	defer cd.DocLoadMutex.RUnlock()
	return len(v.Content), len(v.YYFullDir)
}

//
func (cd *CacheDisk) ExtractDocs(max int) error {

	v := cd.YYVol
	v.Content = make([]*Doc, 0)

	if max < 1 || max >= len(v.YYFullDir) {
		max = len(v.YYFullDir)
	}
	fmt.Printf("total FullDir : %d, need parse: %d\n", len(v.YYFullDir), max)
	for _, dir := range v.YYFullDir {
		docPos := int64(dir.Offset-1)*DEFAULT_HW_SECTOR_SIZE + v.ContentStartPos
		buff, err := cd.Dio.read(docPos, 72)
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

		cd.DocLoadMutex.Lock()
		v.Content = append(v.Content, newDoc)
		cd.DocLoadMutex.Unlock()
		max = max - 1
		if max < 1 {
			break
		}

	}
	fmt.Printf("total content: %d\n", len(v.Content))
	return nil
}

// 从doc中提出http信息
func (cd *CacheDisk) ExtractHttpInfoHeader(doc *Doc) (*HTTPCacheAlt, error) {
	if doc.Magic != DOC_MAGIC {
		return nil, fmt.Errorf("doc magic not match")
	}
	if doc.HLen == 0 {
		return nil, fmt.Errorf("doc is empty")
	}

	startPos := doc.YYDiskOffset + 72
	//fmt.Printf("dir h len: %d\n", d.HLen)
	buf, err := cd.Dio.read(startPos, int64(doc.HLen))
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
