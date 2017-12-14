package diskparser

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"syscall"
)

const (
	START = 8192
)

type CacheParser struct {
	Path  string
	File  *os.File
	Fd    *int
	Start uint64
}

func (cparser *CacheParser) ParseMain(path string) error {
	return nil
}

func (cparse *CacheParser) open(path string) error {
	_, err := os.Stat("path")
	if os.IsNotExist(err) {
		fd, err := syscall.Open(path, syscall.O_RDONLY, 0777)
		if err != nil {
			fmt.Errorf("open raw disk [%s] failed: %s", path, err.Error())
			return err
		}
		cparse.Fd = &fd
		cparse.File = nil
	} else {
		file, err := os.Open(path)
		if err != nil {
			fmt.Errorf("open file :%s failed: %s", path, err.Error())
			return err
		}
		cparse.File = file
		cparse.Fd = nil
	}
	return nil
}

func (cparser *CacheParser) read(offset, size int64) ([]byte, error) {

	ret := make([]byte, size)
	if cparser.Fd != nil {
		pos, err := syscall.Seek(*cparser.Fd, offset, 0)
		if err != nil {
			return ret, fmt.Errorf("seek to cache dis header failed: %s", err.Error())
		}
		if pos != offset {
			return ret, fmt.Errorf("pos not much: %d", offset)
		}
		numRead, err := syscall.Read(*cparser.Fd, ret)
		if err != nil {
			err = fmt.Errorf("read buffer failed: %s", err.Error())
			return ret, err
		}
		if int64(numRead) != size {
			err = fmt.Errorf("read buffer length not match: %d, %d", numRead, size)
			return ret, err
		}
	} else {

	}
	return ret, nil
}

func (cparser *CacheParser) ParseCacheDisk(conf Config) error {
	cparser.Path = conf.Path

	err := cparser.open(conf.Path)
	if err != nil {
		fmt.Errorf("open path failed: %s", err.Error())
		return err
	}

	cparser.Start = 0
	// 分析CacheDisk的Header
	cparser.Start += START
	var cacheDiskHeaderLen = 56

	buffer, err := cparser.read(START, int64(cacheDiskHeaderLen))
	if err != nil {
		fmt.Println(err)
		return err
	}

	cdisk, err := NewCacheDiskFromBuffer(buffer)
	if err != nil {
		fmt.Println(err)
		return err
	}
	cdisk.Path = conf.Path
	cdiskInfo, err := json.Marshal(cdisk)
	fmt.Println(string(cdiskInfo))

	cparser.Start = cparser.Start + uint64(cacheDiskHeaderLen)

	// 分析Vol的Header
	vol, err := cparser.ParseVol(nil, cdisk, &conf)
	if err != nil {
		fmt.Println(err)
		return err
	}
	//
	fmt.Println("+++++++++++++++")
	// 加载Dir
	cparser.LoadDir(vol)
	// 检查Dir是否健康
	success := vol.CheckDir()
	fmt.Printf("check dir: %v\n", success)
	// 统计Dir情况
	vol.DirCheck(false)
	volStr, _ := json.Marshal(vol)
	fmt.Println(string(volStr))

	// 分析content obj
	err = cparser.ParseFullDir(vol)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}

func (cparser *CacheParser) ParseVol(buffer []byte, cacheDisk *CacheDisk, conf *Config) (*Vol, error) {
	if cacheDisk == nil {
		return nil, fmt.Errorf("parse vol failed, cache disk is nil")
	}

	vol, err := NewVol()
	if err != nil {
		return nil, fmt.Errorf("create vol failed: %s", err.Error())
	}
	vol.Conf = conf
	vol.Len = int64(cacheDisk.Header.VolInfo.Len * STORE_BLOCK_SIZE)
	vol.Disk = cacheDisk
	vol.Skip = int64(cacheDisk.Header.VolInfo.Offset)
	vol.PrevRecoverPos = 0
	vol.Start = int64(cacheDisk.Header.VolInfo.Offset)

	// 分析大小
	vol.initData()

	vol.DataBlocks = vol.Len - (vol.Start-vol.Skip)/STORE_BLOCK_SIZE

	cache_config_hit_evacuate_percent := 10
	vol.HitEvacuateWindow = int(vol.DataBlocks) * cache_config_hit_evacuate_percent / 100

	// 分析header
	_, err = cparser.VolHeaderRead(vol)
	if err != nil {
		return nil, fmt.Errorf("vol header read failed: %s", err.Error())
	}

	// 分析freelist
	vol.Header.FreeList = make([]uint16, vol.Segments)
	// Freelist在 80-72的位置
	freelistBufPos := vol.Header.AnalyseDiskOffset + (SIZEOF_VolHeaderFooter - 8)
	freelistBuf, err := cparser.read(freelistBufPos, int64(vol.Segments)*2)
	for i := 0; i < vol.Segments; i++ {
		vol.Header.FreeList[i] = binary.LittleEndian.Uint16(freelistBuf[i*2 : i*2+2])
	}

	hstr, err := json.Marshal(vol.Header)
	if err != nil {
		return nil, err
	}
	fmt.Printf("VolHeaderFooter: \n %s\n", hstr)
	return vol, nil
}

func (cparser *CacheParser) LoadDir(vol *Vol) error {
	// Scan Dir
	vol.DirPos = vol.Header.AnalyseDiskOffset + int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))

	abuf, err := cparser.read(vol.DirPos, int64(vol.DirEntries()*SIZEOF_DIR))
	if err != nil {
		return fmt.Errorf("seek to cache disk header failed: %s", err.Error())
	}
	err = vol.LoadDirs(vol.DirPos, abuf)
	if err != nil {
		return fmt.Errorf("load dir failed: %s", err.Error())
	}
	return nil
}

func (cparser *CacheParser) VolHeaderRead(vol *Vol) ([]*VolHeaderFooter, error) {

	ret := make([]*VolHeaderFooter, 4)
	//
	footerLen := RoundToStoreBlock(SIZEOF_VolHeaderFooter)
	fmt.Printf("footerlen: %d, dir len: %d\n", footerLen, vol.DirLen())
	footerOffset := vol.DirLen() - footerLen

	hfBufferLen := int64(RoundToStoreBlock(SIZEOF_VolHeaderFooter))
	hfBuffer := make([]byte, hfBufferLen)

	// A HEADER
	aHeadPos := vol.Skip
	hfBuffer, err := cparser.read(aHeadPos, hfBufferLen)
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
	hfBuffer, err = cparser.read(aFootPos, hfBufferLen)
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
	hfBuffer, err = cparser.read(bHeadPos, hfBufferLen)
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
	hfBuffer, err = cparser.read(bFootPos, hfBufferLen)
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
	var isFirst bool = true
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

func (cparser *CacheParser) ParseFullDir(v *Vol) error {
	v.Content = make([]*Doc, 0)
	for _, d := range v.YYFullDir {

		docPos := int64(d.Offset-1)*DEFAULT_HW_SECTOR_SIZE + v.ContentStartPos
		con := &Doc{}
		con.YYDiskOffset = docPos
		buff, err := cparser.read(con.YYDiskOffset, 72)
		if err != nil {
			return err
		}
		err = con.LoadFromDisk(buff)
		if err != nil {
			fmt.Println("load content failed")
			return fmt.Errorf("load content failed: %s", err.Error())
		}
		//conStr, _ := json.Marshal(con)
		//fmt.Println(string(conStr))

		if con.Magic != DOC_MAGIC {
			return fmt.Errorf("doc magic not match")
		}
		if con.HLen == 0 {
			continue
		}
		//v.Content = append(v.Content, con)

		hh, err := cparser.ParseHttpInfoHeader(con)
		if err != nil {
			fmt.Printf("parse http info %s\n", err.Error())
			continue
		}
		if hh.Magic != CACHE_ALT_MAGIC_MARSHALED {
			continue
		}

	}

	fmt.Printf("total content: %d\n", len(v.Content))
	//
	//first := make([]*HTTPCacheAlt, 0)
	//for _, c := range v.Content {
	//	hh, err := cparser.ParseHttpInfoHeader(c)
	//	if err != nil {
	//		fmt.Printf("parse http info %s\n", err.Error())
	//		continue
	//	}
	//	if hh.Magic != CACHE_ALT_MAGIC_MARSHALED {
	//		continue
	//	}
	//	first = append(first, hh)
	//
	//}

	//for _, hh := range first {
	//	histr, _ := json.Marshal(hh)
	//	fmt.Println(string(histr))
	//}
	return nil
}

func (cparser *CacheParser) ParseHttpInfoHeader(d *Doc) (*HTTPCacheAlt, error) {
	startPos := d.YYDiskOffset + 72

	//fmt.Printf("dir h len: %d\n", d.HLen)
	buf, err := cparser.read(startPos, int64(d.HLen))
	if err != nil {
		return nil, err
	}

	hi := &HTTPCacheAlt{}
	hi.YYDiskOffset = startPos
	hi.LoadFromBuffer(buf)

	return hi, nil
}
