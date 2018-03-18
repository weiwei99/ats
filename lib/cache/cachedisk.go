/*
For each valid span, an instance of CacheDisk is created. This class is a continuation and so
can be used to perform potentially blocking operations on the span. The primary use of these
is to be passed to the AIO threads as the callback when an I/O operation completes. These are
then dispatched to AIO threads to perform storage unit initialization.
*/
package cache

import (
	"encoding/binary"
	//"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/weiwei99/ats/lib/conf"
	"github.com/weiwei99/ats/lib/disk"
	"github.com/weiwei99/ats/lib/proxy"
	"net/url"
	"sync"
)

/*
#include <string.h>
#include <stdint.h>
struct DiskVolBlock {
  	uint64_t offset; // offset in bytes from the start of the disk
  	uint64_t len;    // length in in store blocks
  	int number;
  	unsigned int type : 3;
  	unsigned int free : 1;
};
struct DiskHeader {
	unsigned int magic;
  	unsigned int num_volumes;      // number of discrete volumes (DiskVol)
	unsigned int num_free;         // number of disk volume blocks free
	unsigned int num_used;         // number of disk volume blocks in use
	unsigned int num_diskvol_blks; // number of disk volume blocks
	uint64_t num_blocks;
	struct DiskVolBlock vol_info[1];
};
*/
import "C"

const (
	START = 8192
)

// 对应文档中的stripe
type DiskVol struct {
	NumVolBlocks int
	VolNumber    int
	Size         uint64
	Disk         *CacheDisk
}

type DiskVolBlock struct {
	Offset uint64 `json:"offset"` // offset in bytes from the start of the disk
	Len    uint64 `json:"len"`    // length in in store blocks
	Number int    `json:"number"`
	Type   uint8  `json:"type"` //0-3
	Free   uint8  `json:"free"` //1
}

const (
	ZYDiskHeaderOffset = 0x2000 // 8192
	ZYVolOffset        = 0xfe00 // 65024
	//ZYDocOffset = 0x

	DISK_HEADER_MAGIC uint32 = 0xABCD1237 // 出现在 0002000， 0009

	STORE_BLOCK_SIZE       = 8192
	STORE_BLOCK_SHIFT      = 13
	DEFAULT_HW_SECTOR_SIZE = 512

	CACHE_BLOCK_SHIFT = 9
	CACHE_BLOCK_SIZE  = 1 << CACHE_BLOCK_SHIFT // 512, smallest sector size
	START_BLOCKS      = 16
	START_POS         = START_BLOCKS * CACHE_BLOCK_SIZE

	VOL_BLOCK_SIZE = 1024 * 1024 * 128
	MIN_VOL_SIZE   = VOL_BLOCK_SIZE

	PAGE_SIZE = 8192

	DiskHeaderLen = 56 // 56个字节

	DiskVolBlockLen = 24
	//LEN_DiskHeader   = 6 + LEN_DiskVolBlock
)

// 磁盘头，一个磁盘可以有多个Volume
type DiskHeader struct {
	Magic          uint32          `json:"magic"`
	NumVolumes     uint32          `json:"num_volumes"`
	NumFree        uint32          `json:"num_free"`
	NumUsed        uint32          `json:"num_used"`
	NumDiskvolBlks uint32          `json:"num_diskvol_blks"`
	NumBlocks      uint64          `json:"num_blocks"`
	VolInfos       []*DiskVolBlock `json:"-"` // 存储在磁盘上的stripe信息
}

// 磁盘信息
type CacheDisk struct {
	Header              *DiskHeader // 磁盘头信息
	HeaderLen           int64       `json:"header_len"` // 磁盘头长度
	Geometry            *disk.Geometry
	Path                string `json:"path"`
	Len                 int64  `json:"len"`
	Start               int64  `json:"start"`
	Skip                int64  `json:"skip"`
	NumUsableBlocks     int64  `json:"num_usable_blocks"`
	HWSectorSize        int    `json:"hw_sector_size"`
	Fd                  int
	DiskVols            []DiskVol // 磁盘的vol，对应ats文档中的stripe
	FreeBlocks          []DiskVol // 尚未使用的磁盘空间
	Cleared             int       `json:"cleared"`
	NumErrors           int       `json:"num_errors"`
	DebugLoad           bool      // debug加载模式，解析保存数据（消耗内存）
	YYVol               *Vol      `json:"-"`
	PsRawDiskHeaderData []byte    `json:"-"`
	PsDiskOffsetStart   int64     // 磁盘上相对起始位置
	PsDiskOffsetEnd     int64     // 磁盘上相对结束位置
	Dio                 *disk.Reader
	AtsConf             *conf.ATSConfig
	DocLoadMutex        *sync.RWMutex
}

func NewCacheDisk(path string, atsconf *conf.ATSConfig) (*CacheDisk, error) {
	/// 初始化reader
	dr := &disk.Reader{}
	err := dr.Open(path)
	if err != nil {
		return nil, fmt.Errorf("parse disk %s failed: %s", path, err.Error())
	}

	cd := &CacheDisk{
		Start:        START,
		Dio:          dr,
		Path:         path,
		AtsConf:      atsconf,
		DocLoadMutex: new(sync.RWMutex),
	}

	// 初始化必要的磁盘信息
	err = cd.initGeometryInfo()
	if err != nil {
		return nil, fmt.Errorf("init disk geometry info failed: %s", err.Error())
	}

	// 计算磁盘头长度，需要注意DiskVolBlock的个数
	diskVolBlockSpaceCnt := 1
	l := (cd.Len * STORE_BLOCK_SIZE) - (cd.Start - cd.Skip)

	if l >= MIN_VOL_SIZE {
		cd.HeaderLen = DiskHeaderLen + (l/MIN_VOL_SIZE-1)*DiskVolBlockLen
		diskVolBlockSpaceCnt += int(l/MIN_VOL_SIZE - 1)
	} else {
		cd.HeaderLen = DiskHeaderLen
	}
	cd.Start = cd.Skip + cd.HeaderLen

	// 初始化变量
	cd.Header = &DiskHeader{
		VolInfos: make([]*DiskVolBlock, diskVolBlockSpaceCnt),
	}
	for i := 0; i < diskVolBlockSpaceCnt; i++ {
		cd.Header.VolInfos[i] = &DiskVolBlock{}
	}

	return cd, nil
}

// 创建磁盘vol
// @volIdx vol索引号
// @volSizeInBlocks vol期望申请大小
// @schemeType int scheme类型，http或者stream
// @return
func (cd *CacheDisk) createVolume(volIdx int, volSizeInBlocks int, schemeType int) *DiskVolBlock {

	return nil
}

// 所需数据大小
func (cd *CacheDisk) CacheDiskHeaderLen() int64 {
	return DiskHeaderLen
}

//// 从buffer中加载CacheDisk结构信息
//func (cd *CacheDisk) load(buffer []byte) error {
//	if len(buffer) < DiskHeaderLen {
//		return fmt.Errorf("need %d raw data for parse disk info", DiskHeaderLen)
//	}
//
//	// 预存数据
//	if cd.DebugLoad {
//		cd.PsRawDiskHeaderData = make([]byte, DiskHeaderLen)
//		copy(cd.PsRawDiskHeaderData, buffer)
//	}
//
//	// 分析磁盘头
//	header, err := cd.loadDiskHeader(buffer[:DiskHeaderLen])
//	if err != nil {
//		return err
//	}
//	cd.Header = header
//
//	return nil
//}

//
func (cd *CacheDisk) initGeometryInfo() error {
	cd.Geometry = GetGeometry()

	// todo: 此处应该引入span结构体
	cd.Len = GetGeometry().TotalSZ/STORE_BLOCK_SIZE - STORE_BLOCK_SIZE>>STORE_BLOCK_SHIFT
	cd.Skip = STORE_BLOCK_SIZE

	return nil
}

const CacheFileSize = 268435456

func (cd *CacheDisk) FindURL(urlStr string) (*Doc, error) {
	if cd.YYVol == nil {
		return nil, fmt.Errorf("%s", "cache do not initialize")
	}
	u := proxy.CacheURL{}
	u11, _ := url.Parse(urlStr)

	hash := u.HashGet(u11)
	fmt.Println(hash)
	fmt.Println(binary.LittleEndian.Uint32(hash[0:4]))
	d1, d2 := cd.YYVol.DirProbe(hash)
	fmt.Printf("result: %s, %s\n", d1, d2)
	if d1 == nil {
		fmt.Println("no dir found!")
		return nil, nil
	}

	// get doc from dir
	docPos := int64(d1.Offset-1)*DEFAULT_HW_SECTOR_SIZE + cd.YYVol.ContentStartPos
	buff, err := cd.Dio.Read(docPos, 72)
	if err != nil {
		return nil, err
	}
	newDoc, err := NewDoc(buff)
	if err != nil {
		return nil, fmt.Errorf("parse doc failed: %s", err.Error())
	}
	return newDoc, nil
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
	//fmt.Printf("total FullDir : %d, need parse: %d\n", len(v.YYFullDir), max)
	for _, dir := range v.YYFullDir {
		cc, _ := json.Marshal(dir)
		fmt.Println(string(cc))

		docPos := int64(dir.Offset-1)*DEFAULT_HW_SECTOR_SIZE + v.ContentStartPos
		buff, err := cd.Dio.Read(docPos, 72)
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
		//if newDoc.HLen == 0 {
		//	continue
		//}

		cd.DocLoadMutex.Lock()
		v.Content = append(v.Content, newDoc)
		cd.DocLoadMutex.Unlock()
		max = max - 1
		if max < 1 {
			break
		}

	}
	//fmt.Printf("total content: %d\n", len(v.Content))
	return nil
}

// 从doc中提出http信息
func (cd *CacheDisk) ExtractHttpInfoHeader(doc *Doc) (*proxy.HTTPCacheAlt, error) {
	if doc.Magic != DOC_MAGIC {
		return nil, fmt.Errorf("doc magic not match")
	}
	if doc.HLen == 0 {
		return nil, fmt.Errorf("doc is empty")
	}

	startPos := doc.YYDiskOffset + 72
	//fmt.Printf("dir h len: %d\n", d.HLen)
	buf, err := cd.Dio.Read(startPos, int64(doc.HLen))
	if err != nil {
		return nil, err
	}

	hi := &proxy.HTTPCacheAlt{}
	hi.YYDiskOffset = startPos
	hi.LoadFromBuffer(buf)

	if hi.Magic != proxy.CACHE_ALT_MAGIC_MARSHALED {
		return nil, fmt.Errorf("not http info block")
	}

	return hi, nil
}
