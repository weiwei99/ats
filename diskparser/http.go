package diskparser

import (
	"encoding/binary"
	//"encoding/hex"
	"fmt"
)

const (
	CACHE_ALT_MAGIC_ALIVE     = 0xabcddeed // 能够判断是否在内存中获取对象
	CACHE_ALT_MAGIC_MARSHALED = 0xdcbadeed // 从磁盘中获取对象
	HDR_BUF_MAGIC_MARSHALED   = 0xDCBAFEED // http的header magic

	INFO_LEN              = 110592
	HTTP_ALT_MARSHAL_SIZE = 248 // 先写死，实际上是：ROUND(sizeof(HTTPCacheAlt), HDR_PTR_SIZE); 且#define ROUND(x, l) (((x) + ((l)-1L)) & ~((l)-1L))
)

// HTTP信息
type HTTPCacheAlt struct {
	Magic        uint32   `json:"magic"`
	RequestHdr   *HTTPHdr `json:"request_hdr"`
	ResponseHdr  *HTTPHdr `json:"response_hdr"`
	BeloneDir    *Dir     `json:"-"`
	YYDiskOffset int64    `json:"yy_disk_offset"`
}

//class HTTPHdr : public MIMEHdr
type HTTPHdr struct {
	HeapPos      int64    `json:"heap_pos"`
	HdrHeep      *HdrHeep `json:"hdr_heep"`
	YYDiskOffset int64    `json:"yy_disk_offset"`
}

type HdrHeep struct {
	Magic        uint32 `json:"magic"`        // 校验码
	MFreeStart   uint64 `json:"m_free_start"` // point addr 64 bits
	MDataStart   uint64 `json:"m_data_start"` // point addr 64 bits
	MSize        uint32 `json:"m_size"`       // 大小
	YYDiskOffset int64  `json:"yy_disk_offset"`
	RawBytes     []byte `json:"-"`
	URL          *URL   `json:"url"`
}

type HdrHeapObjHeader struct {
	MType        uint32   `json:"m_type"`      // 对象类型
	MLength      uint32   `json:"m_length"`    // 对象长度
	MObjFlags    uint32   `json:"m_obj_flags"` // 对象标志
	Content      []byte   `json:"-"`
	YYDiskOffset int64    `json:"yy_disk_offset"`
	HdrHeep      *HdrHeep `json:"-"`
}

//
// \---alt: 48 ---| ---hdrHeap:
func (hca *HTTPCacheAlt) LoadFromBuffer(buffer []byte) error {

	var curPos int64 = 0
	hca.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	if hca.Magic != CACHE_ALT_MAGIC_MARSHALED {
		return nil
	}

	reqh := &HTTPHdr{}
	hca.RequestHdr = reqh

	reqh.YYDiskOffset = hca.YYDiskOffset + 48
	curPos = 48
	reqh.HeapPos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))

	reqh.HdrHeep = &HdrHeep{}
	// len = 2216
	curPos = reqh.HeapPos
	reqh.HdrHeep.YYDiskOffset = hca.YYDiskOffset + reqh.HeapPos // 248

	reqh.HdrHeep.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])

	if reqh.HdrHeep.Magic != HDR_BUF_MAGIC_MARSHALED {
		fmt.Println("hdr heep magic error")
		return fmt.Errorf("hdr heep magic error11111")
	}
	reqh.HdrHeep.RawBytes = buffer[curPos:]
	curPos += 4
	curPos += 4 // 4个结构体对齐
	// 跳过两个指针空间
	reqh.HdrHeep.MFreeStart = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	reqh.HdrHeep.MDataStart = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	//
	reqh.HdrHeep.MSize = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])

	curPos += 4
	//reqhstr, _ := json.Marshal(reqh)
	//fmt.Println(string(reqhstr))

	reqh.HdrHeep.MFreeStart = uint64(reqh.HdrHeep.MSize)
	heapobjbuf := buffer[uint64(reqh.HeapPos)+reqh.HdrHeep.MDataStart : uint64(reqh.HeapPos)+reqh.HdrHeep.MFreeStart]

	grap := (reqh.HdrHeep.MFreeStart - reqh.HdrHeep.MDataStart)

	//fmt.Println(hex.Dump(heapobjbuf[0:1]))
	//return nil
	var next int64
	//return nil
	for {
		cc := &HdrHeapObjHeader{}
		cc.YYDiskOffset = reqh.YYDiskOffset + int64(reqh.HdrHeep.MDataStart) + next
		cc.HdrHeep = reqh.HdrHeep

		err := cc.Load(heapobjbuf[next:grap])
		//fmt.Println(hex.Dump(heapobjbuf[next:grap]))
		if err != nil {
			break
		}

		//ccstr, _ := json.Marshal(cc)
		//fmt.Println(string(ccstr))
		next += int64(cc.MLength)
		//fmt.Println(hex.Dump(heapobjbuf[next:grap]))
		//fmt.Printf("=---: %d\n", cc.MLength)
		if next >= int64(grap) {
			break
		}
		//break
	}

	resh := &HTTPHdr{}
	hca.RequestHdr = resh
	resh.YYDiskOffset = hca.YYDiskOffset + 112

	return nil
}

func (hhdr *HdrHeapObjHeader) Load(buffer []byte) error {

	curPos := 0
	tmp := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	hhdr.MType = uint32(tmp & 0x00ff)

	tmp1 := binary.LittleEndian.Uint16(buffer[curPos+1 : curPos+3])
	hhdr.MLength = uint32(tmp1 & 0xffff)
	tmp2 := binary.LittleEndian.Uint16(buffer[curPos+2 : curPos+4])
	hhdr.MLength = (uint32(tmp2&0x0f00))<<16 + hhdr.MLength

	//hhdr.MLength = (uint32(tmp2&0x000f))<<16 + hhdr.MLength

	hhdr.MObjFlags = uint32(tmp2&0xff00) >> 8

	curPos += 4
	hhdr.Content = buffer[curPos : curPos+int(hhdr.MLength)]

	fmt.Printf("begin parse: %d, content len: %d, object flag: %d\n",
		hhdr.MType, hhdr.MLength, hhdr.MObjFlags)
	if hhdr.MType == uint32(HDR_HEAP_OBJ_URL) {
		err := hhdr.UnmarshalURL(hhdr.Content)
		if err != nil {
			return err
		}

	} else if hhdr.MType == uint32(HDR_HEAP_OBJ_HTTP_HEADER) {
		err := hhdr.UnmarshalHTTPHdr(hhdr.Content)
		if err != nil {
			return err
		}

	} else if hhdr.MType == uint32(HDR_HEAP_OBJ_MIME_HEADER) {
		err := hhdr.UnmarshalMIME(hhdr.Content)
		if err != nil {
			return err
		}
	} else if hhdr.MType == uint32(HDR_HEAP_OBJ_FIELD_BLOCK) {
		err := hhdr.UnmarshalMIME(hhdr.Content)
		if err != nil {
			return err
		}
	}

	return nil
}
