package proxy

import (
	"encoding/binary"
	"fmt"
)

const (
	CACHE_ALT_MAGIC_ALIVE     = 0xabcddeed // 能够判断是否在内存中获取对象
	CACHE_ALT_MAGIC_MARSHALED = 0xdcbadeed // 从磁盘中获取对象
	HDR_BUF_MAGIC_MARSHALED   = 0xDCBAFEED // http的header magic

	INFO_LEN              = 110592
	HTTP_ALT_MARSHAL_SIZE = 248 // 先写死，实际上是：ROUND(sizeof(HTTPCacheAlt), HDR_PTR_SIZE); 且#define ROUND(x, l) (((x) + ((l)-1L)) & ~((l)-1L))
)

type HTTPInfo struct {
	M_ALT *HTTPCacheAlt
}

func (hi *HTTPInfo) RequestGet(hdr *HTTPHdr) {

}

func (hi *HTTPInfo) ResponseGet(hdr *HTTPInfo) {

}

// HTTP信息
type HTTPCacheAlt struct {
	Magic      uint32    `json:"magic"`
	ID         int32     `json:"id"`
	ObjectKey  [4]uint32 `json:"object_key"`
	ObjectSize [2]uint32 `json:"object_size"`

	RequestHdr           *HTTPHdr `json:"request_hdr"`
	ResponseHdr          *HTTPHdr `json:"response_hdr"`
	RequestSentTime      int64    `json:"request_sent_time"`
	ResponseReceivedTime int64    `json:"response_received_time"`
	FragOffsetCount      int      `json:"frag_offset_count"`
	FragOffset           uint64   `json:"-"` // TODO： 指针

	//BeloneDir    *cache.Dir `json:"-"`
	YYDiskOffset int64 `json:"yy_disk_offset"`
}

//class HTTPHdr : public MIMEHdr
type HTTPHdr struct {
	HeapPos      int64    `json:"heap_pos"`
	HdrHeep      *HdrHeep `json:"hdr_heep"`
	YYDiskOffset int64    `json:"yy_disk_offset"`
}

type HdrHeep struct {
	Magic        uint32              `json:"magic"`        // 校验码
	MFreeStart   uint64              `json:"m_free_start"` // point addr 64 bits
	MDataStart   uint64              `json:"m_data_start"` // point addr 64 bits
	MSize        uint32              `json:"m_size"`       // 大小
	YYDiskOffset int64               `json:"yy_disk_offset"`
	RawBytes     []byte              `json:"-"`
	URL          *URLObj             `json:"url"`
	HdrObjects   []*HdrHeapObjHeader `json:"-"`
}

type HdrHeapObjHeader struct {
	MType        uint32      `json:"m_type"`      // 对象类型
	MLength      uint32      `json:"m_length"`    // 对象长度
	MObjFlags    uint32      `json:"m_obj_flags"` // 对象标志
	Content      []byte      `json:"-"`
	YYDiskOffset int64       `json:"yy_disk_offset"`
	HttpHdr      *HTTPHdrImp `json:"-"`
	Url          *URLObj     `json:"-"`

	HdrHeep *HdrHeep `json:"-"`
}

//
// \---alt: 48 ---| ---hdrHeap:
func (hca *HTTPCacheAlt) LoadFromBuffer(buffer []byte) error {

	if len(buffer) < 100 {
		return fmt.Errorf("not enough buffer for http cache alt: %d", len(buffer))
	}
	var curPos int64 = 0
	hca.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	if hca.Magic != CACHE_ALT_MAGIC_MARSHALED {
		return fmt.Errorf("magic not match")
	}

	curPos = 4
	hca.ID = int32(binary.LittleEndian.Uint32(buffer[curPos : curPos+4]))

	curPos = 20
	hca.ObjectKey[0] = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	hca.ObjectKey[1] = binary.LittleEndian.Uint32(buffer[curPos+4 : curPos+8])
	hca.ObjectKey[2] = binary.LittleEndian.Uint32(buffer[curPos+8 : curPos+12])
	hca.ObjectKey[3] = binary.LittleEndian.Uint32(buffer[curPos+12 : curPos+16])

	curPos = 36
	hca.ObjectSize[0] = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	hca.ObjectSize[1] = binary.LittleEndian.Uint32(buffer[curPos+4 : curPos+8])

	// request header process
	//fmt.Println("------begin parse request info----")
	requestHeader := &HTTPHdr{}

	REQUEST_HDR_OFFSET := 48
	requestHeader.YYDiskOffset = hca.YYDiskOffset + int64(REQUEST_HDR_OFFSET)
	curPos = 48
	requestHeader.HeapPos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	// len = 2216
	curPos = requestHeader.HeapPos
	reqHeap, err := UnmarshalHeap(buffer[curPos:])
	if err != nil {
		return fmt.Errorf("unmarshalheap failed: %s", err.Error())
	}
	reqHeap.YYDiskOffset = hca.YYDiskOffset + requestHeader.HeapPos // 248
	requestHeader.HdrHeep = reqHeap
	hca.RequestHdr = requestHeader

	// response header process
	//fmt.Println("------begin parse reponse info----")
	responseHeader := &HTTPHdr{}

	responseHeader.YYDiskOffset = hca.YYDiskOffset + 112
	curPos = 112
	responseHeader.HeapPos = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))
	curPos = responseHeader.HeapPos
	respHeap, err := UnmarshalHeap(buffer[curPos:])
	if err != nil {
		return err
	}
	respHeap.YYDiskOffset = hca.YYDiskOffset + requestHeader.HeapPos // 248
	responseHeader.HdrHeep = respHeap
	hca.ResponseHdr = responseHeader

	//
	curPos = 176
	hca.RequestSentTime = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))

	curPos = 184
	hca.ResponseReceivedTime = int64(binary.LittleEndian.Uint64(buffer[curPos : curPos+8]))

	curPos = 192
	hca.FragOffsetCount = int(binary.LittleEndian.Uint32(buffer[curPos : curPos+8]))

	return nil
}

func UnmarshalHeap(buffer []byte) (*HdrHeep, error) {
	hdrHeap := &HdrHeep{
		HdrObjects: make([]*HdrHeapObjHeader, 0),
	}

	curPos := 0

	hdrHeap.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])

	if hdrHeap.Magic != HDR_BUF_MAGIC_MARSHALED {
		fmt.Println("hdr heep magic error")
		return nil, fmt.Errorf("hdr heep magic error11111")
	}
	hdrHeap.RawBytes = buffer[curPos:]
	curPos += 4
	curPos += 4 // 4个结构体对齐
	// 跳过两个指针空间
	hdrHeap.MFreeStart = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	hdrHeap.MDataStart = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	//
	hdrHeap.MSize = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])

	curPos += 4
	//reqhstr, _ := json.Marshal(reqh)
	//fmt.Println(string(reqhstr))

	hdrHeap.MFreeStart = uint64(hdrHeap.MSize)

	heapobjbuf := buffer[hdrHeap.MDataStart:hdrHeap.MFreeStart]

	grap := hdrHeap.MFreeStart - hdrHeap.MDataStart

	//fmt.Println(hex.Dump(heapobjbuf[0:1]))
	//return nil
	var next int64
	//return nil
	for {
		cc := &HdrHeapObjHeader{}
		//cc.YYDiskOffset = reqh.YYDiskOffset + int64(hdrHeap.MDataStart) + next
		cc.HdrHeep = hdrHeap

		err := cc.ExtractObjects(heapobjbuf[next:grap])
		//fmt.Println(hex.Dump(heapobjbuf[next:grap]))
		if err != nil {
			break
		}
		hdrHeap.HdrObjects = append(hdrHeap.HdrObjects, cc)
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
	return hdrHeap, nil
}

func (hhdr *HdrHeapObjHeader) ExtractObjects(buffer []byte) error {

	curPos := 0
	tmp := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	hhdr.MType = uint32(tmp & 0x00ff)

	tmp1 := binary.LittleEndian.Uint16(buffer[curPos+1 : curPos+3])
	hhdr.MLength = uint32(tmp1 & 0xffff)
	tmp2 := binary.LittleEndian.Uint16(buffer[curPos+2 : curPos+4])
	hhdr.MLength = (uint32(tmp2&0x0f00))<<16 + hhdr.MLength

	// hhdr.MLength = (uint32(tmp2&0x000f))<<16 + hhdr.MLength

	hhdr.MObjFlags = uint32(tmp2&0xff00) >> 8

	curPos += 4
	hhdr.Content = buffer[curPos : curPos+int(hhdr.MLength)]

	// fmt.Printf("begin parse: %d, content len: %d, object flag: %d\n",
	// hhdr.MType, hhdr.MLength, hhdr.MObjFlags)
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
