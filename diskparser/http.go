package diskparser

import (
	"encoding/binary"
	"encoding/json"
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
	Magic        uint32 `json:"magic"`  // 校验码
	MSize        uint32 `json:"m_size"` // 大小
	YYDiskOffset int64  `json:"yy_disk_offset"`
}

func (hca *HTTPCacheAlt) LoadFromBuffer(buffer []byte) error {

	var curPos int64 = 0
	hca.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	if hca.Magic != CACHE_ALT_MAGIC_MARSHALED {
		return nil
	}

	reqh := &HTTPHdr{}
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

	curPos += 4
	reqhstr, _ := json.Marshal(reqh)
	fmt.Println(string(reqhstr))

	//resh := &HTTPHdr{}
	//resh.YYDiskOffset = hca.YYDiskOffset + 112
	//
	return nil
}
