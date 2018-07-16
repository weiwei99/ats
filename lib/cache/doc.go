package cache

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/weiwei99/ats/lib/proxy"
	"math"
	"strings"
)

const (
	SIZEOF_DOC = 72
)

// Note : hdr() needs to be 8 byte aligned. // 小文件一个doc存储，大文件则会将《内容》分为多个fragment，每个fragment有一个doc
// If you change this, change sizeofDoc above
type Doc struct {
	// Validity check value. Set to DOC_MAGIC for a valid document.
	Magic uint32 `json:"magic"` // DOC_MAGIC
	// The length of this segment including the header length, fragment table, and this structure.
	Len uint32 `json:"len"` //length of this fragment (including hlen & sizeof(Doc), unrounded)
	// Total length of the entire document not including meta data but including headers.
	TotalLen uint64 `json:"total_len"` // total length of document
	// First index key in the document (the index key used to locate this object in the volume index).
	FirstKey []byte `json:"first_key"` //16 ///< first key in object. // 小文件则是对象
	// The index key for this fragment. Fragment keys are computationally chained so that the key for the next and previous fragments can be computed from this key.
	Key []byte `json:"key"` //16 ///< Key for this doc.
	// Document header (metadata) length. This is not the length of the HTTP headers.
	HLen uint32 `json:"h_len"` ///< Length of this header.

	DocType     uint8  `json:"doc_type"` //8
	VMajor      uint8  `json:"v_major"`  //8
	VMinor      uint8  `json:"v_minor"`  //8
	UnUsed      uint8  `json:"un_used"`  //8
	SyncSerial  uint32 `json:"sync_serial"`
	WriteSerial uint32 `json:"write_serial"`
	// Flag and timer for pinned objects.
	Pinned         uint32              `json:"pinned"` // pinned until
	CheckSum       uint32              `json:"check_sum"`
	HeaderData     []byte              `json:"-"`
	Data           []byte              `json:"-"`
	YYDiskOffset   int64               `json:"yy_disk_offset"`
	HttpInfoHeader *proxy.HTTPCacheAlt `json:"http_info_header"`
}

func NewDocFromBuffer(buffer []byte) (*Doc, error) {
	d := &Doc{}
	err := d.loadFromBuffer(buffer)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// 返回HTTP的header的初始位置
func (d *Doc) Hdr() []byte {
	return d.HeaderData
}

func (d *Doc) GetData() []byte {

	return d.Data
}

// 将header unmarshl出来
func (d *Doc) Unmarshal() {

	// 根据doc.len考虑
}

func (d *Doc) loadFromBuffer(buffer []byte) error {
	curPos := 0
	d.Magic = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4

	d.Len = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4

	d.TotalLen = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8

	d.FirstKey = buffer[curPos : curPos+16]
	curPos += 16

	d.Key = buffer[curPos : curPos+16]
	curPos += 16

	d.HLen = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4

	data := binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	d.DocType = uint8(data & 0xff)

	d.VMajor = uint8((data >> 8) & 0xff)
	d.VMinor = uint8((data >> 16) & 0xff)
	d.UnUsed = uint8((data >> 24) & 0xff)
	curPos += 4

	d.SyncSerial = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	d.WriteSerial = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	d.Pinned = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	d.CheckSum = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4

	return nil
}

func (d *Doc) DataLen() uint32 {
	return d.Len - SIZEOF_DOC - d.HLen
}

func (d *Doc) PrefixLen() uint32 {
	return SIZEOF_DOC + d.HLen
}

func (d *Doc) SingleFragment() bool {
	return d.DataLen() == uint32(d.TotalLen)
}

//
/*
enum CacheFragType {
   CACHE_FRAG_TYPE_NONE,
   CACHE_FRAG_TYPE_HTTP_V23, ///< DB version 23 or prior.
   CACHE_FRAG_TYPE_RTSP,     ///< Should be removed once Cache Toolkit is implemented.
   CACHE_FRAG_TYPE_HTTP,
   NUM_CACHE_FRAG_TYPES
};
*/
func docTypeString(dt uint8) string {
	if dt == 0 {
		return "CACHE_FRAG_TYPE_NONE"
	} else if dt == 1 {
		return "CACHE_FRAG_TYPE_HTTP_V23"
	} else if dt == 2 {
		return "CACHE_FRAG_TYPE_RTSP"
	} else if dt == 3 {
		return "CACHE_FRAG_TYPE_HTTP"
	} else {
		return "CACHE_FRAG_TYPE_UNKNOWN"
	}
}

func (d *Doc) LoadDocHeader(buffer []byte) error {

	return nil
}

func (d *Doc) LoadDocBody(buffer []byte) error {

	return nil
}
func (d *Doc) Dump() string {

	docPrefix := []byte{}
	if d.Data != nil && d.DataLen() > 0 {
		docPrefix = d.Data[:int(math.Min(8, float64(d.DataLen())))]
	}

	fragmentInfo := ""
	if d.SingleFragment() {
		fragmentInfo = "single fragment"
	} else {
		fragmentInfo = "multi fragment"
		if d.HttpInfoHeader != nil {
			fragmentInfo = fmt.Sprintf("%s - %d", fragmentInfo, d.HttpInfoHeader.FragOffsetCount)
		}
	}

	ret := fmt.Sprintf(
		"doc key: %s, version: %d.%d, type: %s, len: %d, total_len %d,"+
			" h_len: %d, %s, prefix_len: %d, data payload: %d, pinned: %d",
		strings.ToUpper(hex.EncodeToString(d.Key)),
		d.VMajor, d.VMinor,
		docTypeString(d.DocType), d.Len, d.TotalLen, d.HLen,
		fragmentInfo, d.PrefixLen(), d.DataLen(), d.Pinned,
	)

	//
	ret = fmt.Sprintf("%s body prefix: %s", ret, hex.Dump(docPrefix))
	return ret
}
