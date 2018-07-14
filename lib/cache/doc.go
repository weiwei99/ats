package cache

import (
	"encoding/binary"
	"github.com/weiwei99/ats/lib/proxy"
)

const (
	SIZEOF_DOC = 72
)

// Note : hdr() needs to be 8 byte aligned. // 小文件一个doc存储，大文件则会将《内容》分为多个fragment，每个fragment有一个doc
// If you change this, change sizeofDoc above
type Doc struct {
	Magic          uint32              `json:"magic"`     // DOC_MAGIC
	Len            uint32              `json:"len"`       //length of this fragment (including hlen & sizeof(Doc), unrounded)
	TotalLen       uint64              `json:"total_len"` // total length of document
	FirstKey       []byte              `json:"first_key"` //16 ///< first key in object. // 小文件则是对象
	Key            []byte              `json:"key"`       //16 ///< Key for this doc.
	HLen           uint32              `json:"h_len"`     ///< Length of this header.
	DocByte        uint8               `json:"doc_byte"`  //8
	VMajor         uint8               `json:"v_major"`   //8
	VMinor         uint8               `json:"v_minor"`   //8
	UnUsed         uint8               `json:"un_used"`   //8
	SyncSerial     uint32              `json:"sync_serial"`
	WriteSerial    uint32              `json:"write_serial"`
	Pinned         uint32              `json:"pinned"` // pinned until
	CheckSum       uint32              `json:"check_sum"`
	YYDiskOffset   int64               `json:"yy_disk_offset"`
	HttpInfoHeader *proxy.HTTPCacheAlt `json:"http_info_header"`
}

func NewDoc(buffer []byte) (*Doc, error) {
	d := &Doc{}
	err := d.loadFromBuffer(buffer)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// 返回HTTP的header的初始位置
func (d *Doc) Hdr() {

}

func (d *Doc) Data() []byte {

	return []byte{}
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
	d.DocByte = uint8(data & 0xff)

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

func (d *Doc) SingleFragment() bool {
	return d.DataLen() == uint32(d.TotalLen)
}
