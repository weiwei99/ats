package diskparser

import (
	"encoding/binary"
	"encoding/hex"
)

type HDR_HEAP_OBJ_TYPE int

const (
	HDR_HEAP_OBJ_EMPTY HDR_HEAP_OBJ_TYPE = iota
	HDR_HEAP_OBJ_RAW
	HDR_HEAP_OBJ_URL
	HDR_HEAP_OBJ_HTTP_HEADER
	HDR_HEAP_OBJ_MIME_HEADER
	HDR_HEAP_OBJ_FIELD_BLOCK
)

type URLObj struct {
	Scheme        string `json:"scheme"`
	User          string `json:"user"`
	Password      string `json:"password"`
	Host          string `json:"host"`
	Port          string `json:"port"`
	Path          string `json:"path"`
	Params        string `json:"params"`
	Query         string `json:"query"`
	Frangment     string `json:"frangment"`
	PrintedString string `json:"printed_string"` // 该变量用不到，无需关心，且不能赋值
}

type HTTPHdrImp struct {
	MPolarity    uint32 `json:"m_polarity"`
	MVersion     int32  `json:"m_version"`
	MethodOffset uint64 `json:"method_offset"`

	// request
	URLImplOffset uint64 `json:"url_impl_offset"`
	MethodLen     uint16 `json:"method_len"`
	MethodWKSIdx  int16  `json:"method_wks_idx"`
	Method        string `json:"method"`

	// response
	Status     int16  `json:"status"`
	MLenReason uint16 `json:"m_len_reason"`
	//0x7fffeae001d8
	//m_url_impl = 0x308
	//m_ptr_method //0x7fffeae004b8
	//m_len_method //16 0x7fffeae001e8
	//m_method_wks_idx
}

type MIMEHdrImp struct {
	MPresenceBits     uint64             `json:"m_presence_bits"`
	MSlotAccelerators []uint32           `json:"m_slot_accelerators"`
	MCookedStuff      []uint32           `json:"m_cooked_stuff"` // 24个字节
	MFirstFblock      MIMEFieldBlockImpl `json:"m_first_fblock"`
	Headers           []string           `json:"headers"`
}

type MIMEFieldBlockImpl struct {
	MFreetop    uint32        `json:"m_freetop"`
	MFieldSlots [16]MIMEField `json:"m_field_slots"`
}

type MIMEField struct {
	Value string `json:"value"`
}

type MIMECookedCacheControl struct {
}

type MIMECookedPragma struct {
}

func (hhdr *HdrHeapObjHeader) UnmarshalURL(buffer []byte) error {

	curPos := 0
	toLen := make([]uint16, 0)

	url := &URLObj{}
	//
	for i := 0; i < 9; i++ {
		tmp1 := binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
		//fmt.Println(tmp1)
		curPos += 2
		toLen = append(toLen, tmp1)
	}
	curPos += 2 // skip printed_string len
	//
	tmp1 := binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Scheme = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[0])])
	}

	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.User = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[1])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Password = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[2])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Host = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[3])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Port = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[4])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Path = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[5])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Params = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[6])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Query = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[7])])
	}
	tmp1 = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	if tmp1 != 0 {
		url.Frangment = string(hhdr.HdrHeep.RawBytes[tmp1 : tmp1+uint64(toLen[8])])
	}

	hhdr.HdrHeep.URL = url
	return nil
}

func (hhdr *HdrHeapObjHeader) UnmarshalMIME(buffer []byte) error {
	mime := &MIMEHdrImp{
		MPresenceBits:     0,
		MSlotAccelerators: make([]uint32, 4),
		MCookedStuff:      make([]uint32, 6),
	}
	curPos := 0
	// skip 8 bytes
	curPos += 8
	//fmt.Println(hex.Dump(buffer))
	mime.MPresenceBits = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8     // m_presence_bits
	curPos += 4 * 4 // m_slot_accelerators
	curPos += 24    // MIMECooked
	curPos += 8     // point

	mime.MFirstFblock.MFreetop = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])

	//mimestr, _ := json.Marshal(mime)
	//fmt.Println(string(mimestr))
	return nil
}

func (hhdr *HdrHeapObjHeader) UnmarshalHTTPHdr(buffer []byte) error {
	hex.Dump(buffer)
	hdr := &HTTPHdrImp{}
	curPos := 0
	hdr.MPolarity = binary.LittleEndian.Uint32(buffer[curPos : curPos+4])
	curPos += 4
	hdr.MVersion = int32(binary.LittleEndian.Uint32(buffer[curPos : curPos+4]))
	curPos += 4

	// 偏移
	curPos += 4
	// C语言中是union结构体
	if hdr.MPolarity == 1 {
		hhdr.UnmarshalRequestHTTPHdr(hdr, buffer[curPos:])
	} else if hdr.MPolarity == 2 {
		hhdr.UnmarshalResponseHTTPHdr(hdr, buffer[curPos:])
	}

	hhdr.HttpHdr = hdr
	//st, _ := json.Marshal(hdr)
	//fmt.Println(string(st))
	return nil
}

func (hhdr *HdrHeapObjHeader) UnmarshalRequestHTTPHdr(hdr *HTTPHdrImp, buffer []byte) {
	curPos := 0

	//
	hdr.URLImplOffset = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8
	hdr.MethodOffset = binary.LittleEndian.Uint64(buffer[curPos : curPos+8])
	curPos += 8

	hdr.MethodLen = binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	curPos += 2
	hdr.MethodWKSIdx = int16(binary.LittleEndian.Uint16(buffer[curPos : curPos+2]))
	curPos += 2

	//st, _ := json.Marshal(hdr)
	//fmt.Println(string(st))
}

func (hhdr *HdrHeapObjHeader) UnmarshalResponseHTTPHdr(hdr *HTTPHdrImp, buffer []byte) {
	curPos := 0
	//u.resp.m_ptr_reason 8个字节
	curPos += 8
	//fmt.Println(hex.Dump(buffer))

	hdr.MLenReason = binary.LittleEndian.Uint16(buffer[curPos : curPos+2])
	curPos += 2
	hdr.Status = int16(binary.LittleEndian.Uint16(buffer[curPos : curPos+2]))

}
