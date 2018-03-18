package common

import (
	"crypto/md5"
	"encoding/binary"
)

// HASH值占16个字节
type CryptoHash struct {
	U64 [2]uint64
	B   [2]uint64
	U32 [4]uint32
	U8  [16]uint8
}

func NewCryptoHash(seed string) *CryptoHash {
	hasher := md5.New()
	hasher.Write([]byte(seed))

	hashData := hasher.Sum(nil)

	ch := &CryptoHash{
		U64: [2]uint64{
			binary.LittleEndian.Uint64(hashData[0:8]),
			binary.LittleEndian.Uint64(hashData[8:16]),
		},
		U32: [4]uint32{
			binary.LittleEndian.Uint32(hashData[0:4]),
			binary.LittleEndian.Uint32(hashData[4:8]),
			binary.LittleEndian.Uint32(hashData[8:12]),
			binary.LittleEndian.Uint32(hashData[12:16]),
		},
	}
	return ch
}

func (ch *CryptoHash) Fold() uint64 {
	return ch.U64[0] ^ ch.U64[1]
}
