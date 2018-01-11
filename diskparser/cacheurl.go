package diskparser

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"strconv"
)

// HASH值占16个字节
type CryptoHash struct {
	U64 [2]uint64
	B   [2]uint64
	U32 [4]uint32
	U8  [16]uint8
}

type CacheURL struct {
}

// 根据URL产生KEY
func (cu *CacheURL) HashGet(url URL) []byte {

	// 可以选择是否加入method进hash
	// http://:@127.0.0.1/1.jpg;?fdafadf=12112
	strs := make([]string, 13)

	strs[0] = url.Scheme
	strs[1] = "://"
	strs[2] = url.User
	strs[3] = ":"
	strs[4] = url.Password
	strs[5] = "@"
	strs[6] = url.Host // 注意，没有port信息
	strs[7] = "/"
	strs[8] = url.Path

	strs[9] = ";"
	strs[10] = url.Params
	strs[11] = "?"
	strs[12] = url.Query

	// update

	// update port, 注意，是update的int，4个字节，而不是port字符串
	fmt.Println(url.Port)

	port, err := strconv.Atoi(url.Port)
	if err != nil {
		return []byte{}
	}

	hasher := md5.New()
	for _, s := range strs {
		hasher.Write([]byte(s))
	}
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(port))
	hasher.Write(bs)

	return hasher.Sum(nil)
}
