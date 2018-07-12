package proxy

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"net"
	"net/url"
	"strconv"
)

type CacheURL struct {
}

// 根据URL产生KEY
func (cu *CacheURL) HashGet(url *url.URL) []byte {

	// 可以选择是否加入method进hash
	// http://:@127.0.0.1/1.jpg;?fdafadf=12112
	strs := make([]string, 13)

	host, port, err := net.SplitHostPort(url.Host)
	if err != nil {
		host = url.Host
		if url.Scheme == "http" {
			port = "80"
		} else if url.Scheme == "https" {
			port = "443"
		}
	}

	strs[0] = url.Scheme
	strs[1] = "://"
	if url.User != nil {
		strs[2] = url.User.Username()
		strs[4], _ = url.User.Password()
	} else {
		strs[2] = ""
		strs[4] = ""
	}
	strs[3] = ":"
	strs[5] = "@"
	strs[6] = host // 注意，没有port信息
	//strs[7] = "/" -- 无需填充 /，C语言的版本会有 /
	strs[8] = url.Path

	strs[9] = ";"
	strs[10] = ""
	strs[11] = "?"
	strs[12] = url.RawQuery

	// update

	// update port, 注意，是update的int，4个字节，而不是port字符串

	nport, err := strconv.Atoi(port)
	if err != nil {
		return []byte{}
	}

	hasher := md5.New()
	for _, s := range strs {
		hasher.Write([]byte(s))
		fmt.Printf("%s", s)
	}
	fmt.Println()
	bs := make([]byte, 2)
	binary.LittleEndian.PutUint16(bs, uint16(nport))
	hasher.Write(bs)

	return hasher.Sum(nil)
}

// 根据key选择vol
func (cu *CacheURL) KeyToVol(key []byte, host string) {
	VOL_HASH_TABLE_SIZE := 32707
	DIR_TAG_WIDTH := 12

	a := (binary.LittleEndian.Uint32(key[8:12]) >> uint32(DIR_TAG_WIDTH)) % uint32(VOL_HASH_TABLE_SIZE)

	fmt.Println(a)
}

// 根据key选择dir
func (cu *CacheURL) KeyToDir(key []byte) {
	OPEN_DIR_BUCKETS := 256
	a := binary.LittleEndian.Uint32(key[0:4]) % uint32(OPEN_DIR_BUCKETS)
	fmt.Println(a)
}

//
