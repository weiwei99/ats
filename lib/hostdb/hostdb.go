/*
回源缓存
*/
package hostdb

import "github.com/weiwei99/ats/lib/cache"

type Config struct {
	Filename    string `json:"filename"`
	VerifyAfter int    `json:"verify_after"`
	IPResolve   string `json:"ip_resolve"`
	Partitions  int    `json:"partitions"`
}

//
type HostDB struct {
	span   *cache.Span // HostDB也是用span结构体存放
	config *Config
}

func NewHostDB(config *Config) (*HostDB, error) {
	hdb := &HostDB{}
	return hdb, nil
}
