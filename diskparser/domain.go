package diskparser

// 单域名信息
type DomainInfo struct {
	Domain          string        `json:"domain"`
	StatObjectCount uint64        `json:"stat_object_count"` // URL数量
	StatStatusMap   map[int]int64 `json:"stat_status_map"`   // 状态码分布
	StatMethodMap   map[int]int64 `json:"stat_method_map"`   // 方法分布
}

// 域名统计
type DomainsStat struct {
	DomainInfos map[string]DomainInfo `json:"domain_infos"`
}

func NewDomainStat() *DomainsStat {
	ds := &DomainsStat{}
	ds.DomainInfos = make(map[string]DomainInfo, 0)
	return ds
}

func (ds *DomainsStat) AddObject() {

}
