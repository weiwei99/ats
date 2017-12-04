package diskparser

type CacheProcesser struct {
	GVol []*Vol
}

func (cp *CacheProcesser) DirCheck(afix bool) int {
	for _, v := range cp.GVol {
		v.DirCheck(afix)
	}
	return 0
}
