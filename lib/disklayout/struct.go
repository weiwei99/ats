package disklayout

import "github.com/weiwei99/ats/lib/cache"

type DataPos struct {
	Offset int
	Size   int
}
type DLVol struct {
	*cache.Vol
	*DataPos
}

type DLDiskHeader struct {
	*cache.DiskHeader
	*DataPos
}

type DLVolHeaderFooter struct {
	*cache.VolHeaderFooter
	*DataPos
}
