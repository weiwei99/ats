/*
Initialization starts with an instance of Store reading the storage configuration file, by default storage.config.
For each valid element in the file an instance of Span is created. These are of basically four types:

* File
* Directory
* Disk
* Raw device

After creating all the Span instances, they are grouped by device ID to internal linked lists attached to
the Store::disk array[#store-disk-array]_. Spans that refer to the same directory, disk, or raw device are coalesced
in to a single span. Spans that refer to the same file with overlapping offsets are also coalesced [5]. This is all
done in ink_cache_init() called during startup.

*/
package cache

//
type Span struct {
	Blocks          int64    `json:"blocks"`
	Offset          int64    `json:"offset"`
	HWSectorSize    uint32   `json:"hw_sector_size"`
	Alignment       uint32   `json:"alignment"`
	Path            string   `json:"path"`
	DiskId          [16]byte //
	ForcedVolumeNum int      `json:"forced_volume_num"`
}

func NewSpan(path string) (*Span, error) {
	sp := &Span{
		ForcedVolumeNum: -1,
		HWSectorSize:    256,
		Path:            path,
	}
	// 填充BLOCKS等信息
	return sp, nil
}

func (span *Span) TotalBlocks() int {

	return 0
}
