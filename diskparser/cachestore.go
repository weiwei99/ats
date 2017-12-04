package diskparser

type Store struct {
	NDisk uint32  `json:"n_disk"`
	Disk  []*Span `json:"disk"`
}

type Span struct {
	Blocks          int64    `json:"blocks"`
	Offset          int64    `json:"offset"`
	HWSectorSize    uint32   `json:"hw_sector_size"`
	Alignment       uint32   `json:"alignment"`
	DiskId          [16]byte //
	ForcedVolumeNum int      `json:"forced_volume_num"`
}
