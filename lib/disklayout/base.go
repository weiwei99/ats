package disklayout

const (
	ZYDiskHeaderOffset = 0x2000 // 8192
	ZYVolOffset        = 0xfe00 // 65024
	//ZYDocOffset = 0x

	DISK_HEADER_MAGIC uint32 = 0xABCD1237 // 出现在 0002000， 0009

	STORE_BLOCK_SIZE       = 8192
	STORE_BLOCK_SHIFT      = 13
	DEFAULT_HW_SECTOR_SIZE = 512

	CACHE_BLOCK_SHIFT = 9
	CACHE_BLOCK_SIZE  = 1 << CACHE_BLOCK_SHIFT // 512, smallest sector size
	START_BLOCKS      = 16
	START_POS         = START_BLOCKS * CACHE_BLOCK_SIZE

	VOL_BLOCK_SIZE = 1024 * 1024 * 128 // 至少128MB
	MIN_VOL_SIZE   = VOL_BLOCK_SIZE

	PAGE_SIZE = 8192

	DiskHeaderLen = 56 // 56个字节

	LEN_DiskVolBlock       = 21
	LEN_DiskVolBlockAppend = 24 // DiskVolBlock只使用21个字节，但在磁盘中按 24个字节存储
	LEN_DiskHeader         = 6 + LEN_DiskVolBlock
)

func RoundToStoreBlock(size int) int {
	return Align(size, STORE_BLOCK_SIZE)
}

func Align(size, boundary int) int {
	return ((size) + ((boundary) - 1)) & ^((boundary) - 1)
}
