package diskparser

type VersionNumber struct {
	InkMajor int16 `json:"ink_major"`
	InkMinor int16 `json:"ink_minor"`
}

func RoundToStoreBlock(size int) int {
	return Align(size, STORE_BLOCK_SIZE)
}

func Align(size, boundary int) int {
	return ((size) + ((boundary) - 1)) & ^((boundary) - 1)
}

func AtsPagesize() int {
	return 8019
}

func AtsAlign() {

}
