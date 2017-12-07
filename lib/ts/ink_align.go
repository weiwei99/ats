package ts

func InkAlign(size, boundary int64) int64 {
	e := size % boundary
	if e == 0 {
		return size
	} else {
		return size + boundary - e
	}
}
