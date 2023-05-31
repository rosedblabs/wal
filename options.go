package wal

type Options struct {
	DirPath     string
	SegmentSize int64
	BlockCache  uint32
}

var DefaultOptions = Options{
	DirPath:     "/tmp/wal",
	SegmentSize: 1024 * 1024 * 1024,
	BlockCache:  0,
}
