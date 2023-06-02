package wal

import "os"

// Options represents the configuration options for a Write-Ahead Log (WAL).
type Options struct {
	// DirPath specifies the directory path where the WAL segment files will be stored.
	DirPath string

	// SegmentSize specifies the maximum size of each segment file in bytes.
	SegmentSize int64

	// BlockCache specifies the size of the block cache in number of bytes.
	// A block cache is used to store recently accessed data blocks, improving read performance.
	// If BlockCache is set to 0, no block cache will be used.
	BlockCache uint32
}

var DefaultOptions = Options{
	DirPath:     os.TempDir(),
	SegmentSize: 1024 * 1024 * 1024,
	BlockCache:  0,
}
