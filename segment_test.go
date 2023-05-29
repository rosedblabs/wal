package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Write_FULL(t *testing.T) {
	// dir, _ := os.MkdirTemp("", "seg-test")
	dir := "/tmp"
	seg, err := openSegmentFile(dir, 1)
	assert.Nil(t, err)
	defer seg.Remove()

	seg.Write([]byte("hello world1"))
	seg.Write([]byte("hello world2"))
	seg.Write([]byte("hello world3"))
	seg.Write([]byte("hello world4"))
}
