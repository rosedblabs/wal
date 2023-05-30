package wal

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Write_FULL(t *testing.T) {
	// dir, _ := os.MkdirTemp("", "seg-test")
	dir := "/tmp"
	seg, err := openSegmentFile(dir, 1)
	assert.Nil(t, err)
	// defer seg.Remove()

	// val := []byte(strings.Repeat("X", 100))

	// pos, err := seg.Write(val)
	// t.Log(pos, err)
	// pos, err = seg.Write(val)
	// t.Log(pos, err)
	// pos, err = seg.Write(val)
	// t.Log(pos, err)

	res, err := seg.Read(0, 0)
	t.Log(string(res), err)
	// res, err = seg.Read(0, 107)
	// t.Log(string(res), err)

	// val2 := []byte(strings.Repeat("X", 512))
	// for i := 0; i < 100; i++ {
	// 	pos, err = seg.Write(val2)
	// 	t.Log(pos, err)
	// }
	res, err = seg.Read(1, 26222)
	t.Log(len(res), err)
	t.Log(string(res), err)
}

func TestSegment_Write_NOT_FULL(t *testing.T) {
	// dir, _ := os.MkdirTemp("", "seg-test")
	dir := "/tmp"
	seg, err := openSegmentFile(dir, 2)
	assert.Nil(t, err)
	// defer seg.Remove()

	val1 := []byte(strings.Repeat("X", 512))
	for i := 0; i < 100; i++ {
		pos, err := seg.Write(val1)
		t.Log(pos, err)
	}

	// val2 := []byte(strings.Repeat("X", 32*1024*5))
	// pos, err := seg.Write(val2)
	// t.Log(pos, err)

	res, err := seg.Read(1, 19139)
	t.Log(len(res), err)
}
