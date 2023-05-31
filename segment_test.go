package wal

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSegment_Write_FULL1(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-full1")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 1. FULL chunks
	val := []byte(strings.Repeat("X", 100))

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	pos2, err := seg.Write(val)
	assert.Nil(t, err)

	val1, err := seg.Read(pos1.BlockNumber, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	val2, err := seg.Read(pos2.BlockNumber, pos2.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val2)

	// 2. Write until a new block
	for i := 0; i < 100000; i++ {
		pos, err := seg.Write(val)
		assert.Nil(t, err)
		val, err := seg.Read(pos.BlockNumber, pos.ChunkOffset)
		assert.Nil(t, err)
		assert.Equal(t, val, val)
	}
}

func TestSegment_Write_FULL2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-full2")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 3. chunk full with a block
	val := []byte(strings.Repeat("X", blockSize-chunkHeaderSize))

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos1.BlockNumber, uint32(0))
	assert.Equal(t, pos1.ChunkOffset, int64(0))
	val1, err := seg.Read(pos1.BlockNumber, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)

	pos2, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos2.BlockNumber, uint32(1))
	assert.Equal(t, pos2.ChunkOffset, int64(0))
	val2, err := seg.Read(pos2.BlockNumber, pos2.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val2)
}

func TestSegment_Write_Padding(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-padding")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 4. padding
	val := []byte(strings.Repeat("X", blockSize-chunkHeaderSize-3))

	_, err = seg.Write(val)
	assert.Nil(t, err)

	pos1, err := seg.Write(val)
	assert.Nil(t, err)
	assert.Equal(t, pos1.BlockNumber, uint32(1))
	assert.Equal(t, pos1.ChunkOffset, int64(0))
	val1, err := seg.Read(pos1.BlockNumber, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, val, val1)
}

func TestSegment_Write_NOT_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-not-full")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// 5. FIRST-LAST
	bytes1 := []byte(strings.Repeat("X", blockSize+100))

	pos1, err := seg.Write(bytes1)
	assert.Nil(t, err)
	val1, err := seg.Read(pos1.BlockNumber, pos1.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val1)

	pos2, err := seg.Write(bytes1)
	assert.Nil(t, err)
	val2, err := seg.Read(pos2.BlockNumber, pos2.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val2)

	pos3, err := seg.Write(bytes1)
	assert.Nil(t, err)
	val3, err := seg.Read(pos3.BlockNumber, pos3.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val3)

	// 6. FIRST-MIDDLE-LAST
	bytes2 := []byte(strings.Repeat("X", blockSize*3+100))
	pos4, err := seg.Write(bytes2)
	assert.Nil(t, err)
	val4, err := seg.Read(pos4.BlockNumber, pos4.ChunkOffset)
	assert.Nil(t, err)
	assert.Equal(t, bytes2, val4)
}

func TestSegment_Reader_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-full")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	// FULL chunks
	bytes1 := []byte(strings.Repeat("X", blockSize+100))
	seg.Write(bytes1)
	seg.Write(bytes1)

	reader := seg.NewReader()
	val, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, err = reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, err = reader.Next()
	assert.Nil(t, val)
	assert.Equal(t, err, io.EOF)
}

func TestSegment_Reader_Padding(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-padding")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	bytes1 := []byte(strings.Repeat("X", blockSize-chunkHeaderSize-7))

	_, err = seg.Write(bytes1)
	assert.Nil(t, err)
	_, err = seg.Write(bytes1)
	assert.Nil(t, err)

	reader := seg.NewReader()
	val, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, err = reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	_, err = reader.Next()
	assert.Equal(t, err, io.EOF)
}

func TestSegment_Reader_NOT_FULL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "seg-test-reader-not-full")
	seg, err := openSegmentFile(dir, 1, nil)
	assert.Nil(t, err)
	defer func() {
		_ = seg.Remove()
	}()

	bytes1 := []byte(strings.Repeat("X", blockSize+100))
	seg.Write(bytes1)
	seg.Write(bytes1)

	bytes2 := []byte(strings.Repeat("X", blockSize*3+10))
	seg.Write(bytes2)
	seg.Write(bytes2)

	reader := seg.NewReader()
	val, err := reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, err = reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes1, val)

	val, err = reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes2, val)

	val, err = reader.Next()
	assert.Nil(t, err)
	assert.Equal(t, bytes2, val)

	_, err = reader.Next()
	assert.Equal(t, err, io.EOF)
}
