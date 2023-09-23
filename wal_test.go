package wal

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func destroyWAL(wal *WAL) {
	if wal != nil {
		_ = wal.Close()
		_ = os.RemoveAll(wal.options.DirPath)
	}
}

func TestWAL_WriteALL(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write-batch-1")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAllIterate(t, wal, 0, 10)
	assert.True(t, wal.IsEmpty())

	testWriteAllIterate(t, wal, 10000, 512)
	assert.False(t, wal.IsEmpty())
}

func TestWAL_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write1")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	// write 1
	pos1, err := wal.Write([]byte("hello1"))
	assert.Nil(t, err)
	assert.NotNil(t, pos1)
	pos2, err := wal.Write([]byte("hello2"))
	assert.Nil(t, err)
	assert.NotNil(t, pos2)
	pos3, err := wal.Write([]byte("hello3"))
	assert.Nil(t, err)
	assert.NotNil(t, pos3)

	val, err := wal.Read(pos1)
	assert.Nil(t, err)
	assert.Equal(t, "hello1", string(val))
	val, err = wal.Read(pos2)
	assert.Nil(t, err)
	assert.Equal(t, "hello2", string(val))
	val, err = wal.Read(pos3)
	assert.Nil(t, err)
	assert.Equal(t, "hello3", string(val))
}

func TestWAL_Write_large(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write2")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_Write_large2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write3")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 32*1024*3+10)
}

func TestWAL_OpenNewActiveSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-new-active-segment")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 512)
	err = wal.OpenNewActiveSegment()
	assert.Nil(t, err)

	val := strings.Repeat("wal", 100)
	for i := 0; i < 100; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		assert.NotNil(t, pos)
	}
}

func TestWAL_IsEmpty(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-is-empty")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	assert.True(t, wal.IsEmpty())
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.IsEmpty())
}

func TestWAL_Reader(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	var size = 100000
	val := strings.Repeat("wal", 512)
	for i := 0; i < size; i++ {
		_, err := wal.Write([]byte(val))
		assert.Nil(t, err)
	}

	validate := func(walInner *WAL, size int) {
		var i = 0
		reader := walInner.NewReader()
		for {
			chunk, position, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			assert.NotNil(t, chunk)
			assert.NotNil(t, position)
			assert.Equal(t, position.SegmentId, reader.CurrentSegmentId())
			i++
		}
		assert.Equal(t, i, size)
	}

	validate(wal, size)
	err = wal.Close()
	assert.Nil(t, err)

	wal2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	validate(wal2, size)
}

func testWriteAllIterate(t *testing.T, wal *WAL, size, valueSize int) {
	for i := 0; i < size; i++ {
		val := strings.Repeat("wal", valueSize)
		wal.PendingWrites([]byte(val))
	}
	positions, err := wal.WriteAll()
	assert.Nil(t, err)
	assert.Equal(t, len(positions), size)

	count := 0
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, strings.Repeat("wal", valueSize), string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	assert.Equal(t, len(wal.pendingWrites), 0)
}

func testWriteAndIterate(t *testing.T, wal *WAL, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		assert.Nil(t, err)
		positions[i] = pos
	}

	var count int
	// iterates all the data
	reader := wal.NewReader()
	for {
		data, pos, err := reader.Next()
		if err != nil {
			break
		}
		assert.Equal(t, val, string(data))

		assert.Equal(t, positions[count].SegmentId, pos.SegmentId)
		assert.Equal(t, positions[count].BlockNumber, pos.BlockNumber)
		assert.Equal(t, positions[count].ChunkOffset, pos.ChunkOffset)

		count++
	}
	assert.Equal(t, size, count)
}

func TestWAL_Delete(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-delete")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	testWriteAndIterate(t, wal, 2000, 512)
	assert.False(t, wal.IsEmpty())
	defer destroyWAL(wal)

	err = wal.Delete()
	assert.Nil(t, err)

	wal, err = Open(opts)
	assert.Nil(t, err)
	assert.True(t, wal.IsEmpty())
}

func TestWAL_ReaderWithStart(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-wal-reader-with-start")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    8 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	_, err = wal.NewReaderWithStart(nil)
	assert.NotNil(t, err)

	reader1, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 100})
	assert.Nil(t, err)
	_, _, err = reader1.Next()
	assert.Equal(t, err, io.EOF)

	testWriteAndIterate(t, wal, 20000, 512)
	reader2, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 0, BlockNumber: 0, ChunkOffset: 0})
	assert.Nil(t, err)
	_, pos2, err := reader2.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos2.BlockNumber, uint32(0))
	assert.Equal(t, pos2.ChunkOffset, int64(0))

	reader3, err := wal.NewReaderWithStart(&ChunkPosition{SegmentId: 3, BlockNumber: 5, ChunkOffset: 0})
	assert.Nil(t, err)
	_, pos3, err := reader3.Next()
	assert.Nil(t, err)
	assert.Equal(t, pos3.SegmentId, uint32(3))
	assert.Equal(t, pos3.BlockNumber, uint32(5))
}

func TestWAL_RenameFileExt(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-rename-ext")
	opts := Options{
		DirPath:        dir,
		SegmentFileExt: ".VLOG.1.temp",
		SegmentSize:    8 * 1024 * 1024,
		BlockCache:     32 * KB * 10,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)
	testWriteAndIterate(t, wal, 20000, 512)

	err = wal.Close()
	assert.Nil(t, err)

	err = wal.RenameFileExt(".VLOG.1")
	assert.Nil(t, err)

	opts.SegmentFileExt = ".VLOG.1"
	wal2, err := Open(opts)
	assert.Nil(t, err)
	defer func() {
		_ = wal2.Close()
	}()
	for i := 0; i < 20000; i++ {
		_, err = wal2.Write([]byte(strings.Repeat("W", 512)))
		assert.Nil(t, err)
	}
}
