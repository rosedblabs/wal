package wal

import (
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

func TestWAL_Write(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write1")
	opts := Options{
		DirPath:     dir,
		SegmentSize: 32 * 1024 * 1024,
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
		DirPath:     dir,
		SegmentSize: 32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 100000, 512)
}

func TestWAL_Write_large2(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-write3")
	opts := Options{
		DirPath:     dir,
		SegmentSize: 32 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	defer destroyWAL(wal)

	testWriteAndIterate(t, wal, 2000, 32*1024*3+10)
}

func TestWAL_OpenNewActiveSegment(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-new-active-segment")
	opts := Options{
		DirPath:     dir,
		SegmentSize: 32 * 1024 * 1024,
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

func testWriteAndIterate(t *testing.T, wal *WAL, size int, valueSize int) {
	val := strings.Repeat("wal", valueSize)
	totalWrite := 0.0
	positions := make([]*ChunkPosition, size)
	for i := 0; i < size; i++ {
		pos, err := wal.Write([]byte(val))
		totalWrite += float64(len(val))
		assert.Nil(t, err)
		positions[i] = pos
	}
	t.Logf("total write: %.4fMB\n", totalWrite/1024/1024)

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
