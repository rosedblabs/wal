package wal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWAL_Write(t *testing.T) {
	file := filepath.Join("/tmp", "000001.log")

	wal, err := Open(file)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = wal.Close()
		_ = os.Remove(file)
	}()

	// one block
	s := strings.Repeat("A", 2028)
	_, err = wal.Write([]byte(s))

	s = strings.Repeat("A", 30*1024)
	_, err = wal.Write([]byte(s))

	s = strings.Repeat("A", 1)
	_, err = wal.Write([]byte(s))

	s = strings.Repeat("A", 33*1024)
	_, err = wal.Write([]byte(s))

	// multi blocks
	s = strings.Repeat("A", 66*1024)
	_, err = wal.Write([]byte(s))

	t.Log(wal.currentBlockSize)
	assert.Nil(t, err)
}

func TestWAL_Read(t *testing.T) {
	file := filepath.Join("/tmp", "000001.log")

	wal, err := Open(file)
	if err != nil {
		panic(err)
	}
	defer func() {
		_ = wal.Close()
		_ = os.Remove(file)
	}()

	// one block
	s := strings.Repeat("A", 2028)
	pos, err := wal.Write([]byte(s))
	wal.Read(pos.BlockNumber, pos.ChunkOffset)

	// multi blocks
	s = strings.Repeat("A", 45*1024)
	pos, err = wal.Write([]byte(s))
	wal.Read(pos.BlockNumber, pos.ChunkOffset)
}
