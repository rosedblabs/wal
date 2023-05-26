package wal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWAL_Write(t *testing.T) {
	opts := Options{
		DirPath:     "/tmp/wal",
		SegmentSize: 1024 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)

	pos, err := wal.Write([]byte("amazing roseduan is better"))
	assert.Nil(t, err)
	t.Log(pos)

	res, err := wal.Read(pos)
	t.Log(string(res), err)

	// wal.Write([]byte("hello world1"))
	// wal.Write([]byte("hello world2"))
	// wal.Write([]byte("hello world3"))
}
