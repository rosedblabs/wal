package wal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestWAL_Open(t *testing.T) {
	dir, _ := os.MkdirTemp("", "wal-test-open")
	opts := Options{
		DirPath:     dir,
		SegmentSize: 1024 * 1024 * 1024,
	}
	wal, err := Open(opts)
	assert.Nil(t, err)
	assert.NotNil(t, wal)
}
