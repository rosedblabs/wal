package wal

import (
	"os"
	"testing"
	"github.com/stretchr/testify/assert"
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
