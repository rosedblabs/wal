package wal

import (
	"os"
	"testing"

	lru "github.com/hashicorp/golang-lru/v2"
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

func TestLRU(t *testing.T) {
	l, err := lru.New[uint64, string](10)
	assert.Nil(t, err)

	l.Add(12, "a")
	l.Add(11, "b")
	l.Add(43, "c")
	l.Add(12, "d")

	val, ok := l.Get(12)
	t.Log(val, ok)
}
