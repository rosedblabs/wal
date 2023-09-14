package benchmark

import (
	"math/rand"
	"os"
	"strings"
	"testing"

	"github.com/rosedblabs/wal"
	"github.com/stretchr/testify/assert"
)

var walFile *wal.WAL

func init() {
	dir, _ := os.MkdirTemp("", "wal-benchmark-test")
	opts := wal.Options{
		DirPath:        dir,
		SegmentFileExt: ".SEG",
		SegmentSize:    32 * 1024 * 1024,
	}
	var err error
	walFile, err = wal.Open(opts)
	if err != nil {
		panic(err)
	}
}

func BenchmarkWAL_WriteLargeSize(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	content := []byte(strings.Repeat("X", 256*wal.KB+500))
	for i := 0; i < b.N; i++ {
		_, err := walFile.Write(content)
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_Write(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
	}
}

func BenchmarkWAL_WriteBatch(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 31; j++ {
			err := walFile.PendingWrites([]byte(strings.Repeat("X", wal.MB)))
			assert.Nil(b, err)
		}
		err := walFile.PendingWrites([]byte(strings.Repeat("X", wal.MB)))
		assert.Equal(b, wal.ErrPendingSizeTooLarge, err)
		pos, err := walFile.WriteAll()
		assert.Nil(b, err)
		assert.Equal(b, 0, len(pos))
	}
}

func BenchmarkWAL_Read(b *testing.B) {
	var positions []*wal.ChunkPosition
	for i := 0; i < 1000000; i++ {
		pos, err := walFile.Write([]byte("Hello World"))
		assert.Nil(b, err)
		positions = append(positions, pos)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := walFile.Read(positions[rand.Intn(len(positions))])
		assert.Nil(b, err)
	}
}
