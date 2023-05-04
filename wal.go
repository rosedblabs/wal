package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync"
)

type ChunkType = byte

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

const (
	// 7 Bytes
	// Checksum Type Length
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * 1024

	fileModePerm = 0644
)

var (
	ErrClosed = errors.New("the wal is closed")
)

// WAL Write Ahead Log instance.
type WAL struct {
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	mu                 *sync.RWMutex
	closed             bool
}

type ChunkStartPosition struct {
	BlockNumber uint32
	ChunkOffset int64
}

// Open a new wal.
func Open(fileName string) (*WAL, error) {
	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_APPEND, fileModePerm)
	if err != nil {
		return nil, err
	}
	return &WAL{
		fd:                 fd,
		currentBlockNumber: 0,
		currentBlockSize:   0,
		mu:                 new(sync.RWMutex),
	}, nil
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil
	}
	return wal.fd.Sync()
}

func (wal *WAL) Remove() {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if !wal.closed {
		wal.closed = true
		_ = wal.fd.Close()
	}

	_ = os.Remove(wal.fd.Name())
}

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil
	}

	wal.closed = true
	return wal.fd.Close()
}

func (wal *WAL) Write(data []byte) (*ChunkStartPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	if wal.closed {
		return nil, ErrClosed
	}

	// The left block space is not enough for a chunk header
	if wal.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if wal.currentBlockSize < blockSize {
			padding := make([]byte, blockSize-wal.currentBlockSize)
			if _, err := wal.fd.Write(padding); err != nil {
				return nil, err
			}
		}

		// A new block, clear the current block size.
		wal.currentBlockNumber += 1
		wal.currentBlockSize = 0
	}

	// the start position(for read)
	position := &ChunkStartPosition{
		BlockNumber: wal.currentBlockNumber,
		ChunkOffset: int64(wal.currentBlockSize),
	}
	dataSize := uint32(len(data))
	// The entire chunk can fit into the block.
	if wal.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		err := wal.writeInternal(data, ChunkTypeFull)
		if err != nil {
			return nil, err
		}
		return position, nil
	}

	// If the size of the data exceeds the size of the block,
	// the data should be written to the block in batches.
	var leftSize = dataSize
	for leftSize > 0 {
		chunkSize := blockSize - wal.currentBlockSize - chunkHeaderSize
		if chunkSize > leftSize {
			chunkSize = leftSize
		}
		chunk := make([]byte, chunkSize)

		var end = dataSize - leftSize + chunkSize
		if end > dataSize {
			end = dataSize
		}

		copy(chunk[:], data[dataSize-leftSize:end])

		// write the chunks
		var err error
		if leftSize == dataSize {
			// First Chunk
			err = wal.writeInternal(chunk, ChunkTypeFirst)
		} else if leftSize == chunkSize {
			// Last Chunk
			err = wal.writeInternal(chunk, ChunkTypeLast)
		} else {
			// Middle Chunk
			err = wal.writeInternal(chunk, ChunkTypeMiddle)
		}
		if err != nil {
			return nil, err
		}
		leftSize -= chunkSize
	}

	return position, nil
}

func (wal *WAL) writeInternal(data []byte, chunkType ChunkType) error {
	dataSize := uint32(len(data))
	buf := make([]byte, dataSize+chunkHeaderSize)

	// Length	2 Bytes	index:4-5
	binary.LittleEndian.PutUint16(buf[4:6], uint16(dataSize))
	// Type	1 Byte	index:6
	buf[6] = chunkType
	// data N Bytes index:7-end
	copy(buf[7:], data)
	// Checksum	4 Bytes index:0-3
	sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], sum)

	// append to the file
	if _, err := wal.fd.Write(buf); err != nil {
		return err
	}

	if wal.currentBlockSize > blockSize {
		panic("wrong! can not exceed the block size")
	}

	// update the corresponding fields
	wal.currentBlockSize += dataSize + chunkHeaderSize
	// A new block
	if wal.currentBlockSize == blockSize {
		wal.currentBlockNumber += 1
		wal.currentBlockSize = 0
	}

	return nil
}

func (wal *WAL) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	if wal.closed {
		return nil, ErrClosed
	}

	stat, err := wal.fd.Stat()
	if err != nil {
		return nil, err
	}

	var result []byte
	for {
		size := int64(blockSize)
		offset := int64(blockNumber * blockSize)
		if size+offset > stat.Size() {
			size = stat.Size() - offset
		}
		buf := make([]byte, size)
		_, err := wal.fd.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}

		// header part
		header := make([]byte, chunkHeaderSize)
		copy(header, buf[chunkOffset:chunkOffset+chunkHeaderSize])

		// check sum todo

		// length
		legnth := binary.LittleEndian.Uint16(header[4:6])

		// copy data
		start := chunkOffset + chunkHeaderSize
		result = append(result, buf[start:start+int64(legnth)]...)

		// type
		chunkType := header[6]
		if chunkType == ChunkTypeFull || chunkType == ChunkTypeLast {
			break
		}
		blockNumber += 1
		chunkOffset = 0
	}
	return result, nil
}
