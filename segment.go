package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

type ChunkType = byte
type SegmentID = uint32

const (
	ChunkTypeFull ChunkType = iota
	ChunkTypeFirst
	ChunkTypeMiddle
	ChunkTypeLast
)

var (
	ErrClosed     = errors.New("the segment file is closed")
	ErrInvalidCRC = errors.New("invalid crc, the data may be corrupted")
)

const (
	// 7 Bytes
	// Checksum Type Length
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * 1024

	fileModePerm = 0644

	segmentFileSuffix = ".SEG"
)

// Segment represents a single segment file in WAL.
// The segment file is append-only, and the data is written in blocks.
// Each block is 32KB, and the data is written in chunks.
type segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
}

// ChunkPosition represents the position of a chunk in a segment file.
// Used to read the data from the segment file.
type ChunkPosition struct {
	SegmentId   SegmentID
	BlockNumber uint32
	ChunkOffset int64
}

// openSegmentFile a new segment file.
func openSegmentFile(dirPath string, id uint32) (*segment, error) {
	fileName := fmt.Sprintf("%09d"+segmentFileSuffix, id)
	fd, err := os.OpenFile(
		filepath.Join(dirPath, fileName),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}

	// set the current block number and block size.
	offset, err := fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", id, segmentFileSuffix, err))
	}

	return &segment{
		id:                 id,
		fd:                 fd,
		currentBlockNumber: uint32(offset / blockSize),
		currentBlockSize:   uint32(offset % blockSize),
	}, nil
}

func (seg *segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

func (seg *segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}

	return os.Remove(seg.fd.Name())
}

func (seg *segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.fd.Close()
}

func (seg *segment) Size() int64 {
	return int64(seg.currentBlockNumber*blockSize + seg.currentBlockSize)
}

func (seg *segment) Write(data []byte) (*ChunkPosition, error) {
	if seg.closed {
		return nil, ErrClosed
	}

	// The left block space is not enough for a chunk header
	if seg.currentBlockSize+chunkHeaderSize >= blockSize {
		// padding if necessary
		if seg.currentBlockSize < blockSize {
			padding := make([]byte, blockSize-seg.currentBlockSize)
			if _, err := seg.fd.Write(padding); err != nil {
				return nil, err
			}
		}

		// A new block, clear the current block size.
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}

	// the start position(for read operation)
	position := &ChunkPosition{
		SegmentId:   seg.id,
		BlockNumber: seg.currentBlockNumber,
		ChunkOffset: int64(seg.currentBlockSize),
	}
	dataSize := uint32(len(data))
	// The entire chunk can fit into the block.
	if seg.currentBlockSize+dataSize+chunkHeaderSize <= blockSize {
		err := seg.writeInternal(data, ChunkTypeFull)
		if err != nil {
			return nil, err
		}
		return position, nil
	}

	// If the size of the data exceeds the size of the block,
	// the data should be written to the block in batches.
	var leftSize = dataSize
	for leftSize > 0 {
		chunkSize := blockSize - seg.currentBlockSize - chunkHeaderSize
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
			err = seg.writeInternal(chunk, ChunkTypeFirst)
		} else if leftSize == chunkSize {
			// Last Chunk
			err = seg.writeInternal(chunk, ChunkTypeLast)
		} else {
			// Middle Chunk
			err = seg.writeInternal(chunk, ChunkTypeMiddle)
		}
		if err != nil {
			return nil, err
		}
		leftSize -= chunkSize
	}

	return position, nil
}

func (seg *segment) writeInternal(data []byte, chunkType ChunkType) error {
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
	if _, err := seg.fd.Write(buf); err != nil {
		return err
	}

	if seg.currentBlockSize > blockSize {
		panic("wrong! can not exceed the block size")
	}

	// update the corresponding fields
	seg.currentBlockSize += dataSize + chunkHeaderSize
	// A new block
	if seg.currentBlockSize == blockSize {
		seg.currentBlockNumber += 1
		seg.currentBlockSize = 0
	}

	return nil
}

func (seg *segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
	if seg.closed {
		return nil, ErrClosed
	}

	segSize, err := seg.fd.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	var result []byte
	for {
		size := int64(blockSize)
		offset := int64(blockNumber * blockSize)
		if size+offset > segSize {
			size = segSize - offset
		}
		buf := make([]byte, size)
		_, err := seg.fd.ReadAt(buf, offset)
		if err != nil {
			return nil, err
		}

		// header part
		header := make([]byte, chunkHeaderSize)
		copy(header, buf[chunkOffset:chunkOffset+chunkHeaderSize])

		// length
		legnth := binary.LittleEndian.Uint16(header[4:6])

		// copy data
		start := chunkOffset + chunkHeaderSize
		result = append(result, buf[start:start+int64(legnth)]...)

		// check sum
		checkSumEnd := chunkOffset + chunkHeaderSize + int64(legnth)
		checksum := crc32.ChecksumIEEE(buf[chunkOffset+4 : checkSumEnd])
		savedSum := binary.LittleEndian.Uint32(header[:4])
		if savedSum != checksum {
			return nil, ErrInvalidCRC
		}

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