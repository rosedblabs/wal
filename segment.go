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
	ErrClosed = errors.New("the segment file is closed")
)

const (
	// 7 Bytes
	// Checksum Type Length
	//    4      2     1
	chunkHeaderSize = 7

	// 32 KB
	blockSize = 32 * 1024

	fileModePerm = 0644

	segmentFileSuffix = ".seg"
)

type Segment struct {
	id                 SegmentID
	fd                 *os.File
	currentBlockNumber uint32
	currentBlockSize   uint32
	closed             bool
}

type ChunkPosition struct {
	SegmentId   SegmentID
	BlockNumber uint32
	ChunkOffset int64
}

// Open a new segment file.
func OpenSegmentFile(dirPath string, id uint32) (*Segment, error) {
	fileName := fmt.Sprintf("%09d"+segmentFileSuffix, id)
	fd, err := os.OpenFile(
		filepath.Join(dirPath, fileName),
		os.O_CREATE|os.O_RDWR|os.O_APPEND,
		fileModePerm,
	)

	if err != nil {
		return nil, err
	}
	return &Segment{
		id:                 id,
		fd:                 fd,
		currentBlockNumber: 0,
		currentBlockSize:   0,
	}, nil
}

func (seg *Segment) Sync() error {
	if seg.closed {
		return nil
	}
	return seg.fd.Sync()
}

func (seg *Segment) Remove() error {
	if !seg.closed {
		seg.closed = true
		_ = seg.fd.Close()
	}

	return os.Remove(seg.fd.Name())
}

func (seg *Segment) Close() error {
	if seg.closed {
		return nil
	}

	seg.closed = true
	return seg.fd.Close()
}

func (seg *Segment) Size() int64 {
	return int64(seg.currentBlockNumber*blockSize + seg.currentBlockSize)
}

func (seg *Segment) Write(data []byte) (*ChunkPosition, error) {
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

func (seg *Segment) writeInternal(data []byte, chunkType ChunkType) error {
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

func (seg *Segment) Read(blockNumber uint32, chunkOffset int64) ([]byte, error) {
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
