package wal

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
)

const (
	initialSegmentFileID = 1
)

type WAL struct {
	activeSegment *Segment               // active segment file, used for new incoming writes.
	olderSegments map[SegmentID]*Segment // older segment files, only used for read.
	options       Options
	mu            sync.RWMutex
}

func Open(options Options) (*WAL, error) {
	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*Segment),
	}

	// create the directory if not exists.
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// iterate the dir and open all segment files.
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

	// get all segment file ids.
	var segmengIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+segmentFileSuffix, &id)
		if err != nil {
			continue
		}
		segmengIDs = append(segmengIDs, id)
	}

	// empty directory, just initialize a new segment file and return.
	if len(segmengIDs) == 0 {
		segment, err := OpenSegmentFile(options.DirPath, initialSegmentFileID)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		// open the segment files in order, get the max one as the active segment file.
		sort.Ints(segmengIDs)

		for i, segId := range segmengIDs {
			segment, err := OpenSegmentFile(options.DirPath, uint32(segId))
			if err != nil {
				return nil, err
			}
			if i == len(segmengIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}

	// set the current block number and block size.
	wal.setActiveSegmentState()

	return wal, nil
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// if the active segment file is full, close it and create a new one.
	if wal.isFull(int64(len(data))) {
		err := wal.activeSegment.Close()
		if err != nil {
			return nil, err
		}
		segment, err := OpenSegmentFile(wal.options.DirPath, wal.activeSegment.id+1)
		if err != nil {
			return nil, err
		}
		wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
		wal.activeSegment = segment
	}

	// write the data to the active segment file.
	return wal.activeSegment.Write(data)
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// find the segment file according to the position.
	var segment *Segment
	if pos.SegmentId == wal.activeSegment.id {
		segment = wal.activeSegment
	} else {
		segment = wal.olderSegments[pos.SegmentId]
	}

	if segment == nil {
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, segmentFileSuffix)
	}

	// read the data from the segment file.
	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+delta+chunkHeaderSize > wal.options.SegmentSize
}

func (wal *WAL) setActiveSegmentState() {
	offset, err := wal.activeSegment.fd.Seek(0, io.SeekEnd)
	if err != nil {
		panic(fmt.Errorf("seek to the end of segment file %d%s failed: %v", wal.activeSegment.id, segmentFileSuffix, err))
	}
	wal.activeSegment.currentBlockNumber = uint32(offset / blockSize)
	wal.activeSegment.currentBlockSize = uint32(offset % blockSize)
}
