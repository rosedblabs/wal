package wal

import (
	"fmt"
	"os"
	"sort"
	"sync"
)

const (
	initialSegmentFileID = 1
)

type WAL struct {
	activeSegment *Segment
	olderSegments map[SegmentID]*Segment
	mu            sync.RWMutex
}

func Open(options Options) (*WAL, error) {
	wal := &WAL{
		olderSegments: make(map[SegmentID]*Segment),
	}

	// iterate the dir and open all segment files.
	entries, err := os.ReadDir(options.DirPath)
	if err != nil {
		return nil, err
	}

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
		return wal, nil
	}

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
	return wal, nil
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	return nil, nil
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	return nil, nil
}
