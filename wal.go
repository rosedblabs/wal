package wal

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	initialSegmentFileID = 1
)

// WAL represents a Write-Ahead Log structure that provides durability
// and fault-tolerance for incoming writes.
// It consists of an activeSegment, which is the current segment file
// used for new incoming writes, and olderSegments,
// which is a map of segment files used for read operations.
//
// The options field stores various configuration options for the WAL.
//
// The mu sync.RWMutex is used for concurrent access to the WAL data structure,
// ensuring safe access and modification.
//
// The blockCache is an LRU cache used to store recently accessed data blocks,
// improving read performance by reducing disk I/O.
// It is implemented using a lru.Cache structure with keys of type uint64 and values of type []byte.
type WAL struct {
	activeSegment *segment               // active segment file, used for new incoming writes.
	olderSegments map[SegmentID]*segment // older segment files, only used for read.
	options       Options
	mu            sync.RWMutex
	blockCache    *lru.Cache[uint64, []byte]
	bytesWrite    uint32
}

// Reader represents a reader for the WAL.
// It consists of segmentReaders, which is a slice of segmentReader
// structures sorted by segment id,
// and currentReader, which is the index of the current segmentReader in the slice.
//
// The currentReader field is used to iterate over the segmentReaders slice.
type Reader struct {
	segmentReaders []*segmentReader
	currentReader  int
}

func Open(options Options) (*WAL, error) {
	wal := &WAL{
		options:       options,
		olderSegments: make(map[SegmentID]*segment),
	}

	// create the directory if not exists.
	if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
		return nil, err
	}

	// create the block cache if needed.
	if options.BlockCache > 0 {
		var lruSize = options.BlockCache / blockSize
		if options.BlockCache%blockSize != 0 {
			lruSize += 1
		}
		cache, err := lru.New[uint64, []byte](int(lruSize))
		if err != nil {
			return nil, err
		}
		wal.blockCache = cache
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

	// empty directory, just initialize a new segment file.
	if len(segmengIDs) == 0 {
		segment, err := openSegmentFile(options.DirPath, initialSegmentFileID, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		// open the segment files in order, get the max one as the active segment file.
		sort.Ints(segmengIDs)

		for i, segId := range segmengIDs {
			segment, err := openSegmentFile(options.DirPath, uint32(segId), wal.blockCache)
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

	return wal, nil
}

func (wal *WAL) NewReader() *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// get all segment readers.
	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		reader := segment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}
	reader := wal.activeSegment.NewReader()
	segmentReaders = append(segmentReaders, reader)

	// sort the segment readers by segment id.
	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

// Next returns the next chunk data and its position in the WAL.
// If there is no data, io.EOF will be returned.
//
// The position can be used to read the data from the segment file.
func (r *Reader) Next() ([]byte, *ChunkPosition, error) {
	if r.currentReader >= len(r.segmentReaders) {
		return nil, nil, io.EOF
	}

	data, position, err := r.segmentReaders[r.currentReader].Next()
	if err == io.EOF {
		r.currentReader++
		return r.Next()
	}
	return data, position, err
}

func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// if the active segment file is full, sync it and create a new one.
	if wal.isFull(int64(len(data))) {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}

		segment, err := openSegmentFile(wal.options.DirPath, wal.activeSegment.id+1, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
		wal.activeSegment = segment
	}

	// write the data to the active segment file.
	position, err := wal.activeSegment.Write(data)
	if err != nil {
		return nil, err
	}

	// update the bytesWrite field.
	wal.bytesWrite += position.ChunkSize

	// sync the active segment file if needed.
	var needSync = wal.options.Sync
	if !needSync && wal.options.BytesPerSync > 0 {
		needSync = wal.bytesWrite >= wal.options.BytesPerSync
	}
	if needSync {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
	}

	return position, nil
}

func (wal *WAL) Read(pos *ChunkPosition) ([]byte, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// find the segment file according to the position.
	var segment *segment
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

func (wal *WAL) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// purge the block cache.
	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}

	// close all segment files.
	for _, segment := range wal.olderSegments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// close the active segment file.
	return wal.activeSegment.Close()
}

func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+delta+chunkHeaderSize > wal.options.SegmentSize
}
