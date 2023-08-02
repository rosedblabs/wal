package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

const (
	initialSegmentFileID = 1
)

var (
	ErrValueTooLarge = errors.New("the data size can't larger than segment size")
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

// Open opens a WAL with the given options.
// It will create the directory if not exists, and open all segment files in the directory.
// If there is no segment file in the directory, it will create a new one.
func Open(options Options) (*WAL, error) {
	if !strings.HasPrefix(options.SegmentFileExt, ".") {
		return nil, fmt.Errorf("segment file extension must start with '.'")
	}
	if options.BlockCache > uint32(options.SegmentSize) {
		return nil, fmt.Errorf("BlockCache must be smaller than SegmentSize")
	}
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
	var segmentIDs []int
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		var id int
		_, err := fmt.Sscanf(entry.Name(), "%d"+options.SegmentFileExt, &id)
		if err != nil {
			continue
		}
		segmentIDs = append(segmentIDs, id)
	}

	// empty directory, just initialize a new segment file.
	if len(segmentIDs) == 0 {
		segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
			initialSegmentFileID, wal.blockCache)
		if err != nil {
			return nil, err
		}
		wal.activeSegment = segment
	} else {
		// open the segment files in order, get the max one as the active segment file.
		sort.Ints(segmentIDs)

		for i, segId := range segmentIDs {
			segment, err := openSegmentFile(options.DirPath, options.SegmentFileExt,
				uint32(segId), wal.blockCache)
			if err != nil {
				return nil, err
			}
			if i == len(segmentIDs)-1 {
				wal.activeSegment = segment
			} else {
				wal.olderSegments[segment.id] = segment
			}
		}
	}

	return wal, nil
}

// SegmentFileName returns the file name of a segment file.
func SegmentFileName(dirPath string, extName string, id SegmentID) string {
	return filepath.Join(dirPath, fmt.Sprintf("%09d"+extName, id))
}

// OpenNewActiveSegment opens a new segment file
// and sets it as the active segment file.
// It is used when even the active segment file is not full,
// but the user wants to create a new segment file.
//
// It is now used by Merge operation of rosedb, not a common usage for most users.
func (wal *WAL) OpenNewActiveSegment() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	// sync the active segment file.
	if err := wal.activeSegment.Sync(); err != nil {
		return err
	}
	// create a new segment file and set it as the active one.
	segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
		wal.activeSegment.id+1, wal.blockCache)
	if err != nil {
		return err
	}
	wal.olderSegments[wal.activeSegment.id] = wal.activeSegment
	wal.activeSegment = segment
	return nil
}

// ActiveSegmentID returns the id of the active segment file.
func (wal *WAL) ActiveSegmentID() SegmentID {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return wal.activeSegment.id
}

// IsEmpty returns whether the WAL is empty.
// Only there is only one active segment file and it is empty,
// the WAL is empty.
func (wal *WAL) IsEmpty() bool {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	return len(wal.olderSegments) == 0 && wal.activeSegment.Size() == 0
}

// NewReaderWithMax returns a new reader for the WAL,
// and the reader will only read the data from the segment file
// whose id is less than or equal to the given segId.
//
// It is now used by the Merge operation of rosedb, not a common usage for most users.
func (wal *WAL) NewReaderWithMax(segId SegmentID) *Reader {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	// get all segment readers.
	var segmentReaders []*segmentReader
	for _, segment := range wal.olderSegments {
		if segId == 0 || segment.id <= segId {
			reader := segment.NewReader()
			segmentReaders = append(segmentReaders, reader)
		}
	}
	if segId == 0 || wal.activeSegment.id <= segId {
		reader := wal.activeSegment.NewReader()
		segmentReaders = append(segmentReaders, reader)
	}

	// sort the segment readers by segment id.
	sort.Slice(segmentReaders, func(i, j int) bool {
		return segmentReaders[i].segment.id < segmentReaders[j].segment.id
	})

	return &Reader{
		segmentReaders: segmentReaders,
		currentReader:  0,
	}
}

// NewReaderWithStart returns a new reader for the WAL,
// and the reader will only read the data from the segment file
// whose position is greater than or equal to the given position.
func (wal *WAL) NewReaderWithStart(startPos *ChunkPosition) (*Reader, error) {
	if startPos == nil {
		return nil, errors.New("start position is nil")
	}
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	reader := wal.NewReader()
	for {
		// skip the segment readers whose id is less than the given position's segment id.
		if reader.CurrentSegmentId() < startPos.SegmentId {
			reader.SkipCurrentSegment()
			continue
		}
		// skip the chunk whose position is less than the given position.
		currentPos := reader.CurrentChunkPosition()
		if currentPos.BlockNumber >= startPos.BlockNumber &&
			currentPos.ChunkOffset >= startPos.ChunkOffset {
			break
		}
		// call Next to find again.
		if _, _, err := reader.Next(); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return reader, nil
}

// NewReader returns a new reader for the WAL.
// It will iterate all segment files and read all data from them.
func (wal *WAL) NewReader() *Reader {
	return wal.NewReaderWithMax(0)
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

// SkipCurrentSegment skips the current segment file
// when reading the WAL.
//
// It is now used by the Merge operation of rosedb, not a common usage for most users.
func (r *Reader) SkipCurrentSegment() {
	r.currentReader++
}

// CurrentSegmentId returns the id of the current segment file
// when reading the WAL.
func (r *Reader) CurrentSegmentId() SegmentID {
	return r.segmentReaders[r.currentReader].segment.id
}

// CurrentChunkPosition returns the position of the current chunk data
func (r *Reader) CurrentChunkPosition() *ChunkPosition {
	reader := r.segmentReaders[r.currentReader]
	return &ChunkPosition{
		SegmentId:   reader.segment.id,
		BlockNumber: reader.blockNumber,
		ChunkOffset: reader.chunkOffset,
	}
}

// Write writes the data to the WAL.
// Actually, it writes the data to the active segment file.
// It returns the position of the data in the WAL, and an error if any.
func (wal *WAL) Write(data []byte) (*ChunkPosition, error) {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	if int64(len(data))+chunkHeaderSize > wal.options.SegmentSize {
		return nil, ErrValueTooLarge
	}
	// if the active segment file is full, sync it and create a new one.
	if wal.isFull(int64(len(data))) {
		if err := wal.activeSegment.Sync(); err != nil {
			return nil, err
		}
		wal.bytesWrite = 0
		segment, err := openSegmentFile(wal.options.DirPath, wal.options.SegmentFileExt,
			wal.activeSegment.id+1, wal.blockCache)
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

// Read reads the data from the WAL according to the given position.
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
		return nil, fmt.Errorf("segment file %d%s not found", pos.SegmentId, wal.options.SegmentFileExt)
	}

	// read the data from the segment file.
	return segment.Read(pos.BlockNumber, pos.ChunkOffset)
}

// Close closes the WAL.
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

// Delete deletes all segment files of the WAL.
func (wal *WAL) Delete() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	// purge the block cache.
	if wal.blockCache != nil {
		wal.blockCache.Purge()
	}

	// delete all segment files.
	for _, segment := range wal.olderSegments {
		if err := segment.Remove(); err != nil {
			return err
		}
	}
	wal.olderSegments = nil

	// delete the active segment file.
	return wal.activeSegment.Remove()
}

// Sync syncs the active segment file to stable storage like disk.
func (wal *WAL) Sync() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	return wal.activeSegment.Sync()
}

func (wal *WAL) isFull(delta int64) bool {
	return wal.activeSegment.Size()+delta+chunkHeaderSize > wal.options.SegmentSize
}
