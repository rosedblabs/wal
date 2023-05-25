package wal

type WAL struct {
	activeSegment *Segment
	olderSegments []*Segment
}

func Open(fileName string) (*WAL, error) {
	return nil, nil
}

func (wal *WAL) Write(data []byte) (*ChunkStartPosition, error) {
	return nil, nil
}

func (wal *WAL) Read(pos *ChunkStartPosition) ([]byte, error) {
	return nil, nil
}
