package main

import (
	"path/filepath"
	"wal"
)

func main() {
	wal, _ := wal.Open(filepath.Join("/tmp", "00001.log"))
	pos, _ := wal.Write([]byte("wal log entry")) // get the position of the record

	res, _ := wal.Read(pos.BlockNumber, pos.ChunkOffset) // read the specified record
	println(res)
}
