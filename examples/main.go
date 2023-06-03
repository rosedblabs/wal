package main

import (
	"fmt"
	"io"
	"github.com/rosedblabs/wal"
)

func main() {
	wal, _ := wal.Open(wal.DefaultOptions)
	// write some data
	chunkPosition, _ := wal.Write([]byte("some data 1"))
	// read by the posistion
	val, _ := wal.Read(chunkPosition)
	fmt.Println(string(val))

	wal.Write([]byte("some data 2"))
	wal.Write([]byte("some data 3"))

	// iterate all data in wal
	reader := wal.NewReader()
	for {
		val, err := reader.Next()
		if err == io.EOF {
			break
		}
		fmt.Println(string(val))
	}
}
