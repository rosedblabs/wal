# wal
Write Ahead Log for LSM or bitcask storage.

## Key Features
* Disk based, support large data volume
* Append only write, high performance
* Fast read, one disk seek to retrieve any value
* Support batch write, all data in a batch will be written in a single disk seek
* Iterate all data in wal with `NewReader` function
* Extremely fast read
* Support concurrent write and read, all functions are thread safe

## Design Overview

![wal-logo.png](https://s2.loli.net/2024/05/19/Iz4JML2SokFdrfN.png)

## Format

**Format of a single segment file:**

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |      r1     |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       |<---- BlockSize ----->|<---- BlockSize ----->|

  rn = variable size records
  P = Padding
  BlockSize = 32KB
```

**Format of a single record:**

```
+----------+-------------+-----------+--- ... ---+
| CRC (4B) | Length (2B) | Type (1B) |  Payload  |
+----------+-------------+-----------+--- ... ---+

CRC = 32-bit hash computed over the payload using CRC
Length = Length of the payload data
Type = Type of record
       (FullType, FirstType, MiddleType, LastType)
       The type is used to group a bunch of records together to represent
       blocks that are larger than BlockSize
Payload = Byte stream as long as specified by the payload size
```

## Getting Started

```go
func main() {
	wal, _ := wal.Open()
	// write some data
	chunkPosition, _ := wal.Write([]byte("some data 1"))
	// read by the position
	val, _ := wal.Read(chunkPosition)
	fmt.Println(string(val))

	wal.Write([]byte("some data 2"))
	wal.Write([]byte("some data 3"))

	// iterate all data in wal
	reader := wal.NewReader()
	for {
		val, pos, err := reader.Next()
		if err == io.EOF {
			break
		}
		fmt.Println(string(val))
		fmt.Println(pos) // get position of the data for next read
	}
}

```
