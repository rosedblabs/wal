# wal
**[WIP] Write Ahead Log for Go applications.**

## Design Overview

![https://pica.zhimg.com/80/v2-9314b84c1c96d13638c5924087399644_1440w.png?source=d16d100b](https://pica.zhimg.com/80/v2-9314b84c1c96d13638c5924087399644_1440w.png?source=d16d100b)

## Format

**Format of the WAL file:**

```
       +-----+-------------+--+----+----------+------+-- ... ----+
 File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
       +-----+-------------+--+----+----------+------+-- ... ----+
       <--- BlockSize ------->|<--- BlockSize ------>|

  rn = variable size records
  P = Padding
  BlockSize = 32KB
```

**Format of a single record:**

```
+---------+-------------+-----------+--- ... ---+
| CRC (4B)| Length (2B) | Type (1B) | Payload   |
+---------+-------------+-----------+--- ... ---+

CRC = 32bit hash computed over the payload using CRC
Length = Length of the payload data
Type = Type of record
       (FullType, FirstType, MiddleType, LastType)
       The type is used to group a bunch of records together to represent
       blocks that are larger than BlockSize
Payload = Byte stream as long as specified by the payload size
```

## Getting Started

```go
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
```
