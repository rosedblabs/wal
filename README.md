# wal
**[WIP] Write Ahead Log for Go applications.**

## Design Overview

![](https://img-blog.csdnimg.cn/ee6ff16b879a4434aa90f9b4f1a417a9.png#pic_center)

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
