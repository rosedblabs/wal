# Release 1.2.0 (2023-07-01)

## 🚀 New Features
* Add `NewReaderWithStart` function to support read log from specified position.

## 🎠 Community
* Thanks to@yanxiaoqi932
  * enhancement: add wal delete function ([#7](https://github.com/rosedblabs/wal/pull/9))

# Release 1.1.0 (2023-06-21)

## 🚀 New Features
* Add tests in windows, with worlflow.
* Add some functions to support rosedb Merge operation.

## 🎠 Community
* Thanks to@SPCDTS
  * fix: calculate seg fle size by seg.size ([#7](https://github.com/rosedblabs/wal/pull/7))
  * fix: limit data size ([#6](https://github.com/rosedblabs/wal/pull/6))
  * fix: spelling error ([#5](https://github.com/rosedblabs/wal/pull/5))

# Release 1.0.0 (2023-06-13)

## 🚀 New Features
* First release, basic operations, read, write, and iterate the log files.
* Add block cache for log files.
