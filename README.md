## Single file storage design

This is a design for signle file storage that support

```
store.add(Reader) index
store.del(index)
store.put(index, Reader)
store.get(index) Reader
```

`Reader` is an abstract which can get content with `read` method.

file format design

```
magic number |metadata  |index        |data
-------------|----------|-------------|--------------------------
[16]byte     |          |             |

```

`magic number` is solid byte, its value is ascii bytes `f.sfs`
`metadata` hold all the global information about the file

* version [uint32] - current using version
* indexCount [uint32] - how many indexes available to use
* indexTotal [uint32] - how total index count
* segment size [uint16] - we save data in segment, we must know each segment size

metadata size is `10 + reserved 22`

`index`

* state          [uint8]
* prev           [uint32]
* next           [uint32]
* firstSegmentAt [int64]
* segments       [uint16]

we wanna hold max 10K elements, so the index area size is `18 * 10K`

`segment`

* index [uint32]
* prev [int64]
* next [int64]
* content [segmentsize - 20]
