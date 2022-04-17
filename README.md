## Single file storage design

This is a design for signle file storage that support

```





page based storage
```

page based storage

```
At(idx)
Free(idx)
Acquire() idx
Total()
Used() bool
Full() bool
```

```
magic     | metadata           | page
----------|--------------------|-------------
[16]byte  | page size [2]byte  | free page
          | free head [8]byte  | used page
          | free last [8]byte  |
          | total     [4]byte  |
          | version   [6]byte  |
          | reserved  []byte   |
```


