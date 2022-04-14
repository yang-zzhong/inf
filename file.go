package inf

import (
	"io/fs"
	"os"
	"sync"
)

type file struct {
	rlock sync.Mutex
	*os.File
}

func NewFile(f *os.File) *file {
	return &file{File: f}
}

func (file *file) ReadAt(pos int64, bs []byte) (read int, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	if _, err = file.File.Seek(pos, os.SEEK_SET); err != nil {
		return
	}
	return file.File.Read(bs)
}

func (file *file) WriteAt(pos int64, bs []byte) (read int, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	if _, err = file.File.Seek(pos, os.SEEK_SET); err != nil {
		return
	}
	return file.File.Write(bs)
}

func (file *file) Size() (size int64, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	var stat fs.FileInfo
	stat, err = file.File.Stat()
	size = stat.Size()
	return
}
