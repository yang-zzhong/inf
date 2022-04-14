package inf

import (
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

func (file *file) ReadAt(pos int64, whence int, bs []byte) (read int, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	if _, err = file.File.Seek(pos, whence); err != nil {
		return
	}
	return file.File.Read(bs)
}

func (file *file) WriteAt(pos int64, whence int, bs []byte) (read int, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	if _, err = file.File.Seek(pos, whence); err != nil {
		return
	}
	return file.File.Write(bs)
}

func (file *file) EndPos() (end int64, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	end, err = file.File.Seek(0, os.SEEK_END)
	return
}
