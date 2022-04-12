package store

import (
	"os"
	"sync"
)

type File struct {
	rlock sync.Mutex
	*os.File
}

func NewFile(file *os.File) *File {
	return &File{File: file}
}

func (file *File) ReadAt(pos int64, whence int, bs []byte) (read int, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	if _, err = file.File.Seek(pos, whence); err != nil {
		return
	}
	return file.File.Read(bs)
}

func (file *File) WriteAt(pos int64, whence int, bs []byte) (read int, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	if _, err = file.File.Seek(pos, whence); err != nil {
		return
	}
	return file.File.Write(bs)
}

func (file *File) EndPos() (end int64, err error) {
	file.rlock.Lock()
	defer file.rlock.Unlock()
	end, err = file.File.Seek(0, os.SEEK_END)
	return
}
