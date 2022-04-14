package inf

import "io"

type WriterGetter interface {
	Writer() io.Writer
}

type Sieable interface {
	Size() uint32
}

type Writable struct {
	WriterGetter
	Sieable
}

type ReaderGetter interface {
	Reader() io.Reader
}

type item struct {
}

type itemReader struct {
}

func (item *itemForRead) Reader() io.Reader {
}
