package inf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/fs"
	"os"
	"sync"
)

type PageStore interface {
	At(idx int) ([]byte, error)
	Free(idx int) error
	Acquire() (idx int)
	Total() int
	Used() int
	Fulled() bool
}

type (
	version [6]byte // big endien uin16-uint16-uint16
	Type    uint8
)

const (
	magicSize   = 16
	metaSize    = 104
	dataStartAt = 128
	TypeFree    = Type(0)
)

var (
	V010000     version
	magicNumber = [magicSize]byte{'f', '.', 'f', 's', 'f'}
	metaPool    = sync.Pool{
		New: func() interface{} {
			return make([]byte, metaSize)
		},
	}
	headerPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, magicSize+metaSize)
		},
	}
)

func init() {
	v := make([]byte, 6)
	binary.BigEndian.PutUint16(v[0:2], uint16(1)) // 1.0.0
	copy(V010000[:], v)
}

type pagedStore struct {
	v        version
	pageSize uint16
	pathfile string

	file     *os.File
	freeHead int
	freeTail int
	total    int
	pagePool sync.Pool
	prepared bool
}

type Page struct {
	Type Type
	Next int
	Data []byte

	size     uint16
	idx      int
	allocate bool
}

func (p Page) Size() uint16 {
	return p.size
}

func New(pathfile string) *pagedStore {
	return &pagedStore{
		pathfile: pathfile,
	}
}

func (s *pagedStore) Create(v version, pageSize uint16) (err error) {
	s.v = v
	s.pageSize = pageSize
	if s.file, err = os.OpenFile(s.pathfile, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return
	}
	var stat fs.FileInfo
	if stat, err = s.file.Stat(); err != nil {
		err = fmt.Errorf("can't create file:%w", err)
		return
	}
	if stat.Size() != 0 {
		err = fmt.Errorf("file exists")
		return
	}
	if _, err = s.file.Write(magicNumber[:]); err != nil {
		return
	}
	if err = s.syncMetaData(); err != nil {
		return
	}
	s.pagePool.New = func() interface{} {
		return make([]byte, s.pageSize)
	}
	// write page in pos 0, this page is not for use
	if _, err := s.file.Write(s.pagePool.Get().([]byte)); err != nil {
		return err
	}
	s.prepared = true
	return nil
}

func (s *pagedStore) Close() error {
	if s.prepared {
		return s.file.Close()
	}
	return nil
}

func (s *pagedStore) Open() (err error) {
	if s.file, err = os.Open(s.pathfile); err != nil {
		err = fmt.Errorf("can't open file: %w", err)
		return
	}
	bs := headerPool.Get().([]byte)
	if _, err = s.file.Read(bs); err != nil {
		err = fmt.Errorf("file occurred when read metadata: %w", err)
		return
	}
	if !bytes.Equal(bs[:magicSize], magicNumber[:]) {
		err = fmt.Errorf("malform: %w", err)
		return
	}
	start := magicSize
	if !bytes.Equal(bs[start+14:start+20], V010000[:]) {
		err = fmt.Errorf("unsupported version")
		return
	}
	s.pageSize = binary.BigEndian.Uint16(bs[start : start+2])
	s.freeHead = int(binary.BigEndian.Uint32(bs[start+2 : start+6]))
	s.freeTail = int(binary.BigEndian.Uint32(bs[start+6 : start+10]))
	s.total = int(binary.BigEndian.Uint32(bs[start+10 : start+14]))

	s.pagePool.New = func() interface{} {
		return make([]byte, s.pageSize)
	}
	s.prepared = true
	return nil
}

func (s *pagedStore) Acquire(count int) []Page {
	ret := make([]Page, count)
	s.ensure(func() error {
		i := 0
		freeIdx := s.freeHead
		for i < count {
			if freeIdx != 0 {
				freeIdx = s.readFreeNext(freeIdx)
				next := freeIdx
				if i == count-1 {
					next = 0
				} else if next == 0 {
					next = s.total
				}
				ret[i] = Page{idx: freeIdx, size: s.pageSize, Next: next}
				i++
				continue
			}
			break
		}
		rest := count - i
		for i := 0; i < rest; i++ {
			next := s.total + i + 1
			if i == rest-1 {
				next = 0
			}
			ret[i] = Page{idx: s.total + i + 1, size: s.pageSize - 5, allocate: true, Next: next}
		}
		return nil
	})
	return ret
}

func (s *pagedStore) Free(idx int) error {
	return s.ensure(func() error {
		if err := s.putFreePage(idx, 0); err != nil {
			return err
		}
		if s.freeTail != 0 {
			if err := s.putFreePage(s.freeTail, idx); err != nil {
				return err
			}
		}
		if err := s.modifyFreeNext(s.freeTail, idx); err != nil {
			return err
		}
		if err := s.modifyFreeTail(idx); err != nil {
			return err
		}
		return nil
	})
}

func (s *pagedStore) putFreePage(idx int, next int) error {
	bs := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(bs)
	bs[0] = byte(TypeFree)
	binary.BigEndian.PutUint16(bs[1:3], 0)
	binary.BigEndian.PutUint32(bs[3:7], uint32(next))
	pos := s.pagePos(idx)
	if _, err := s.file.Seek(pos, os.SEEK_SET); err != nil {
		return err
	}
	if _, err := s.file.Write(bs[:7]); err != nil {
		return err
	}
	return nil
}

func (s *pagedStore) Put(pages []Page) error {
	return s.ensure(func() error {
		bs := s.pagePool.Get().([]byte)
		freeHead := s.freeHead
		for i := range pages {
			bs[0] = byte(pages[i].Type)
			max := s.pageSize - 7
			if len(pages[i].Data) > int(max) {
				return fmt.Errorf("max user data length is %d", max)
			}
			binary.BigEndian.PutUint16(bs[1:3], uint16(len(pages[i].Data)))
			binary.BigEndian.PutUint32(bs[3:7], uint32(pages[i].Next))
			copy(bs[7:7+len(pages[i].Data)], pages[i].Data)
			pos := s.pagePos(pages[i].idx)
			if !pages[i].allocate && pages[i].idx != freeHead {
				return fmt.Errorf("put must begin with free head")
			}
			if _, err := s.file.Seek(pos, os.SEEK_SET); err != nil {
				return err
			}
			if _, err := s.file.Write(bs); err != nil {
				return err
			}
			if i == len(pages)-1 {
				continue
			}
			if pages[i+1].allocate {
				freeHead = 0
				s.freeTail = 0
			} else {
				freeHead = s.readFreeNext(freeHead)
			}
		}
		s.freeHead = freeHead
		if s.freeHead == 0 {
			s.freeTail = 0
		}
		s.syncMetaData()
		return nil
	})
}

func (s *pagedStore) Get(idx int, page *Page) error {
	return s.ensure(func() error {
		bs := s.pagePool.Get().([]byte)
		defer s.pagePool.Put(bs)
		pos := s.pagePos(idx)
		fmt.Printf("index: %d - at: %d\n", idx, pos)
		if _, err := s.file.Seek(pos, os.SEEK_SET); err != nil {
			return err
		}
		if _, err := s.file.Read(bs); err != nil {
			return err
		}
		page.Type = Type(bs[0])
		page.size = binary.BigEndian.Uint16(bs[1:3])
		page.Next = int(binary.BigEndian.Uint32(bs[3:7]))
		page.Data = make([]byte, page.size)
		copy(page.Data, bs[7:7+page.size])
		return nil
	})
}

func (s *pagedStore) pagePos(idx int) int64 {
	if idx == 0 {
		panic("index should begin with 1")
	}
	return int64(dataStartAt + int(s.pageSize)*idx)
}

func (s *pagedStore) modifyFreeNext(idx int, next int) error {
	return nil
}

func (s *pagedStore) readFreeNext(freeIdx int) (nextIdx int) {
	pos := s.pagePos(freeIdx)
	if _, err := s.file.Seek(pos, os.SEEK_SET); err != nil {
		panic(err)
	}
	bs := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(bs)
	if _, err := s.file.Read(bs); err != nil {
		panic(err)
	}
	if Type(bs[0]) != TypeFree {
		panic("not a free page")
	}
	nextIdx = int(binary.BigEndian.Uint32(bs[1:5]))
	return
}

func (s *pagedStore) syncMetaData() error {
	bs := metaPool.Get().([]byte)
	start := 0
	binary.BigEndian.PutUint16(bs[start:start+2], s.pageSize)
	binary.BigEndian.PutUint32(bs[start+2:start+6], uint32(s.freeHead))
	binary.BigEndian.PutUint32(bs[start+6:start+10], uint32(s.freeTail))
	binary.BigEndian.PutUint32(bs[start+10:start+14], uint32(s.total))
	copy(bs[start+14:start+20], s.v[:])
	if _, err := s.file.Seek(magicSize, os.SEEK_SET); err != nil {
		return err
	}
	_, err := s.file.Write(bs)
	return err
}

func (s *pagedStore) modifyFreeTail(idx int) error {
	return nil
}

func (s *pagedStore) ensure(handle func() error) error {
	if !s.prepared {
		panic("you should call Open or Create before any operation")
	}
	return handle()
}
