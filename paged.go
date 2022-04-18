package inf

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type PageStore interface {
	Free(idx int) error
	Acquire(idx int) []Page
	Put([]Page) error
	Get(idx int, p *Page) error
}

type (
	version [6]byte // big endian uin16-uint16-uint16
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

func (v version) String() string {
	max := binary.BigEndian.Uint16(v[0:2])
	mid := binary.BigEndian.Uint16(v[2:4])
	min := binary.BigEndian.Uint16(v[4:6])
	return fmt.Sprintf("v%02d.%02d.%02d", max, mid, min)
}

func init() {
	v := make([]byte, 6)
	binary.BigEndian.PutUint16(v[0:2], uint16(1)) // 1.0.0
	copy(V010000[:], v)
}

type RWSC interface {
	io.ReadWriteSeeker
	io.Closer
}

type pagedStore struct {
	v        version
	pageSize uint16
	rwsNew   func() (RWSC, error)
	pathfile string

	rws      RWSC
	freeHead int
	freeTail int
	total    int
	pagePool sync.Pool
	prepared bool
}

var _ PageStore = &pagedStore{}

type Page struct {
	Type Type
	Next int
	Data []byte

	size     uint16
	idx      int
	allocate bool
}

func FileRWSC(pathfile string) func() (RWSC, error) {
	return func() (RWSC, error) {
		return os.OpenFile(pathfile, os.O_CREATE|os.O_RDWR, 0644)
	}
}

func (p Page) Size() uint16 {
	return p.size
}

func New(rwsNew func() (RWSC, error)) *pagedStore {
	return &pagedStore{rwsNew: rwsNew}
}

func (s *pagedStore) Create(v version, pageSize uint16) (err error) {
	s.v = v
	s.pageSize = pageSize
	if s.rws, err = s.rwsNew(); err != nil {
		return
	}
	if em, e := s.emptyRWSC(); e != nil {
		return e
	} else if !em {
		return fmt.Errorf("rwsc exists")
	}
	if _, err = s.rws.Write(magicNumber[:]); err != nil {
		return
	}
	if err = s.syncMetaData(); err != nil {
		return
	}
	s.pagePool.New = func() interface{} {
		return make([]byte, s.pageSize)
	}
	ff := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(ff)
	// write page in pos 0, this page is not for use
	if _, err := s.rws.Write(ff); err != nil {
		return err
	}
	s.prepared = true
	return nil
}

func (s *pagedStore) Close() error {
	if s.prepared {
		return s.rws.Close()
	}
	return nil
}

func (s *pagedStore) Open() (err error) {
	if s.rws, err = s.rwsNew(); err != nil {
		err = fmt.Errorf("can't open file: %w", err)
		return
	}
	if em, e := s.emptyRWSC(); e != nil {
		return e
	} else if em {
		return fmt.Errorf("rwsc not exists")
	}
	bs := headerPool.Get().([]byte)
	if _, err = s.rws.Read(bs); err != nil {
		err = fmt.Errorf("file occurred when read metadata: %w", err)
		return
	}
	if !bytes.Equal(bs[:magicSize], magicNumber[:]) {
		err = fmt.Errorf("malformed format: %w", err)
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
			if freeIdx == 0 || freeIdx == s.total+1 {
				break
			}
			next := s.readFreeNext(freeIdx)
			if i == count-1 {
				next = 0
			} else if next == 0 {
				next = s.total + 1
			}
			ret[i] = Page{idx: freeIdx, size: s.pageSize - 7, Next: next}
			freeIdx = next
			i++
		}
		setted := i
		for i := setted; i < count; i++ {
			idx := s.total + i - setted + 1
			next := idx + 1
			if i == count-1 {
				next = 0
			}
			ret[i] = Page{idx: idx, size: s.pageSize - 7, allocate: true, Next: next}
		}
		return nil
	})
	return ret
}

func (s *pagedStore) Free(idx int) error {
	return s.ensure(func() error {
		if err := s.putPage(idx, TypeFree, 0, []byte{}); err != nil {
			return err
		}
		if s.freeTail != 0 {
			if err := s.putPage(s.freeTail, TypeFree, idx, []byte{}); err != nil {
				return err
			}
		}
		s.freeTail = idx
		if s.freeHead == 0 {
			s.freeHead = idx
		}
		return s.syncMetaData()
	})
}

func (s *pagedStore) putPage(idx int, t Type, next int, data []byte) error {
	bs := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(bs)
	bs[0] = byte(t)
	binary.BigEndian.PutUint16(bs[1:3], uint16(len(data)))
	binary.BigEndian.PutUint32(bs[3:7], uint32(next))
	max := s.pageSize - 7
	if len(data) > int(max) {
		return fmt.Errorf("max user data length is %d", max)
	}
	copy(bs[7:7+len(data)], data)
	pos := s.pagePos(idx)
	if _, err := s.rws.Seek(pos, os.SEEK_SET); err != nil {
		return err
	}
	writable := bs[:7+len(data)]
	fmt.Printf("%d - %s\n", len(writable), writable)
	if _, err := s.rws.Write(writable); err != nil {
		return err
	}
	return nil
}

func (s *pagedStore) Put(pages []Page) error {
	return s.ensure(func() error {
		freeHead := s.freeHead
		for i := range pages {
			if !pages[i].allocate && pages[i].idx != freeHead {
				return fmt.Errorf("put must begin with free head")
			}
			if err := s.putPage(pages[i].idx, pages[i].Type, pages[i].Next, pages[i].Data); err != nil {
				return err
			}
			if pages[i].allocate {
				s.total += 1
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
		if _, err := s.rws.Seek(pos, os.SEEK_SET); err != nil {
			return err
		}
		if _, err := s.rws.Read(bs); err != nil {
			return err
		}
		page.Type = Type(bs[0])
		page.size = binary.BigEndian.Uint16(bs[1:3])
		page.Next = int(binary.BigEndian.Uint32(bs[3:7]))
		page.Data = make([]byte, page.size)
		page.idx = idx
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

func (s *pagedStore) readFreeNext(freeIdx int) (nextIdx int) {
	bs := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(bs)
	pos := s.pagePos(freeIdx)
	if _, err := s.rws.Seek(pos, os.SEEK_SET); err != nil {
		panic(err)
	}
	if _, err := s.rws.Read(bs); err != nil {
		panic(err)
	}
	if Type(bs[0]) != TypeFree {
		panic("not a free page")
	}
	nextIdx = int(binary.BigEndian.Uint32(bs[3:7]))
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
	if _, err := s.rws.Seek(magicSize, os.SEEK_SET); err != nil {
		return err
	}
	_, err := s.rws.Write(bs)
	return err
}

func (s *pagedStore) ensure(handle func() error) error {
	if !s.prepared {
		panic("you should call Open or Create before any operation")
	}
	return handle()
}

func (s *pagedStore) emptyRWSC() (em bool, err error) {
	var total int64
	if total, err = s.rws.Seek(0, os.SEEK_END); err != nil {
		return
	}
	em = total == 0
	_, err = s.rws.Seek(0, os.SEEK_SET)
	return
}
