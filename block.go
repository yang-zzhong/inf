package inf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
)

type BlockStore interface {
	Erase(idx int) error
	Acquire(lenInBytes int) ([]Block, error)
	Put([]Block) error
	Get(idx int, p *Block) error
	DataSize() uint16
	From(idx int) ([]Block, error)
	WriteTo(w io.Writer, idx int) ([]Block, error)
}

type (
	version [6]byte // big endian uin16-uint16-uint16
	Type    uint8
)

const (
	magicSize   = 16
	metaSize    = 104
	dataStartAt = 128
	TypeEmpty   = Type(0)
	TypeSuper   = Type(1)
	TypeSingle  = Type(2)
	TypeChained = Type(3)
)

var (
	V010000     version
	magicNumber = [magicSize]byte{'f', '.', 'b', 'l', 'k'}
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
	ErrRWSCNotExists = errors.New("rwsc not exists")
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

type blockStore struct {
	v         version
	blockSize uint16
	rwsNew    func() (RWSC, error)
	pathfile  string

	rws      RWSC
	freeHead int
	freeTail int
	total    int
	pagePool sync.Pool
	prepared bool
}

var _ BlockStore = &blockStore{}

type Block struct {
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

func (p Block) Size() uint16 {
	return p.size
}

func (p Block) Index() int {
	return p.idx
}

func New(rwsNew func() (RWSC, error)) *blockStore {
	return &blockStore{rwsNew: rwsNew}
}

func (s *blockStore) Create(v version, blockSize uint16) (err error) {
	s.v = v
	s.blockSize = blockSize
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
		return make([]byte, s.blockSize)
	}
	// write super block, this block can not free
	if err := s.putPage(0, TypeSuper, 0, []byte{}); err != nil {
		return err
	}
	s.prepared = true
	return nil
}

func (s *blockStore) Close() error {
	if s.prepared {
		return s.rws.Close()
	}
	return nil
}

func (s *blockStore) Open() (err error) {
	if s.rws, err = s.rwsNew(); err != nil {
		err = fmt.Errorf("can't open file: %w", err)
		return
	}
	if em, e := s.emptyRWSC(); e != nil {
		return e
	} else if em {
		return ErrRWSCNotExists
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
	s.blockSize = binary.BigEndian.Uint16(bs[start : start+2])
	s.freeHead = int(binary.BigEndian.Uint32(bs[start+2 : start+6]))
	s.freeTail = int(binary.BigEndian.Uint32(bs[start+6 : start+10]))
	s.total = int(binary.BigEndian.Uint32(bs[start+10 : start+14]))

	s.pagePool.New = func() interface{} {
		return make([]byte, s.blockSize)
	}
	s.prepared = true
	return nil
}

func (s *blockStore) Acquire(lenInBytes int) (blocks []Block, err error) {
	count := int(math.Ceil(float64(lenInBytes) / float64(s.DataSize())))
	blocks = make([]Block, count)
	ty := TypeSingle
	if count > 0 {
		ty = TypeChained
	}
	err = s.ensure(func() error {
		i := 0
		freeIdx := s.freeHead
		for i < count {
			if freeIdx == 0 || freeIdx == s.total+1 {
				break
			}
			next, err := s.nextFreeBlock(freeIdx)
			if err != nil {
				return err
			}
			if i == count-1 {
				next = 0
			} else if next == 0 {
				next = s.total + 1
			}
			blocks[i] = Block{Type: ty, idx: freeIdx, size: s.DataSize(), Next: next}
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
			blocks[i] = Block{Type: ty, idx: idx, size: s.blockSize - 7, allocate: true, Next: next}
		}
		return nil
	})
	return
}

func (s *blockStore) Erase(idx int) error {
	return s.ensure(func() error {
		if idx == 0 {
			return errors.New("super block can not be erased")
		}
		if err := s.putPage(idx, TypeEmpty, 0, []byte{}); err != nil {
			return err
		}
		if s.freeTail != 0 {
			if err := s.putPage(s.freeTail, TypeEmpty, idx, []byte{}); err != nil {
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

func (s *blockStore) putPage(idx int, t Type, next int, data []byte) error {
	bs := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(bs)
	bs[0] = byte(t)
	binary.BigEndian.PutUint16(bs[1:3], uint16(len(data)))
	var hl = 3
	if t == TypeChained {
		binary.BigEndian.PutUint32(bs[3:7], uint32(next))
		hl = 7
	}
	max := int(s.blockSize) - hl
	if len(data) > int(max) {
		return fmt.Errorf("max user data length is %d", max)
	}
	copy(bs[hl:hl+len(data)], data)
	pos := s.blockAt(idx)
	if _, err := s.rws.Seek(pos, os.SEEK_SET); err != nil {
		return err
	}
	writable := bs[:hl+len(data)]
	if _, err := s.rws.Write(writable); err != nil {
		return err
	}
	return nil
}

func (s *blockStore) Put(pages []Block) error {
	return s.ensure(func() error {
		freeHead := s.freeHead
		var err error
		for i := range pages {
			if pages[i].Type == TypeEmpty {
				return fmt.Errorf("can't put free block")
			}
			if pages[i].Type == TypeSuper && pages[i].idx != 0 {
				return fmt.Errorf("super block must at position 0")
			}
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
			} else if freeHead, err = s.nextFreeBlock(freeHead); err != nil {
				return err
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

func (s *blockStore) Get(idx int, page *Block) error {
	return s.ensure(func() error {
		bs := s.pagePool.Get().([]byte)
		defer s.pagePool.Put(bs)
		pos := s.blockAt(idx)
		if _, err := s.rws.Seek(pos, os.SEEK_SET); err != nil {
			return err
		}
		if _, err := s.rws.Read(bs); err != nil {
			return err
		}
		page.Type = Type(bs[0])
		page.size = binary.BigEndian.Uint16(bs[1:3])
		hl := 3
		if page.Type == TypeChained {
			page.Next = int(binary.BigEndian.Uint32(bs[3:7]))
			hl = 7
		}
		page.Data = make([]byte, page.size)
		page.idx = idx
		copy(page.Data, bs[hl:hl+int(page.size)])
		return nil
	})
}

func (s *blockStore) From(idx int) (blocks []Block, err error) {
	blocks = []Block{}
	var i = idx
	for {
		var block Block
		if err = s.Get(i, &block); err != nil {
			return
		}
		blocks = append(blocks, block)
		if block.Type != TypeChained {
			return
		}
		if block.Next != 0 {
			i = block.Next
			continue
		}
		break
	}
	return
}

func (s *blockStore) WriteTo(w io.Writer, idx int) (blocks []Block, err error) {
	if blocks, err = s.From(idx); err != nil {
		return
	}
	for _, block := range blocks {
		if _, err = w.Write(block.Data); err != nil {
			return
		}
	}
	return
}

func (s *blockStore) blockAt(idx int) int64 {
	return int64(dataStartAt + int(s.blockSize)*idx)
}

func (s *blockStore) nextFreeBlock(freeIdx int) (nextIdx int, err error) {
	bs := s.pagePool.Get().([]byte)
	defer s.pagePool.Put(bs)
	pos := s.blockAt(freeIdx)
	if _, err = s.rws.Seek(pos, os.SEEK_SET); err != nil {
		return
	}
	if _, err = s.rws.Read(bs[:7]); err != nil {
		return
	}
	if Type(bs[0]) != TypeEmpty {
		panic("not a free page")
	}
	nextIdx = int(binary.BigEndian.Uint32(bs[3:7]))
	return
}

func (s *blockStore) syncMetaData() error {
	bs := metaPool.Get().([]byte)
	start := 0
	binary.BigEndian.PutUint16(bs[start:start+2], s.blockSize)
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

func (s *blockStore) ensure(handle func() error) error {
	if !s.prepared {
		return errors.New("you should call Open or Create before any operation")
	}
	return handle()
}

func (s *blockStore) emptyRWSC() (em bool, err error) {
	var total int64
	if total, err = s.rws.Seek(0, os.SEEK_END); err != nil {
		return
	}
	em = total == 0
	_, err = s.rws.Seek(0, os.SEEK_SET)
	return
}

func (s *blockStore) DataSize() uint16 {
	return s.blockSize - 7
}
