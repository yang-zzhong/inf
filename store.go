package inf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

type Store interface {
	Add(io.Reader) (idx int, err error)
	Del(idx int) error
	Put(idx int, r io.Reader) error
	Get(idx int) (io.Reader, error)
}

type version uint32
type state uint8

const (
	V1_1       version = 0x00000001
	magicSize          = 16
	metaSize           = 32
	indexSize          = 19
	indexBegin         = magicSize + metaSize

	stateEmpty   state = 0
	stateCreated state = 1
	stateDeleted state = 2
)

var (
	magic                 = []byte{'f', '.', 's', 'f', 's'}
	ErrEmptyStore         = errors.New("empty store")
	ErrUnsupportedVersion = func(v version) error {
		return fmt.Errorf("unsupported version: %d", uint32(v))
	}
	ErrZeroIndexTotal       = errors.New("zero index total")
	ErrIndexCountOutOfRange = errors.New("index count out of range")
	ErrZeroSegmentSize      = errors.New("zero segment size")
)

type index struct {
	state          state
	prev           uint32
	next           uint32
	firstSegmentAt int64
	segments       uint16
	pos            uint32
}

type segment struct {
	state state
	index uint32
	prev  int64
	next  int64
	data  []byte
}

type store struct {
	pathfile    string
	file        *file
	v           version
	indexTotal  uint32
	indexCount  uint32
	segmentSize uint16
	indexes     []index
}

func New(pathfile string) *store {
	return &store{
		pathfile: pathfile,
	}
}

func (store *store) Open() error {
	file, err := os.Open(store.pathfile)
	if err != nil {
		return fmt.Errorf("can't open file: %w", err)
	}
	store.file = NewFile(file)
	if err := store.verifyMagicNumber(); err != nil {
		return fmt.Errorf("can't verify magic number: %w", err)
	}
	if err := store.readMetaData(); err != nil {
		return fmt.Errorf("can't read metadata: %w", err)
	}
	if err := store.initIndexes(); err != nil {
		return fmt.Errorf("can't init indexes: %w", err)
	}
	return nil
}

func (store *store) Create(v version, segmentSize uint16, indexTotal uint32) error {
	store.v = v
	store.segmentSize = segmentSize
	store.indexTotal = indexTotal
	file, err := os.OpenFile(store.pathfile, 0644, fs.FileMode(os.O_RDWR))
	if err != nil {
		return err
	}
	store.file = NewFile(file)
	if err := store.writeMagicNumber(); err != nil {
		return err
	}
	if err := store.writeMetaData(); err != nil {
		return err
	}
	for i := 0; i < int(store.indexTotal); i++ {
		if err := store.writeIndex(i, &index{state: stateEmpty}); err != nil {
			return err
		}
	}
	return nil
}

func (store *store) Add(r io.Reader) (idx int, err error) {
	var pos int64
	var from int64
	from, err = store.unuseStateFrom(store.segmentFrom())
	if err != nil {
		return
	}
	var index = index{
		state:          stateCreated,
		firstSegmentAt: from,
	}
	if store.indexCount > 0 {
		index.prev = store.indexes[store.indexCount].pos
	}
	seg := segment{
		data: make([]byte, store.segmentSize-21),
	}
	for {
		if pos, err = store.unuseStateFrom(store.segmentFrom()); err != nil {
			return
		}
		if rl, err = r.Read(bs[21:]); err != nil {
			if !errors.Is(err, io.EOF) {
				return
			}
		}
		store.writeTo(pos, &segment{})
	}
	return
}

func (store *store) Del(idx int) error {
	return nil
}

func (store *store) Get(idx int) (r io.Reader, err error) {
	return
}

func (store *store) Put(idx int, r io.Reader) error {
	return nil
}

func (store *store) writeMagicNumber() error {
	_, err := store.file.WriteAt(0, magic)
	return err
}

func (store *store) writeMetaData() error {
	mb := make([]byte, metaSize)
	binary.BigEndian.PutUint32(mb[0:4], uint32(store.v))
	binary.BigEndian.PutUint32(mb[4:8], store.indexCount)
	binary.BigEndian.PutUint32(mb[8:12], store.indexTotal)
	binary.BigEndian.PutUint16(mb[12:14], store.segmentSize)

	_, err := store.file.WriteAt(magicSize, mb)
	return err
}

func (store *store) verifyMagicNumber() error {
	mb := make([]byte, magicSize)
	if _, err := store.file.ReadAt(0, mb); err != nil {
		return err
	}
	if !bytes.Equal(mb, magic) {
		return errors.New("not a .sfs file")
	}
	return nil
}

func (store *store) readMetaData() error {
	mb := make([]byte, metaSize)
	if _, err := store.file.ReadAt(magicSize, mb); err != nil {
		return err
	}
	store.v = version(binary.BigEndian.Uint32(mb[0:4]))
	if store.v != V1_1 {
		return ErrUnsupportedVersion(store.v)
	}
	store.indexCount = binary.BigEndian.Uint32(mb[4:8])
	store.indexTotal = binary.BigEndian.Uint32(mb[8:12])
	if store.indexTotal == 0 {
		return ErrZeroIndexTotal
	}
	if store.indexCount >= store.indexTotal {
		return ErrIndexCountOutOfRange
	}
	store.segmentSize = binary.BigEndian.Uint16(mb[12:14])
	if store.segmentSize == 0 {
		return ErrZeroSegmentSize
	}
	return nil
}

func (store *store) initIndexes() error {
	var index index
	if err := store.getRandomNonEmptyIndex(&index); err != nil {
		if errors.Is(err, ErrEmptyStore) {
			return nil
		}
		return err
	}
	store.indexes = append(store.indexes, index)
	return store.initIndexFrom(0)
}

func (store *store) initIndexFrom(idx int) error {
	ind := store.indexes[idx]
	if ind.prev != 0 {
		var prev index
		store.readIndexAt(ind.prev, &prev)
		store.indexes = append(store.indexes[:idx], append([]index{prev}, store.indexes[idx:]...)...)
		return store.initIndexFrom(idx)
	}
	if ind.next != 0 {
		var next index
		store.readIndexAt(ind.next, &next)
		if idx == len(store.indexes)-1 {
			store.indexes = append(store.indexes, next)
		}
		store.indexes = append(store.indexes[:idx+1], append([]index{next}, store.indexes[idx+1:]...)...)
		return store.initIndexFrom(idx + 1)
	}
	return nil
}

func (store *store) getRandomNonEmptyIndex(index *index) error {
	for i := 0; i < int(store.indexTotal); i++ {
		if err := store.readIndex(i, index); err != nil {
			return err
		}
		if index.state == stateCreated {
			return nil
		}
	}
	return ErrEmptyStore
}

func (store *store) readIndexAt(pos uint32, index *index) error {
	if pos >= indexBegin+indexSize*store.indexTotal {
		return errors.New("index out of range")
	}
	bs := make([]byte, indexSize)
	if _, err := store.file.ReadAt(int64(pos), bs); err != nil {
		return err
	}
	index.state = state(bs[0])
	index.prev = binary.BigEndian.Uint32(bs[1:5])
	index.next = binary.BigEndian.Uint32(bs[5:9])
	index.firstSegmentAt = int64(binary.BigEndian.Uint64(bs[9:17]))
	index.segments = binary.BigEndian.Uint16(bs[17:19])
	index.pos = pos
	return nil
}

func (store *store) writeIndexAt(pos uint32, index *index) error {
	if pos >= indexBegin+indexSize*store.indexTotal {
		return errors.New("index out of range")
	}
	bs := make([]byte, indexSize)
	bs[0] = byte(index.state)
	binary.BigEndian.PutUint32(bs[1:5], index.prev)
	binary.BigEndian.PutUint32(bs[5:9], index.next)
	binary.BigEndian.PutUint64(bs[9:17], uint64(index.firstSegmentAt))
	binary.BigEndian.PutUint16(bs[17:19], index.segments)
	_, err := store.file.WriteAt(int64(pos), bs)
	return err
}

func (store *store) readIndex(idx int, index *index) (err error) {
	return store.readIndexAt(uint32(indexBegin+idx*indexSize), index)
}

func (store *store) writeIndex(idx int, index *index) error {
	return store.writeIndexAt(uint32(indexBegin+idx*indexSize), index)
}

func (store *store) writeTo(pos int64, seg *segment) (rl int, err error) {
	bs := make([]byte, store.segmentSize)
	bs[0] = byte(seg.state)
	binary.BigEndian.PutUint64(bs[1:9], uint64(seg.prev))
	binary.BigEndian.PutUint64(bs[9:17], uint64(seg.next))
	binary.BigEndian.PutUint32(bs[17:21], seg.index)

	if rl == 0 {
		return
	}
	_, err = store.file.WriteAt(pos, bs)
	return
}

func (store *store) unuseStateFrom(pos int64) (ret int64, err error) {
	var size int64
	size, err = store.file.Size()
	if err != nil {
		return
	}
	var state state
	for pos < size {
		if err = store.readSegStateAt(pos, &state); err != nil {
			return
		}
		if state != stateCreated {
			pos += int64(store.segmentSize)
			continue
		}
		ret = pos
		return
	}
	return
}

func (store *store) readSegmentAt(pos int64, seg *segment) error {
	bs := make([]byte, store.segmentSize)
	if _, err := store.file.ReadAt(pos, bs); err != nil {
		return err
	}
	seg.state = state(bs[0])
	seg.prev = int64(binary.BigEndian.Uint64(bs[1:9]))
	seg.next = int64(binary.BigEndian.Uint64(bs[9:17]))
	seg.index = binary.BigEndian.Uint32(bs[17:21])
	copy(seg.data, bs[21:])
	return nil
}

func (store *store) readSegStateAt(pos int64, s *state) error {
	bs := make([]byte, 1)
	if _, err := store.file.ReadAt(pos, bs); err != nil {
		return err
	}
	*s = state(bs[0])

	return nil
}

func (store *store) segmentFrom() int64 {
	return int64(indexBegin + indexSize*store.indexTotal)
}
